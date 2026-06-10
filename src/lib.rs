//! stryke-mongo — MongoDB cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn mongo__*` is a JSON-string-in /
//! JSON-string-out wrapper around `mongodb`'s async client API. stryke's
//! FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at
//! first `use Mongo`, registers each one as a stryke-callable function,
//! and on each call passes a JSON-encoded args dict and copies the
//! returned JSON into a stryke string.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `CLIENTS` — `mongodb::Client` cache per connection URI. v1
//!     helper rebuilt the client (TCP+TLS+auth handshake) per fork;
//!     this reuses the same client + underlying connection pool across
//!     calls.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

use anyhow::{anyhow, Result};
use bson::{doc, Bson, Document};
use futures_util::TryStreamExt;
use mongodb::options::{
    AggregateOptions, ClientOptions, CountOptions, FindOneOptions, FindOptions,
};
use mongodb::Client;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};

// ── runtime + client cache ──────────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static CLIENTS: OnceCell<Mutex<HashMap<String, Client>>> = OnceCell::new();

fn clients() -> &'static Mutex<HashMap<String, Client>> {
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn get_client(opts: &Value) -> Result<Client> {
    let uri = opts
        .get("uri")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| std::env::var("MONGODB_URI").ok())
        .unwrap_or_else(|| "mongodb://127.0.0.1:27017".to_string());
    {
        let map = clients().lock();
        if let Some(c) = map.get(&uri) {
            return Ok(c.clone());
        }
    }
    let mut co = ClientOptions::parse(&uri).await?;
    co.app_name = Some("stryke-mongo".to_string());
    // Default to fast-fail timeouts so `Mongo::ping` on a missing/wrong
    // host returns in seconds, not in the mongodb driver's 30s default.
    // Skipped when the URI sets the same params (driver fills them from
    // `?serverSelectionTimeoutMS=...` / `?connectTimeoutMS=...`); both
    // are independently overridable via the `opts` hash.
    let sst_ms = opts
        .get("server_selection_timeout_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(2000);
    let ct_ms = opts
        .get("connect_timeout_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(2000);
    if co.server_selection_timeout.is_none() {
        co.server_selection_timeout = Some(std::time::Duration::from_millis(sst_ms));
    }
    if co.connect_timeout.is_none() {
        co.connect_timeout = Some(std::time::Duration::from_millis(ct_ms));
    }
    let client = Client::with_options(co)?;
    clients().lock().insert(uri, client.clone());
    Ok(client)
}

/// `target` is either `db.coll` (preferred) or `coll` (uses default db
/// from the URI). Returns (db_name, coll_name).
fn parse_target<'a>(opts: &'a Value, default_db: Option<&'a str>) -> Result<(String, String)> {
    let target = opts["target"]
        .as_str()
        .ok_or_else(|| anyhow!("missing target (db.coll)"))?;
    if let Some(dot) = target.find('.') {
        Ok((target[..dot].to_string(), target[dot + 1..].to_string()))
    } else if let Some(db) = default_db {
        Ok((db.to_string(), target.to_string()))
    } else {
        Err(anyhow!(
            "target `{}` has no db prefix and no default db specified",
            target
        ))
    }
}

fn json_to_doc(v: &Value) -> Result<Document> {
    let b: Bson = bson::to_bson(v)?;
    match b {
        Bson::Document(d) => Ok(d),
        Bson::Null => Ok(Document::new()),
        _ => Err(anyhow!("expected JSON object, got {:?}", b)),
    }
}

fn doc_to_json(d: &Document) -> Result<Value> {
    Ok(serde_json::to_value(d)?)
}

// ── ops ─────────────────────────────────────────────────────────────────────

async fn op_ping(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().unwrap_or("admin");
    let r = c.database(db).run_command(doc! { "ping": 1 }).await?;
    Ok(json!({"ok": r.get_f64("ok").unwrap_or(0.0) == 1.0}))
}

async fn op_list_databases(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let names = c.list_database_names().await?;
    Ok(json!({"databases": names}))
}

async fn op_list_collections(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().ok_or_else(|| anyhow!("missing db"))?;
    let names = c.database(db).list_collection_names().await?;
    Ok(json!({"collections": names}))
}

async fn op_find(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = match opts.get("filter") {
        Some(v) if !v.is_null() => json_to_doc(v)?,
        _ => Document::new(),
    };
    let limit = opts["limit"].as_i64();
    let skip = opts["skip"].as_u64();
    let sort = opts.get("sort").and_then(|v| {
        if v.is_null() {
            None
        } else {
            json_to_doc(v).ok()
        }
    });
    let projection = opts.get("projection").and_then(|v| {
        if v.is_null() {
            None
        } else {
            json_to_doc(v).ok()
        }
    });
    let mut fo = FindOptions::default();
    fo.limit = limit;
    fo.skip = skip;
    fo.sort = sort;
    fo.projection = projection;
    let coll = c.database(&db).collection::<Document>(&coll);
    let mut cursor = coll.find(filter).with_options(fo).await?;
    let mut out: Vec<Value> = Vec::new();
    while let Some(d) = cursor.try_next().await? {
        out.push(doc_to_json(&d)?);
    }
    Ok(json!({"docs": out}))
}

async fn op_find_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = match opts.get("filter") {
        Some(v) if !v.is_null() => json_to_doc(v)?,
        _ => Document::new(),
    };
    let projection = opts.get("projection").and_then(|v| {
        if v.is_null() {
            None
        } else {
            json_to_doc(v).ok()
        }
    });
    let mut fo = FindOneOptions::default();
    fo.projection = projection;
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.find_one(filter).with_options(fo).await?;
    Ok(json!({"doc": match r { Some(d) => doc_to_json(&d)?, None => Value::Null }}))
}

async fn op_count(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = match opts.get("filter") {
        Some(v) if !v.is_null() => json_to_doc(v)?,
        _ => Document::new(),
    };
    let coll = c.database(&db).collection::<Document>(&coll);
    let n = coll
        .count_documents(filter)
        .with_options(CountOptions::default())
        .await?;
    Ok(json!({"value": n as i64}))
}

async fn op_aggregate(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let pipeline_v = opts["pipeline"]
        .as_array()
        .ok_or_else(|| anyhow!("missing pipeline (array)"))?;
    let pipeline: Result<Vec<Document>> = pipeline_v.iter().map(json_to_doc).collect();
    let coll = c.database(&db).collection::<Document>(&coll);
    let mut cursor = coll
        .aggregate(pipeline?)
        .with_options(AggregateOptions::default())
        .await?;
    let mut out: Vec<Value> = Vec::new();
    while let Some(d) = cursor.try_next().await? {
        out.push(doc_to_json(&d)?);
    }
    Ok(json!({"docs": out}))
}

async fn op_insert_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let doc = json_to_doc(&opts["doc"])?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.insert_one(doc).await?;
    Ok(json!({"inserted_id": serde_json::to_value(r.inserted_id)?}))
}

async fn op_insert_many(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let docs_v = opts["docs"]
        .as_array()
        .ok_or_else(|| anyhow!("missing docs (array)"))?;
    let docs: Result<Vec<Document>> = docs_v.iter().map(json_to_doc).collect();
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.insert_many(docs?).await?;
    Ok(json!({"inserted_count": r.inserted_ids.len() as i64}))
}

async fn op_update_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let update = json_to_doc(&opts["update"])?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.update_one(filter, update).await?;
    Ok(json!({
        "matched_count": r.matched_count as i64,
        "modified_count": r.modified_count as i64,
        "upserted_id": serde_json::to_value(r.upserted_id)?,
    }))
}

async fn op_update_many(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let update = json_to_doc(&opts["update"])?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.update_many(filter, update).await?;
    Ok(json!({
        "matched_count": r.matched_count as i64,
        "modified_count": r.modified_count as i64,
    }))
}

async fn op_replace_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let replacement = json_to_doc(&opts["doc"])?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.replace_one(filter, replacement).await?;
    Ok(json!({
        "matched_count": r.matched_count as i64,
        "modified_count": r.modified_count as i64,
    }))
}

async fn op_delete_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.delete_one(filter).await?;
    Ok(json!({"deleted_count": r.deleted_count as i64}))
}

async fn op_delete_many(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.delete_many(filter).await?;
    Ok(json!({"deleted_count": r.deleted_count as i64}))
}

async fn op_create_index(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let keys = json_to_doc(&opts["keys"])?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let model = mongodb::IndexModel::builder().keys(keys).build();
    let r = coll.create_index(model).await?;
    Ok(json!({"name": r.index_name}))
}

async fn op_drop_index(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let coll = c.database(&db).collection::<Document>(&coll);
    coll.drop_index(&name).await?;
    Ok(json!({"ok": true}))
}

async fn op_indexes(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let mut cursor = coll.list_indexes().await?;
    let mut out: Vec<Value> = Vec::new();
    while let Some(m) = cursor.try_next().await? {
        out.push(serde_json::to_value(m)?);
    }
    Ok(json!({"indexes": out}))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-mongo handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn mongo__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn mongo__ping(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ping)
}

#[no_mangle]
pub extern "C" fn mongo__list_databases(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_list_databases)
}

#[no_mangle]
pub extern "C" fn mongo__list_collections(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_list_collections)
}

#[no_mangle]
pub extern "C" fn mongo__find(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_find)
}

#[no_mangle]
pub extern "C" fn mongo__find_one(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_find_one)
}

#[no_mangle]
pub extern "C" fn mongo__count(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_count)
}

#[no_mangle]
pub extern "C" fn mongo__aggregate(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_aggregate)
}

#[no_mangle]
pub extern "C" fn mongo__insert_one(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_insert_one)
}

#[no_mangle]
pub extern "C" fn mongo__insert_many(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_insert_many)
}

#[no_mangle]
pub extern "C" fn mongo__update_one(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_update_one)
}

#[no_mangle]
pub extern "C" fn mongo__update_many(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_update_many)
}

#[no_mangle]
pub extern "C" fn mongo__replace_one(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_replace_one)
}

#[no_mangle]
pub extern "C" fn mongo__delete_one(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_delete_one)
}

#[no_mangle]
pub extern "C" fn mongo__delete_many(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_delete_many)
}

#[no_mangle]
pub extern "C" fn mongo__create_index(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_create_index)
}

#[no_mangle]
pub extern "C" fn mongo__drop_index(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_drop_index)
}

#[no_mangle]
pub extern "C" fn mongo__indexes(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_indexes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_target_dotted_form() {
        let (db, coll) = parse_target(&json!({"target": "shop.orders"}), None).unwrap();
        assert_eq!(db, "shop");
        assert_eq!(coll, "orders");
    }

    #[test]
    fn parse_target_uses_default_db_when_no_dot() {
        let (db, coll) = parse_target(&json!({"target": "orders"}), Some("shop")).unwrap();
        assert_eq!(db, "shop");
        assert_eq!(coll, "orders");
    }

    #[test]
    fn parse_target_no_dot_no_default_errors() {
        let err = parse_target(&json!({"target": "orders"}), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no db prefix"), "{err}");
        assert!(err.contains("orders"), "{err}");
    }

    #[test]
    fn parse_target_missing_errors() {
        let err = parse_target(&json!({}), None).unwrap_err().to_string();
        assert!(err.contains("missing target"), "{err}");
    }

    #[test]
    fn parse_target_first_dot_is_separator() {
        // `db.with.dots.coll` → db="db", coll="with.dots.coll".
        // First dot wins; mongo collection names may contain dots.
        let (db, coll) = parse_target(&json!({"target": "db.with.dots.coll"}), None).unwrap();
        assert_eq!(db, "db");
        assert_eq!(coll, "with.dots.coll");
    }

    #[test]
    fn json_to_doc_round_trips_object() {
        let v = json!({"name": "ada", "age": 36});
        let d = json_to_doc(&v).unwrap();
        assert_eq!(d.get_str("name").unwrap(), "ada");
        // serde_json::Number → bson::Bson maps integers to i64 by default,
        // not i32 — assert against the wider type.
        assert_eq!(d.get_i64("age").unwrap(), 36);
    }

    #[test]
    fn json_to_doc_null_yields_empty_doc() {
        let d = json_to_doc(&Value::Null).unwrap();
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn json_to_doc_non_object_errors() {
        let err = json_to_doc(&json!([1, 2, 3])).unwrap_err().to_string();
        assert!(err.contains("expected JSON object"), "{err}");
    }

    #[test]
    fn doc_to_json_round_trips() {
        let d = doc! { "id": 1, "name": "ada" };
        let v = doc_to_json(&d).unwrap();
        assert_eq!(v["id"], json!(1));
        assert_eq!(v["name"], json!("ada"));
    }

    #[test]
    fn json_to_doc_to_json_preserves_basic_fields() {
        let original = json!({"city": "ny", "active": true, "count": 42});
        let d = json_to_doc(&original).unwrap();
        let back = doc_to_json(&d).unwrap();
        assert_eq!(back["city"], json!("ny"));
        assert_eq!(back["active"], json!(true));
        assert_eq!(back["count"], json!(42));
    }

    /// Multi-dot target `"db.coll.sub"` — first dot wins, everything after
    /// is the coll name. Pin the contract so a future refactor that
    /// "helpfully" rsplits or rejects the multi-dot form gets caught.
    /// Real-world use: collection names with dots are legal in MongoDB,
    /// so a hand-written `"shop.events.2026"` must round-trip as
    /// (db=shop, coll=events.2026).
    #[test]
    fn parse_target_multi_dot_keeps_everything_after_first_dot_as_coll() {
        let (db, coll) = parse_target(
            &json!({"target": "shop.events.2026"}),
            None,
        )
        .unwrap();
        assert_eq!(db, "shop");
        assert_eq!(coll, "events.2026");
    }

    /// Empty `target` string with no default_db → error (not silent
    /// success with empty db/coll). Pin this so a future refactor that
    /// short-circuits empty-string to `("", "")` instead of erroring
    /// gets caught at test time.
    #[test]
    fn parse_target_empty_string_errors_without_default_db() {
        let err = parse_target(&json!({"target": ""}), None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no db prefix") || err.contains("has no db"),
            "got: {err}"
        );
    }

    /// `default_db` only fills in when `target` has no dot — if `target`
    /// already contains a dot, the in-target db wins.
    #[test]
    fn parse_target_in_target_db_wins_over_default_db() {
        let (db, coll) = parse_target(
            &json!({"target": "explicit.coll"}),
            Some("fallback"),
        )
        .unwrap();
        assert_eq!(db, "explicit");
        assert_eq!(coll, "coll");
    }

    // ── audit additions ────────────────────────────────────────────────────

    /// pkg_version is the one export that hits no network and no mongo
    /// client cache — it's the canary for the FFI plumbing itself. This
    /// pins:
    ///   * null input (handler runs with Value::Null, doesn't panic)
    ///   * returned pointer is a valid non-null C string
    ///   * JSON envelope contains {"version": ...} matching CARGO_PKG_VERSION
    ///   * stryke_free_cstring on the returned pointer doesn't crash and
    ///     accepts the *mut cast (mirrors what stryke's FFI bridge does)
    /// If ffi_call_async ever regressed to panicking on null args, or
    /// stopped returning a valid CString, or stryke_free_cstring's null
    /// guard broke, this would catch it.
    #[test]
    fn pkg_version_ffi_roundtrip_with_null_args_and_free() {
        let ptr = mongo__pkg_version(std::ptr::null());
        assert!(!ptr.is_null(), "pkg_version returned null");
        // SAFETY: ptr came from CString::into_raw inside ffi_call_async.
        let s = unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() };
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            v["version"].as_str().unwrap(),
            env!("CARGO_PKG_VERSION"),
            "pkg_version FFI envelope drifted from CARGO_PKG_VERSION"
        );
        // Reclaim ownership via the public free fn — the actual API
        // stryke calls on every returned string. Also exercises the
        // *mut c_char cast.
        unsafe { stryke_free_cstring(ptr as *mut c_char) };
        // And the null guard.
        unsafe { stryke_free_cstring(std::ptr::null_mut()) };
    }

    /// ffi_call_async catches panics inside the handler future and
    /// converts them to a JSON error envelope, NOT an unwind through
    /// the C ABI boundary (which is UB). This is the load-bearing
    /// safety net for the entire cdylib — if it ever broke, every
    /// panic in any handler would corrupt the host process.
    ///
    /// We invoke ffi_call_async directly with a synchronous handler
    /// that panics inside its async body, then assert we get the
    /// "stryke-mongo handler panicked" envelope back as a valid
    /// CString (not a null pointer, not an unwind).
    #[test]
    fn ffi_call_async_converts_handler_panic_to_error_envelope() {
        let args = CString::new(r#"{}"#).unwrap();
        let ptr = ffi_call_async(args.as_ptr(), |_v: Value| async {
            panic!("synthetic handler panic — should be caught by AssertUnwindSafe");
            #[allow(unreachable_code)]
            Ok::<Value, anyhow::Error>(json!({}))
        });
        assert!(!ptr.is_null(), "panic path must still return a CString");
        let s = unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() };
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            v["error"].as_str().unwrap(),
            "stryke-mongo handler panicked",
            "panic envelope drift — handler panics may now leak across FFI (UB)"
        );
        unsafe { stryke_free_cstring(ptr as *mut c_char) };
    }

    /// parse_target's `find('.')` returns the FIRST dot's byte offset.
    /// Three boundary inputs that are technically accepted but
    /// semantically broken: empty db, empty coll, and just ".". The
    /// driver will reject these at the mongo wire layer, but the helper
    /// passes them through. Pinning current behavior so the boss sees
    /// the gap explicitly — if a future guard rejects empty components,
    /// this test breaks and the new behavior gets pinned by an updated
    /// test rather than slipped in by accident.
    /// (`""` alone is covered by the upstream empty-string test above.)
    #[test]
    fn parse_target_accepts_empty_db_or_coll_silently() {
        // Empty db component, non-empty coll.
        let (db, coll) = parse_target(&json!({"target": ".orders"}), None).unwrap();
        assert_eq!(db, "");
        assert_eq!(coll, "orders");

        // Non-empty db, empty coll component.
        let (db, coll) = parse_target(&json!({"target": "shop."}), None).unwrap();
        assert_eq!(db, "shop");
        assert_eq!(coll, "");

        // Both empty — just a dot.
        let (db, coll) = parse_target(&json!({"target": "."}), None).unwrap();
        assert_eq!(db, "");
        assert_eq!(coll, "");
    }
}
