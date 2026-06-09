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
