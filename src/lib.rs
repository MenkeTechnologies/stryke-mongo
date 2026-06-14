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
    AggregateOptions, ClientOptions, CountOptions, FindOneAndDeleteOptions,
    FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOneOptions, FindOptions, ReplaceOptions,
    ReturnDocument, UpdateOptions,
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

/// Optional doc field → `Document` (None when absent/null).
fn opt_doc(opts: &Value, key: &str) -> Option<Document> {
    match opts.get(key) {
        Some(v) if !v.is_null() => json_to_doc(v).ok(),
        _ => None,
    }
}

/// `array_filters` opt → Vec<Document> when present.
fn opt_array_filters(opts: &Value) -> Option<Vec<Document>> {
    opts.get("array_filters")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|d| json_to_doc(d).ok()).collect())
}

/// `return` opt ("before"|"after") → ReturnDocument.
fn return_document(opts: &Value) -> Option<ReturnDocument> {
    match opts.get("return").and_then(Value::as_str) {
        Some("before") => Some(ReturnDocument::Before),
        Some("after") => Some(ReturnDocument::After),
        _ => None,
    }
}

async fn op_update_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let update = json_to_doc(&opts["update"])?;
    let mut uo = UpdateOptions::default();
    uo.upsert = opts["upsert"].as_bool();
    uo.array_filters = opt_array_filters(&opts);
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.update_one(filter, update).with_options(uo).await?;
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
    let mut uo = UpdateOptions::default();
    uo.upsert = opts["upsert"].as_bool();
    uo.array_filters = opt_array_filters(&opts);
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.update_many(filter, update).with_options(uo).await?;
    Ok(json!({
        "matched_count": r.matched_count as i64,
        "modified_count": r.modified_count as i64,
        "upserted_id": serde_json::to_value(r.upserted_id)?,
    }))
}

async fn op_replace_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let replacement = json_to_doc(&opts["doc"])?;
    let mut ro = ReplaceOptions::default();
    ro.upsert = opts["upsert"].as_bool();
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll
        .replace_one(filter, replacement)
        .with_options(ro)
        .await?;
    Ok(json!({
        "matched_count": r.matched_count as i64,
        "modified_count": r.modified_count as i64,
        "upserted_id": serde_json::to_value(r.upserted_id)?,
    }))
}

// ── findAndModify family ─────────────────────────────────────────────────────

async fn op_find_one_and_update(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let update = json_to_doc(&opts["update"])?;
    let mut o = FindOneAndUpdateOptions::default();
    o.upsert = opts["upsert"].as_bool();
    o.return_document = return_document(&opts);
    o.sort = opt_doc(&opts, "sort");
    o.projection = opt_doc(&opts, "projection");
    o.array_filters = opt_array_filters(&opts);
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll
        .find_one_and_update(filter, update)
        .with_options(o)
        .await?;
    Ok(json!({"doc": match r { Some(d) => doc_to_json(&d)?, None => Value::Null }}))
}

async fn op_find_one_and_replace(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let replacement = json_to_doc(&opts["doc"])?;
    let mut o = FindOneAndReplaceOptions::default();
    o.upsert = opts["upsert"].as_bool();
    o.return_document = return_document(&opts);
    o.sort = opt_doc(&opts, "sort");
    o.projection = opt_doc(&opts, "projection");
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll
        .find_one_and_replace(filter, replacement)
        .with_options(o)
        .await?;
    Ok(json!({"doc": match r { Some(d) => doc_to_json(&d)?, None => Value::Null }}))
}

async fn op_find_one_and_delete(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let filter = json_to_doc(&opts["filter"])?;
    let mut o = FindOneAndDeleteOptions::default();
    o.sort = opt_doc(&opts, "sort");
    o.projection = opt_doc(&opts, "projection");
    let coll = c.database(&db).collection::<Document>(&coll);
    let r = coll.find_one_and_delete(filter).with_options(o).await?;
    Ok(json!({"doc": match r { Some(d) => doc_to_json(&d)?, None => Value::Null }}))
}

// ── distinct / estimated count / admin / run_command ─────────────────────────

async fn op_distinct(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let field = opts["field"]
        .as_str()
        .ok_or_else(|| anyhow!("missing field"))?
        .to_string();
    let filter = opt_doc(&opts, "filter").unwrap_or_default();
    let coll = c.database(&db).collection::<Document>(&coll);
    let values = coll.distinct(&field, filter).await?;
    let json_values: Vec<Value> = values
        .iter()
        .map(|b| serde_json::to_value(b).unwrap_or(Value::Null))
        .collect();
    Ok(json!({"values": json_values}))
}

async fn op_estimated_count(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let coll = c.database(&db).collection::<Document>(&coll);
    let n = coll.estimated_document_count().await?;
    Ok(json!({"value": n as i64}))
}

async fn op_create_collection(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().ok_or_else(|| anyhow!("missing db"))?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    c.database(db).create_collection(name).await?;
    Ok(json!({"ok": true, "created": name}))
}

async fn op_drop_collection(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    c.database(&db).collection::<Document>(&coll).drop().await?;
    Ok(json!({"ok": true, "dropped": coll}))
}

async fn op_run_command(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().ok_or_else(|| anyhow!("missing db"))?;
    let command = json_to_doc(&opts["command"])?;
    if command.is_empty() {
        return Err(anyhow!("missing command (a non-empty command document)"));
    }
    let r = c.database(db).run_command(command).await?;
    Ok(json!({"result": doc_to_json(&r)?}))
}

async fn op_rename_collection(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let to = opts["to"]
        .as_str()
        .ok_or_else(|| anyhow!("missing to (new name)"))?;
    let drop_target = opts["drop_target"].as_bool().unwrap_or(false);
    // renameCollection lives on the admin DB and takes fully-qualified namespaces.
    let cmd = json_to_doc(&json!({
        "renameCollection": format!("{}.{}", db, coll),
        "to": format!("{}.{}", db, to),
        "dropTarget": drop_target,
    }))?;
    c.database("admin").run_command(cmd).await?;
    Ok(json!({"ok": true, "renamed": to}))
}

async fn op_coll_stats(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let cmd = json_to_doc(&json!({"collStats": coll}))?;
    let r = c.database(&db).run_command(cmd).await?;
    Ok(json!({"stats": doc_to_json(&r)?}))
}

async fn op_db_stats(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().ok_or_else(|| anyhow!("missing db"))?;
    let cmd = json_to_doc(&json!({"dbStats": 1}))?;
    let r = c.database(db).run_command(cmd).await?;
    Ok(json!({"stats": doc_to_json(&r)?}))
}

async fn op_explain(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let verbosity = opts["verbosity"].as_str().unwrap_or("queryPlanner");
    // Explain a find (default) or an aggregate when a pipeline is supplied.
    let inner = if let Some(pipeline) = opts.get("pipeline").filter(|p| p.is_array()) {
        json!({ "aggregate": coll, "pipeline": pipeline, "cursor": {} })
    } else {
        let filter = match opts.get("filter") {
            Some(v) if !v.is_null() => v.clone(),
            _ => json!({}),
        };
        json!({ "find": coll, "filter": filter })
    };
    let cmd = json_to_doc(&json!({ "explain": inner, "verbosity": verbosity }))?;
    let r = c.database(&db).run_command(cmd).await?;
    Ok(json!({ "explain": doc_to_json(&r)? }))
}

async fn op_server_status(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().unwrap_or("admin");
    let cmd = json_to_doc(&json!({"serverStatus": 1}))?;
    let r = c.database(db).run_command(cmd).await?;
    Ok(json!({ "status": doc_to_json(&r)? }))
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

#[no_mangle]
pub extern "C" fn mongo__find_one_and_update(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_find_one_and_update)
}

#[no_mangle]
pub extern "C" fn mongo__find_one_and_replace(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_find_one_and_replace)
}

#[no_mangle]
pub extern "C" fn mongo__find_one_and_delete(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_find_one_and_delete)
}

#[no_mangle]
pub extern "C" fn mongo__distinct(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_distinct)
}

#[no_mangle]
pub extern "C" fn mongo__estimated_count(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_estimated_count)
}

#[no_mangle]
pub extern "C" fn mongo__create_collection(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_create_collection)
}

#[no_mangle]
pub extern "C" fn mongo__drop_collection(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_drop_collection)
}

#[no_mangle]
pub extern "C" fn mongo__run_command(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_run_command)
}

#[no_mangle]
pub extern "C" fn mongo__rename_collection(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_rename_collection)
}

#[no_mangle]
pub extern "C" fn mongo__coll_stats(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_coll_stats)
}

#[no_mangle]
pub extern "C" fn mongo__db_stats(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_db_stats)
}

#[no_mangle]
pub extern "C" fn mongo__explain(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_explain)
}

#[no_mangle]
pub extern "C" fn mongo__server_status(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_server_status)
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
        let (db, coll) = parse_target(&json!({"target": "shop.events.2026"}), None).unwrap();
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
        let (db, coll) =
            parse_target(&json!({"target": "explicit.coll"}), Some("fallback")).unwrap();
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
    ///
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

    /// `ffi_call_async` silently substitutes `Value::Null` for any args
    /// bytes that fail `serde_json::from_slice` (see the `unwrap_or` at
    /// the input-parsing site). That means a stryke marshalling bug
    /// that hands the cdylib non-JSON bytes (truncated buffer, wrong
    /// encoding, accidental Pascal-string prefix) is invisible — the
    /// handler runs as if the caller passed no args at all, and the
    /// downstream error is the wrong one ("missing target" rather than
    /// "malformed args").
    ///
    /// Pin the current (silent-swallow) behavior so any future change
    /// that surfaces the parse error gets attention here and the boss
    /// can decide whether to keep the silent fallback or convert it to
    /// `{"error":"malformed JSON args"}`. We pick a handler that
    /// observes whether the input is `Null` vs anything else, so we
    /// can detect the substitution without depending on op-specific
    /// error wording. Worth a hand-rolled test because the only other
    /// FFI test exercises the panic path, not the input-parse path.
    #[test]
    fn ffi_call_async_silently_substitutes_null_for_malformed_json_args() {
        // `not json at all {` is intentionally not parseable as JSON.
        // CString::new still accepts it (no interior NULs).
        let bad = CString::new("not json at all {").unwrap();
        let ptr = ffi_call_async(bad.as_ptr(), |v: Value| async move {
            // Echo back whether the input was Null or not. If the
            // substitution stopped happening (e.g. the unwrap_or got
            // replaced with a real error envelope), `received_null`
            // would be `false`/absent and this assert would fire.
            Ok(json!({ "received_null": v.is_null() }))
        });
        assert!(
            !ptr.is_null(),
            "ffi_call_async must always return a CString"
        );
        let s = unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() };
        let out: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            out["received_null"],
            json!(true),
            "ffi_call_async no longer substitutes Value::Null on malformed JSON args — \
             decide whether to keep the silent-swallow or convert to an explicit error \
             envelope, then update this test"
        );
        unsafe { stryke_free_cstring(ptr as *mut c_char) };
    }

    /// MongoDB extended-JSON `{"$oid": "..."}` is interpreted by
    /// `bson::to_bson` as an `ObjectId` BSON value, not as a regular
    /// nested document with a `$oid` field. The serializer path then
    /// re-emits that ObjectId in the same `{"$oid": "..."}` extended
    /// form. The risk this catches: a future "helpful" refactor that
    /// switches `doc_to_json` away from `serde_json::to_value(d)` to
    /// e.g. canonical JSON or relaxed-mode JSON would silently change
    /// the shape stryke users see — breaking every `Mongo::find` /
    /// `Mongo::insert_one` round-trip that involves `_id`.
    ///
    /// Pinning the current shape so that the contract is explicit:
    /// extended-JSON in => extended-JSON out, byte-equal. If the
    /// driver bson crate ever changes its default serializer mode,
    /// this catches it on the next CI run rather than silently in
    /// production stryke scripts.
    #[test]
    fn json_to_doc_round_trips_object_id_extended_json_byte_equal() {
        let oid_hex = "507f1f77bcf86cd799439011";
        let input = json!({"_id": {"$oid": oid_hex}});
        let d = json_to_doc(&input).unwrap();
        // Confirm bson actually interpreted `$oid` as an ObjectId, not
        // a nested document. If a future refactor of `json_to_doc`
        // switches to a serializer that keeps `$oid` as a sub-doc,
        // this assert tells us the round-trip semantics changed.
        let id_bson = d.get("_id").expect("missing _id");
        assert!(
            matches!(id_bson, Bson::ObjectId(_)),
            "expected _id to deserialize as Bson::ObjectId, got {id_bson:?} — \
             a refactor changed json_to_doc's extended-JSON interpretation"
        );
        // And round-trip: doc_to_json must re-emit the `$oid` form so
        // the user-visible shape stays the same on the way out.
        let back = doc_to_json(&d).unwrap();
        assert_eq!(
            back["_id"]["$oid"].as_str().unwrap(),
            oid_hex,
            "doc_to_json no longer re-emits ObjectId as $oid extended JSON — \
             every stryke find/insert round-trip just broke"
        );
    }

    /// `json_to_doc` must reject EVERY non-object non-null JSON primitive
    /// with the same "expected JSON object" error. The existing coverage
    /// only checks arrays — but `bson::to_bson` will happily convert a
    /// JSON string into `Bson::String`, a number into `Bson::Int64`, a
    /// bool into `Bson::Boolean`, etc., each of which lands in the `_ =>`
    /// arm. A future "smart" refactor that special-cases say `Value::Bool`
    /// as a shorthand for `{$truthy: true}` would silently change filter
    /// semantics for every `Mongo::find` / `Mongo::count` call where the
    /// user fat-fingered `filter: true` instead of `filter: {active:
    /// true}`. Pinning that all four primitive shapes still error keeps
    /// the contract explicit: only `{}` and `null` are accepted.
    ///
    /// Catches: any future divergence between primitive-rejection paths
    /// (someone fixing one without the others), or relaxation of the
    /// rejection that would make malformed filters silently match.
    #[test]
    fn json_to_doc_rejects_all_non_object_primitives_uniformly() {
        let cases = [
            ("bool true", json!(true)),
            ("bool false", json!(false)),
            ("int", json!(42)),
            ("negative int", json!(-7)),
            ("float", json!(1.5)),
            ("string", json!("not a doc")),
            ("empty string", json!("")),
            ("array", json!([1, 2, 3])),
            ("empty array", json!([])),
        ];
        for (label, v) in cases.iter() {
            let err = json_to_doc(v)
                .err()
                .unwrap_or_else(|| panic!("{label}: expected error, got Ok"))
                .to_string();
            assert!(
                err.contains("expected JSON object"),
                "{label}: error wording drifted — got {err:?}; \
                 a refactor likely special-cased this primitive shape \
                 and broke filter-rejection uniformity",
            );
        }
    }

    /// MongoDB extended-JSON `{"$date": {"$numberLong": "<ms>"}}` is the
    /// canonical wire form for timestamps. `bson::to_bson` interprets it
    /// as `Bson::DateTime` and `serde_json::to_value` re-emits the same
    /// extended form on the way out. This is the date-side analogue of
    /// the existing `$oid` round-trip test — both pins guard the
    /// driver's serializer mode (`relaxed` vs `canonical` vs `legacy`).
    ///
    /// Risk this catches: a bson-crate upgrade that flips the default to
    /// relaxed mode would emit `{"$date": "2026-06-10T..."}` (ISO string)
    /// instead of the `{"$numberLong": ...}` envelope, silently breaking
    /// every stryke script that round-trips `created_at` timestamps
    /// through `Mongo::find` → modify → `Mongo::update_one`. The user-
    /// visible shape changes WITHOUT a stryke-mongo version bump.
    ///
    /// Worth a hand-rolled test because the only existing extended-JSON
    /// pin (`$oid`) covers ObjectIds — dates have their own serializer
    /// mode and would not be caught by the ObjectId test.
    #[test]
    fn json_to_doc_round_trips_date_extended_json_byte_equal() {
        // 2026-06-10T00:00:00Z in epoch ms.
        let epoch_ms: i64 = 1_780_704_000_000;
        let input = json!({
            "created_at": {"$date": {"$numberLong": epoch_ms.to_string()}}
        });
        let d = json_to_doc(&input).unwrap();
        let dt_bson = d.get("created_at").expect("missing created_at");
        assert!(
            matches!(dt_bson, Bson::DateTime(_)),
            "expected created_at to deserialize as Bson::DateTime, got \
             {dt_bson:?} — a refactor changed json_to_doc's extended-JSON \
             date interpretation; every stryke timestamp round-trip just \
             changed shape",
        );
        let back = doc_to_json(&d).unwrap();
        // serde_json::to_value on a Bson::DateTime must re-emit the
        // `$date` envelope (canonical mode). The exact inner shape
        // (`$numberLong` vs ISO string) is what we're pinning — if the
        // driver flips to relaxed mode, `back["created_at"]["$date"]`
        // will be a string instead of an object and this assert fires.
        let date_field = &back["created_at"]["$date"];
        assert!(
            date_field.is_object(),
            "doc_to_json no longer re-emits DateTime as \
             {{\"$date\":{{\"$numberLong\":...}}}} canonical extended JSON \
             (got {date_field:?}) — bson serializer mode drifted to \
             relaxed; stryke scripts that pattern-match on $numberLong \
             will silently break",
        );
        assert_eq!(
            date_field["$numberLong"].as_str().unwrap(),
            epoch_ms.to_string(),
            "DateTime epoch_ms round-trip drifted from input",
        );
    }

    /// Field INSERTION ORDER must survive json → bson::Document → json.
    /// This is a silent-data-corruption contract, not a cosmetic one:
    /// `serde_json` is pulled with the `preserve_order` feature (Cargo.toml)
    /// so `Value::Object` is backed by an `IndexMap`, and `bson::Document`
    /// is itself order-preserving. If a future Cargo.toml edit drops
    /// `preserve_order` (or a refactor routes through a `BTreeMap`/`HashMap`
    /// intermediate), `doc_to_json` output would silently re-sort keys
    /// alphabetically. Every stryke `Mongo::find` consumer that pattern-
    /// matches positionally, prints `_id`-first, or diffs documents would
    /// break with NO version bump and NO error — the worst failure class.
    ///
    /// Uses a key set whose insertion order is the REVERSE of alphabetical
    /// (`z`, `m`, `a`, `_id`) so an accidental sort is unambiguously
    /// detectable: sorted output would be `_id, a, m, z`, which differs from
    /// the expected insertion order in every position.
    #[test]
    fn doc_to_json_preserves_field_insertion_order_not_sorted() {
        // Build via json! so the source ordering is explicit and reversed
        // vs. alphabetical. serde_json with preserve_order keeps this order.
        let input = json!({"z": 1, "m": 2, "a": 3, "_id": 4});
        let d = json_to_doc(&input).unwrap();

        // bson::Document iterates in insertion order.
        let doc_keys: Vec<&str> = d.keys().map(String::as_str).collect();
        assert_eq!(
            doc_keys,
            vec!["z", "m", "a", "_id"],
            "json_to_doc scrambled field order — a HashMap/BTreeMap crept \
             into the json→bson path (sorted would be [_id, a, m, z])",
        );

        // And back out: doc_to_json must NOT re-sort.
        let back = doc_to_json(&d).unwrap();
        let out_keys: Vec<&str> = back
            .as_object()
            .expect("doc_to_json must yield an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            out_keys,
            vec!["z", "m", "a", "_id"],
            "doc_to_json re-ordered fields — `preserve_order` was dropped \
             from serde_json or a sorting map intermediate was introduced; \
             stryke find() output silently re-sorts keys",
        );
    }

    /// Unicode keys AND values must survive the json → bson → json
    /// round-trip byte-for-byte. The json↔bson conversion crosses two
    /// serializers; a regression that truncated on bytes instead of chars
    /// (or mishandled surrogate pairs / combining marks) would corrupt
    /// real-world data: non-ASCII collection field names, emoji in user
    /// content, accented names. None of the existing round-trip tests use
    /// any non-ASCII codepoint — they're all `"ada"` / `"ny"` ASCII.
    ///
    /// Mixes: a CJK key, a combining-mark sequence (e + U+0301 = é as two
    /// codepoints, the classic byte-vs-char trap), an emoji that is a
    /// surrogate pair in UTF-16 (4 bytes UTF-8), and a NUL-free control-
    /// adjacent string. Asserts exact equality both ways.
    #[test]
    fn json_to_doc_round_trips_unicode_keys_and_values_byte_equal() {
        // "名前" = CJK key; value mixes combining é (e + U+0301), emoji,
        // and a right-to-left Arabic snippet.
        let combining_e_acute = "e\u{0301}"; // NOT precomposed U+00E9
        let value = format!("{combining_e_acute}-\u{1F680}-مرحبا");
        let input = json!({
            "名前": value,
            "emoji_key_\u{1F4BE}": "💾",
            "ascii": "plain",
        });
        let d = json_to_doc(&input).unwrap();

        // Key with a 4-byte-UTF-8 codepoint must be retrievable verbatim.
        assert_eq!(
            d.get_str("名前").unwrap(),
            value,
            "CJK-keyed unicode value corrupted in json→bson",
        );
        assert_eq!(d.get_str("emoji_key_\u{1F4BE}").unwrap(), "💾");

        // Full round-trip back to json must be byte-identical to input.
        let back = doc_to_json(&d).unwrap();
        assert_eq!(
            back, input,
            "unicode round-trip json→bson→json was not byte-equal — a \
             serializer in the path is truncating on bytes or mangling \
             surrogate pairs / combining marks",
        );
        // Explicitly confirm the combining sequence was NOT silently
        // normalized to the precomposed form (which would change byte len
        // and break exact-match queries).
        assert!(
            back["名前"].as_str().unwrap().starts_with("e\u{0301}"),
            "combining-mark sequence was NFC-normalized — exact-match \
             filters on this value would miss in mongo",
        );
    }

    /// Nested arrays-of-documents must convert recursively. Existing
    /// coverage only round-trips FLAT scalar fields (`json_to_doc_to_json_
    /// preserves_basic_fields`) — nothing exercises a `Bson::Array` of
    /// `Bson::Document`, which is the shape of every real mongo doc with an
    /// embedded subdocument list (e.g. `order.items`, `user.addresses`).
    ///
    /// Catches: a refactor that special-cased top-level objects but lost
    /// recursion into array elements, or that flattened nested docs — both
    /// would corrupt `Mongo::insert_one` payloads with embedded arrays
    /// while leaving the flat-field tests green.
    #[test]
    fn json_to_doc_round_trips_nested_array_of_documents() {
        let input = json!({
            "order": "o-1",
            "items": [
                {"sku": "a", "qty": 2},
                {"sku": "b", "qty": 1, "tags": ["x", "y"]},
            ],
            "meta": {"nested": {"deep": true}},
        });
        let d = json_to_doc(&input).unwrap();

        // The array element must be a real BSON array of subdocuments,
        // not a stringified or flattened blob.
        let items = d.get_array("items").expect("items not a BSON array");
        assert_eq!(items.len(), 2);
        match &items[1] {
            Bson::Document(sub) => {
                assert_eq!(sub.get_i64("qty").unwrap(), 1);
                let tags = sub.get_array("tags").expect("tags not array");
                assert_eq!(tags.len(), 2);
            }
            other => panic!("nested item not a document: {other:?}"),
        }

        // Full recursive round-trip back to json must equal the input.
        let back = doc_to_json(&d).unwrap();
        assert_eq!(
            back, input,
            "nested array-of-documents round-trip diverged — recursive \
             json↔bson conversion lost or reshaped embedded subdocuments",
        );
    }

    // ── new-surface option helpers ───────────────────────────────────────────

    #[test]
    fn return_document_maps_before_after_only() {
        assert!(matches!(
            return_document(&json!({"return": "after"})),
            Some(ReturnDocument::After)
        ));
        assert!(matches!(
            return_document(&json!({"return": "before"})),
            Some(ReturnDocument::Before)
        ));
        // Absent or bogus → None (driver default).
        assert!(return_document(&json!({})).is_none());
        assert!(return_document(&json!({"return": "sideways"})).is_none());
    }

    #[test]
    fn opt_array_filters_parses_array_of_docs() {
        let af = opt_array_filters(&json!({"array_filters": [{"x.y": {"$gt": 3}}]}))
            .expect("filters built");
        assert_eq!(af.len(), 1);
        assert!(af[0].contains_key("x.y"));
        // Absent → None.
        assert!(opt_array_filters(&json!({})).is_none());
    }

    #[test]
    fn opt_doc_returns_none_for_absent_or_null() {
        assert!(opt_doc(&json!({}), "sort").is_none());
        assert!(opt_doc(&json!({"sort": null}), "sort").is_none());
        let d = opt_doc(&json!({"sort": {"ts": -1}}), "sort").expect("sort doc");
        // serde_json → bson may pick i32 or i64 for the literal; accept either.
        let ts = d
            .get("ts")
            .and_then(Bson::as_i64)
            .or_else(|| d.get_i32("ts").ok().map(i64::from));
        assert_eq!(ts, Some(-1));
    }
}
