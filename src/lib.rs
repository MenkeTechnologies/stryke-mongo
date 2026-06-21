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

/// Read a flag that stryke may send as a JSON bool OR a 0/1 number (stryke's
/// `to_json` does not always emit JSON booleans). `Some(true)`/`Some(false)`
/// when the key is present, `None` when absent — so callers can tell "not set"
/// from "set false". Mirrors the `srv` handling in `build_connection_string`.
fn opt_truthy(opts: &Value, key: &str) -> Option<bool> {
    match opts.get(key) {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::Number(n)) => Some(n.as_f64().is_some_and(|x| x != 0.0)),
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

async fn op_drop_database(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().ok_or_else(|| anyhow!("missing db"))?;
    c.database(db).drop().await?;
    Ok(json!({"ok": true, "dropped": db}))
}

async fn op_drop_indexes(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    c.database(&db)
        .collection::<Document>(&coll)
        .drop_indexes()
        .await?;
    Ok(json!({"ok": true}))
}

/// Database-level aggregation (`db.aggregate(...)`) for admin pipelines such as
/// `$currentOp` and `$listLocalSessions` that run against the database rather
/// than a single collection. opts: `db` (required), `pipeline` (array).
async fn op_aggregate_db(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().ok_or_else(|| anyhow!("missing db"))?;
    let pipeline_v = opts["pipeline"]
        .as_array()
        .ok_or_else(|| anyhow!("missing pipeline (array)"))?;
    let pipeline: Result<Vec<Document>> = pipeline_v.iter().map(json_to_doc).collect();
    let mut cursor = c
        .database(db)
        .aggregate(pipeline?)
        .with_options(AggregateOptions::default())
        .await?;
    let mut out: Vec<Value> = Vec::new();
    while let Some(d) = cursor.try_next().await? {
        out.push(doc_to_json(&d)?);
    }
    Ok(json!({"docs": out}))
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

async fn op_create_indexes(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let specs = opts["indexes"]
        .as_array()
        .ok_or_else(|| anyhow!("missing indexes (array of {{ keys, name?, unique? }})"))?;
    if specs.is_empty() {
        return Err(anyhow!("indexes must be non-empty"));
    }
    // Build a createIndexes command from each spec: keys (required) + the
    // optional name/unique flags carried straight through.
    let mut index_docs = Vec::with_capacity(specs.len());
    for s in specs {
        let keys = json_to_doc(&s["keys"])?;
        if keys.is_empty() {
            return Err(anyhow!("each index needs a non-empty keys object"));
        }
        let mut idx = json!({ "key": doc_to_json(&keys)? });
        if let Some(name) = s["name"].as_str() {
            idx["name"] = json!(name);
        }
        if let Some(u) = s["unique"].as_bool() {
            idx["unique"] = json!(u);
        }
        index_docs.push(idx);
    }
    let cmd = json_to_doc(&json!({ "createIndexes": coll, "indexes": index_docs }))?;
    let r = c.database(&db).run_command(cmd).await?;
    Ok(json!({ "result": doc_to_json(&r)? }))
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

/// Full collection specifications for $db — the richer companion to
/// `list_collections`, which returns only the names. Each spec carries the
/// collection `name`, `type` ("collection"|"view"|"timeseries"), `options`
/// (capped/size/validator/viewOn/pipeline), and `info` (readOnly, uuid). opts:
/// `db` (required), optional `filter` (a `listCollections` filter document, e.g.
/// `{ "type": "view" }`). Returns `{collections: [ {…}, … ]}`.
async fn op_collection_specs(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let db = opts["db"].as_str().ok_or_else(|| anyhow!("missing db"))?;
    let database = c.database(db);
    let action = database.list_collections();
    let action = match opt_doc(&opts, "filter") {
        Some(f) => action.filter(f),
        None => action,
    };
    let mut cursor = action.await?;
    let mut out: Vec<Value> = Vec::new();
    while let Some(spec) = cursor.try_next().await? {
        out.push(serde_json::to_value(spec)?);
    }
    Ok(json!({"collections": out}))
}

/// Run the `validate` command on $target ("db.coll") — server-side integrity
/// check of a collection and its indexes. opts: `full` (bool → deep scan of
/// every document; slower, holds a lock) and `repair` (bool → attempt to fix
/// inconsistencies; standalone only). Returns `{result}` with the raw validate
/// document (`valid`, `nrecords`, `nIndexes`, `errors`, `warnings`, …).
async fn op_validate(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let (db, coll) = parse_target(&opts, None)?;
    let mut cmd = json!({ "validate": coll });
    if let Some(full) = opt_truthy(&opts, "full") {
        cmd["full"] = json!(full);
    }
    if let Some(repair) = opt_truthy(&opts, "repair") {
        cmd["repair"] = json!(repair);
    }
    let r = c.database(&db).run_command(json_to_doc(&cmd)?).await?;
    Ok(json!({ "result": doc_to_json(&r)? }))
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

// ── pure helpers (no connection) ─────────────────────────────────────────────

/// RFC 3986 percent-decode for userinfo / database / option values in a
/// MongoDB URI. Invalid escapes are left verbatim.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-encode every byte that is not an RFC 3986 unreserved char
/// (`A-Za-z0-9-._~`). The exact inverse of `percent_decode` for the userinfo and
/// database components of a MongoDB URI, so a parse→build round-trip is stable.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Parse a MongoDB connection string `mongodb[+srv]://[user[:pass]@]host[:port]
/// [,host2…][/[db][?opts]]` into its parts. Unlike a SQL DSN this carries a
/// host LIST. Userinfo / db / option values are percent-decoded. Pure — opens
/// nothing and never resolves `+srv` DNS.
fn op_parse_connection_string(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .or_else(|| opts.get("dsn"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| anyhow!("not a MongoDB URI (missing `://`): {uri}"))?;
    let srv = match scheme {
        "mongodb" => false,
        "mongodb+srv" => true,
        other => {
            return Err(anyhow!(
                "unsupported scheme `{other}` (want mongodb|mongodb+srv)"
            ))
        }
    };
    let (authority_path, query) = match rest.split_once('?') {
        Some((ap, q)) => (ap, Some(q)),
        None => (rest, None),
    };
    let (authority, path) = match authority_path.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (authority_path, None),
    };
    let (userinfo, hosts_str) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (user, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(percent_decode(u)), Some(percent_decode(p))),
            None => (Some(percent_decode(ui)), None),
        },
        None => (None, None),
    };
    let hosts: Vec<Value> = hosts_str
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|hp| match hp.rsplit_once(':') {
            Some((h, p)) => match p.parse::<u32>() {
                Ok(port) => json!({"host": h, "port": port}),
                Err(_) => json!({"host": hp, "port": Value::Null}),
            },
            None => json!({"host": hp, "port": Value::Null}),
        })
        .collect();
    let database = path.filter(|p| !p.is_empty()).map(percent_decode);
    let mut params = serde_json::Map::new();
    if let Some(q) = query {
        for pair in q.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            params.insert(percent_decode(k), json!(percent_decode(v)));
        }
    }
    Ok(json!({
        "scheme": scheme,
        "srv": srv,
        "user": user,
        "password": password,
        "hosts": hosts,
        "database": database,
        "params": Value::Object(params),
    }))
}

/// Assemble a MongoDB connection URI from parts — the inverse of
/// `parse_connection_string`. opts: `hosts` (required, array of `{host, port?}`
/// or bare host strings), `srv` (bool) or `scheme`, `user`, `password`,
/// `database`, and `params` (object). User/password/database are percent-encoded
/// and param keys sorted for a stable round-trip. Produces
/// `mongodb[+srv]://[user[:pw]@]host[,…][/db][?k=v…]`. Pure.
fn op_build_connection_string(opts: Value) -> Result<Value> {
    // stryke sends booleans as 0/1 numbers, so accept any truthy `srv`.
    let srv_flag = opts.get("srv").is_some_and(|v| match v {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|x| x != 0.0),
        _ => false,
    });
    let srv = srv_flag || opts.get("scheme").and_then(Value::as_str) == Some("mongodb+srv");
    let scheme = if srv { "mongodb+srv" } else { "mongodb" };
    let hosts = opts
        .get("hosts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing hosts (array of {{host, port}} or host strings)"))?;
    if hosts.is_empty() {
        return Err(anyhow!("hosts is empty"));
    }
    let mut host_strs: Vec<String> = Vec::with_capacity(hosts.len());
    for h in hosts {
        if let Some(s) = h.as_str() {
            host_strs.push(s.to_string());
        } else {
            let host = h
                .get("host")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("host entry missing host"))?;
            match h.get("port").and_then(Value::as_u64) {
                Some(port) => host_strs.push(format!("{host}:{port}")),
                None => host_strs.push(host.to_string()),
            }
        }
    }
    let opt = |k: &str| {
        opts.get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    let mut uri = format!("{scheme}://");
    if let Some(user) = opt("user") {
        uri.push_str(&percent_encode(user));
        if let Some(pw) = opt("password") {
            uri.push(':');
            uri.push_str(&percent_encode(pw));
        }
        uri.push('@');
    }
    uri.push_str(&host_strs.join(","));
    let database = opt("database");
    // Collect + sort params for a deterministic, round-trippable query string.
    let params: Vec<(String, String)> = match opts.get("params").and_then(Value::as_object) {
        Some(m) => {
            let mut v: Vec<(String, String)> = m
                .iter()
                .map(|(k, val)| (k.clone(), val.as_str().unwrap_or("").to_string()))
                .collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v
        }
        None => Vec::new(),
    };
    // A `/` must precede `?options` even when there is no database path.
    if database.is_some() || !params.is_empty() {
        uri.push('/');
        if let Some(db) = database {
            uri.push_str(&percent_encode(db));
        }
    }
    if !params.is_empty() {
        uri.push('?');
        let query: Vec<String> = params
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect();
        uri.push_str(&query.join("&"));
    }
    Ok(json!({ "uri": uri }))
}

/// Non-throwing structural validator for a MongoDB connection URI — the
/// predicate companion to `parse_connection_string` (which throws). Beyond a
/// successful parse it enforces the host rules the parser is lax about: a
/// standard `mongodb://` URI needs at least one host, and a `mongodb+srv://` URI
/// must have exactly one host and no port (per the DNS-seed-list format). opts:
/// `uri` (or `dsn`) required. Returns `{uri, valid, reason}` — `reason` is null
/// when valid. Pure.
fn op_valid_connection_string(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .or_else(|| opts.get("dsn"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let reason: Option<String> = match op_parse_connection_string(opts.clone()) {
        Err(e) => Some(e.to_string()),
        Ok(parsed) => {
            let hosts = parsed.get("hosts").and_then(Value::as_array);
            let n = hosts.map_or(0, Vec::len);
            let srv = parsed.get("srv").and_then(Value::as_bool).unwrap_or(false);
            if n == 0 {
                Some("at least one host is required".into())
            } else if srv && n != 1 {
                Some("mongodb+srv requires exactly one host".into())
            } else if srv && hosts.is_some_and(|h| !h[0].get("port").is_none_or(Value::is_null)) {
                Some("mongodb+srv must not specify a port".into())
            } else {
                None
            }
        }
    };
    Ok(json!({ "uri": uri, "valid": reason.is_none(), "reason": reason }))
}

/// Replace the password in a MongoDB connection URI with a mask (`***` by
/// default) so the URI can be safely logged. Only the `user:password@` segment
/// is rewritten — the scheme, hosts, database, and query options are preserved
/// byte-for-byte (unlike a parse-then-build round trip, which would normalize
/// param order and encoding). A URI with no password is returned unchanged.
/// opts: `uri` (or `dsn`) required, optional `mask` (default `***`). Returns
/// `{redacted, had_password}`. Pure.
fn op_redact_connection_string(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .or_else(|| opts.get("dsn"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let mask = opts.get("mask").and_then(Value::as_str).unwrap_or("***");
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| anyhow!("not a MongoDB URI (missing `://`): {uri}"))?;
    // The authority ends at the first '/' or '?' after the scheme.
    let auth_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(auth_end);
    let (redacted, had_password) = match authority.rsplit_once('@') {
        // Split userinfo on the FIRST ':' — a password may itself be empty.
        Some((userinfo, hosts)) => match userinfo.split_once(':') {
            Some((user, _pw)) => (format!("{scheme}://{user}:{mask}@{hosts}{tail}"), true),
            None => (uri.to_string(), false),
        },
        None => (uri.to_string(), false),
    };
    Ok(json!({ "redacted": redacted, "had_password": had_password }))
}

/// Split a `db.collection` namespace on its FIRST dot (collection names may
/// contain dots; database names may not). Pure.
fn op_parse_namespace(opts: Value) -> Result<Value> {
    let ns = opts
        .get("namespace")
        .or_else(|| opts.get("target"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing namespace (db.collection)"))?;
    let dot = ns
        .find('.')
        .ok_or_else(|| anyhow!("namespace `{ns}` has no `db.` prefix"))?;
    Ok(json!({
        "db": &ns[..dot],
        "collection": &ns[dot + 1..],
    }))
}

/// Build a `db.collection` namespace from parts — the inverse of
/// `parse_namespace`. opts: `db` and `collection` (both required, non-empty).
/// The db must not contain a `.` (MongoDB forbids it, and `parse_namespace`
/// splits on the first dot, so a dotted db would not round-trip). Pure.
fn op_build_namespace(opts: Value) -> Result<Value> {
    let db = opts
        .get("db")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing db"))?;
    let collection = opts
        .get("collection")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing collection"))?;
    if db.contains('.') {
        return Err(anyhow!("db name must not contain `.`: {db}"));
    }
    Ok(json!({"namespace": format!("{db}.{collection}")}))
}

/// Validate a MongoDB collection name against the server's documented hard
/// rules: not empty, no `$`, no null character, and not the reserved `system.`
/// prefix. When a `db` is supplied, the combined `db.collection` namespace must
/// also be ≤ 255 bytes (the unsharded-collection limit). The "should start with
/// a letter/underscore" guidance is a convention the server does not enforce, so
/// it is not checked here. Returns `{name, valid, reason}`. Pure.
fn op_valid_collection_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason: Option<&str> = if name.is_empty() {
        Some("must not be empty")
    } else if name.contains('$') {
        Some("must not contain '$'")
    } else if name.contains('\0') {
        Some("must not contain a null character")
    } else if name.starts_with("system.") {
        Some("must not begin with the reserved `system.` prefix")
    } else if matches!(opts.get("db").and_then(Value::as_str), Some(db) if db.len() + 1 + name.len() > 255)
    {
        Some("namespace (db.collection) must be at most 255 bytes")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a MongoDB database name (distinct rules from a collection name): it
/// must not be empty, must be fewer than 64 characters, and must not contain a
/// null character or any of `/ \ . " $ * < > : | ?` or a space — the
/// cross-platform restricted set. Returns `{name, valid, reason}`. Pure.
fn op_valid_database_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    const FORBIDDEN: &[char] = &['/', '\\', '.', ' ', '"', '$', '*', '<', '>', ':', '|', '?'];
    let reason: Option<&str> = if name.is_empty() {
        Some("must not be empty")
    } else if name.chars().count() >= 64 {
        Some("must be fewer than 64 characters")
    } else if name.contains('\0') {
        Some("must not contain a null character")
    } else if name.chars().any(|c| FORBIDDEN.contains(&c)) {
        Some("must not contain a space or any of: / \\ . \" $ * < > : | ?")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a MongoDB document field name (BSON key): must not be empty, must not
/// start with `$` (reserved for operators), must not contain `.` (the path
/// separator), and must not contain a null character. opts: `name` (required).
/// Returns `{name, valid, reason}`. Pure.
fn op_valid_field_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason: Option<&str> = if name.is_empty() {
        Some("must not be empty")
    } else if name.starts_with('$') {
        Some("must not start with '$' (reserved for operators)")
    } else if name.contains('.') {
        Some("must not contain '.' (the path separator)")
    } else if name.contains('\0') {
        Some("must not contain a null character")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a full MongoDB namespace `database.collection` in one call — splits on
/// the first `.` (a database name can't contain `.`, a collection name can), then
/// validates the database part with `valid_database_name` and the collection part
/// with `valid_collection_name`, which also enforces the combined 255-byte
/// namespace limit. A value with no `.` is rejected. opts: `namespace` (or `name`,
/// required). Returns `{namespace, valid, reason, database, collection}`. Pure.
fn op_valid_namespace(opts: Value) -> Result<Value> {
    let ns = opts
        .get("namespace")
        .or_else(|| opts.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing namespace"))?;
    let (db, coll) = match ns.split_once('.') {
        Some(parts) => parts,
        None => {
            return Ok(json!({
                "namespace": ns,
                "valid": false,
                "reason": "must be of the form database.collection",
                "database": Value::Null,
                "collection": Value::Null,
            }))
        }
    };
    // Reuse the per-part validators (collection validation also checks the 255-byte
    // namespace limit when handed the db).
    let db_v = op_valid_database_name(json!({ "name": db }))?;
    let coll_v = op_valid_collection_name(json!({ "name": coll, "db": db }))?;
    let reason = if !db_v["valid"].as_bool().unwrap_or(false) {
        Some(format!(
            "database name: {}",
            db_v["reason"].as_str().unwrap_or("invalid")
        ))
    } else if !coll_v["valid"].as_bool().unwrap_or(false) {
        Some(format!(
            "collection name: {}",
            coll_v["reason"].as_str().unwrap_or("invalid")
        ))
    } else {
        None
    };
    Ok(json!({
        "namespace": ns,
        "valid": reason.is_none(),
        "reason": reason,
        "database": db,
        "collection": coll,
    }))
}

/// Escape the PCRE regular-expression metacharacters in `value` so it matches
/// itself literally inside a MongoDB `$regex` query — each of
/// `. ^ $ * + ? ( ) [ ] { } | \` is backslash-prefixed. Use it to build a literal
/// or prefix `$regex` from user input without the input acting as a pattern (e.g.
/// `{ name: { $regex: "^" + escape_regex(s) } }` for a prefix match). opts:
/// `value` (required). Returns `{value, escaped}`. Pure.
fn op_escape_regex(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if matches!(
            c,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    Ok(json!({ "value": value, "escaped": out }))
}

/// Recover the literal from an `escape_regex` output — the inverse of
/// `escape_regex`. Each `\<metacharacter>` (one of `. ^ $ * + ? ( ) [ ] { } | \`)
/// collapses back to the bare metacharacter. An unescaped metacharacter or a
/// backslash before a non-metacharacter (or a dangling trailing backslash) means
/// the input is a real regex pattern, not an `escape_regex` output, so both are
/// rejected — making `unescape_regex(escape_regex(s)) == s` for every `s`. opts:
/// `escaped` (or `value`). Returns `{escaped, value}`. Pure.
fn op_unescape_regex(opts: Value) -> Result<Value> {
    let escaped = opts
        .get("escaped")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing escaped"))?;
    const META: &[char] = &[
        '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\',
    ];
    let mut out = String::with_capacity(escaped.len());
    let mut chars = escaped.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some(m) if META.contains(&m) => out.push(m),
                Some(other) => {
                    return Err(anyhow!(
                        "invalid escape `\\{other}` (not an escape_regex output)"
                    ))
                }
                None => return Err(anyhow!("dangling backslash (not an escape_regex output)")),
            }
        } else if META.contains(&c) {
            return Err(anyhow!(
                "unescaped metacharacter `{c}` (a real regex, not an escape_regex output)"
            ));
        } else {
            out.push(c);
        }
    }
    Ok(json!({ "escaped": escaped, "value": out }))
}

/// Whether a string is a valid 24-hex-char MongoDB ObjectId. Validation is
/// delegated to `bson::oid::ObjectId`, so it tracks the library exactly.
fn op_is_valid_objectid(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    Ok(json!({"id": id, "valid": bson::oid::ObjectId::parse_str(id).is_ok()}))
}

/// Generate a fresh ObjectId as a 24-hex string (time + counter + random, per
/// the BSON spec via `bson::oid::ObjectId::new`).
fn op_new_objectid(_opts: Value) -> Result<Value> {
    Ok(json!({"oid": bson::oid::ObjectId::new().to_hex()}))
}

/// Extract the creation timestamp embedded in an ObjectId's leading 4 bytes
/// (`ObjectId.getTimestamp()` in the shell). Delegates to
/// `bson::oid::ObjectId::timestamp`, so it tracks the library's decoding
/// exactly. Returns `{ epoch_seconds, epoch_millis, iso }`. Pure.
fn op_objectid_timestamp(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let oid =
        bson::oid::ObjectId::parse_str(id).map_err(|e| anyhow!("invalid ObjectId `{id}`: {e}"))?;
    let dt = oid.timestamp();
    let millis = dt.timestamp_millis();
    let iso = dt
        .try_to_rfc3339_string()
        .map_err(|e| anyhow!("ObjectId timestamp out of range: {e}"))?;
    Ok(json!({
        "epoch_seconds": millis / 1000,
        "epoch_millis": millis,
        "iso": iso,
    }))
}

/// Decompose an ObjectId into its three fields, per the MongoDB layout: a
/// 4-byte big-endian timestamp (seconds), a 5-byte per-process random value, and
/// a 3-byte big-endian incrementing counter. Where `objectid_timestamp` returns
/// only the time, this returns every part. opts: `id` (required). Returns
/// `{hex, epoch_seconds, iso, random, counter}`. Pure.
fn op_parse_objectid(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let oid =
        bson::oid::ObjectId::parse_str(id).map_err(|e| anyhow!("invalid ObjectId `{id}`: {e}"))?;
    let b = oid.bytes();
    let epoch_seconds = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let random = format!(
        "{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[4], b[5], b[6], b[7], b[8]
    );
    let counter = ((b[9] as u32) << 16) | ((b[10] as u32) << 8) | (b[11] as u32);
    let iso = oid
        .timestamp()
        .try_to_rfc3339_string()
        .map_err(|e| anyhow!("ObjectId timestamp out of range: {e}"))?;
    Ok(json!({
        "hex": oid.to_hex(),
        "epoch_seconds": epoch_seconds,
        "iso": iso,
        "random": random,
        "counter": counter,
    }))
}

/// Compare two ObjectIds in their natural (`_id` sort) order — the 12 bytes
/// compared lexicographically, which is timestamp-then-random-then-counter. This
/// is finer than comparing `objectid_timestamp`: two ObjectIds created in the same
/// second still have a defined order (via the counter), so this resolves ties a
/// second-resolution time comparison cannot. opts: `a`, `b` (required ObjectId hex
/// strings). Returns `{a, b, cmp: -1|0|1, equal, older}` where `older` is the
/// earlier-sorting id (`null` when equal). Pure.
fn op_objectid_compare(opts: Value) -> Result<Value> {
    let a = opts
        .get("a")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing a"))?;
    let b = opts
        .get("b")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing b"))?;
    let oa =
        bson::oid::ObjectId::parse_str(a).map_err(|e| anyhow!("invalid ObjectId `{a}`: {e}"))?;
    let ob =
        bson::oid::ObjectId::parse_str(b).map_err(|e| anyhow!("invalid ObjectId `{b}`: {e}"))?;
    let cmp = oa.bytes().cmp(&ob.bytes());
    let ord = match cmp {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    };
    let older = match ord {
        x if x < 0 => json!(a),
        x if x > 0 => json!(b),
        _ => Value::Null,
    };
    Ok(json!({
        "a": a,
        "b": b,
        "cmp": ord,
        "equal": ord == 0,
        "older": older,
    }))
}

/// Reconstruct an ObjectId from its three components — the inverse of
/// `parse_objectid`. The 12 bytes are `epoch_seconds` (4, big-endian) + `random`
/// (5 bytes, given as a 10-character hex string) + `counter` (3 bytes,
/// big-endian). `epoch_seconds` must fit a u32, `random` must be exactly 10 hex
/// digits, and `counter` must be 0..=16777215 (24 bits). opts: `epoch_seconds`,
/// `random`, `counter`. Returns `{oid, epoch_seconds, random, counter}`. Pure.
fn op_build_objectid(opts: Value) -> Result<Value> {
    let epoch = opts
        .get("epoch_seconds")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing epoch_seconds"))?;
    let secs = u32::try_from(epoch)
        .map_err(|_| anyhow!("epoch_seconds out of ObjectId range (0..=4294967295): {epoch}"))?;
    let random = opts
        .get("random")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing random (10 hex characters)"))?;
    if random.len() != 10 {
        return Err(anyhow!(
            "random must be 10 hex characters (5 bytes): `{random}`"
        ));
    }
    let counter = opts
        .get("counter")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing counter"))?;
    if !(0..=0xff_ffff).contains(&counter) {
        return Err(anyhow!("counter out of range (0..=16777215): {counter}"));
    }
    let mut bytes = [0u8; 12];
    bytes[..4].copy_from_slice(&secs.to_be_bytes());
    for i in 0..5 {
        bytes[4 + i] = u8::from_str_radix(&random[i * 2..i * 2 + 2], 16)
            .map_err(|_| anyhow!("random must be hex: `{random}`"))?;
    }
    let c = counter as u32;
    bytes[9] = ((c >> 16) & 0xff) as u8;
    bytes[10] = ((c >> 8) & 0xff) as u8;
    bytes[11] = (c & 0xff) as u8;
    let oid = bson::oid::ObjectId::from_bytes(bytes);
    Ok(json!({
        "oid": oid.to_hex(),
        "epoch_seconds": epoch,
        "random": random,
        "counter": counter,
    }))
}

/// Resolve a timestamp from `epoch_seconds`, `epoch_millis`, or an RFC-3339
/// `iso` string (in that precedence) down to whole epoch seconds. Shared by the
/// boundary-ObjectId builders.
fn resolve_epoch_seconds(opts: &Value) -> Result<i64> {
    if let Some(s) = opts.get("epoch_seconds").and_then(Value::as_i64) {
        Ok(s)
    } else if let Some(ms) = opts.get("epoch_millis").and_then(Value::as_i64) {
        Ok(ms.div_euclid(1000))
    } else if let Some(iso) = opts.get("iso").and_then(Value::as_str) {
        Ok(bson::DateTime::parse_rfc3339_str(iso)
            .map_err(|e| anyhow!("invalid iso timestamp `{iso}`: {e}"))?
            .timestamp_millis()
            .div_euclid(1000))
    } else {
        Err(anyhow!("missing epoch_seconds, epoch_millis, or iso"))
    }
}

/// Build a boundary ObjectId: the first 4 bytes carry the second-precision
/// timestamp (big-endian) and the trailing 8 bytes are all `fill`. `0x00` gives
/// the smallest ObjectId for that second, `0xFF` the largest. Shared by
/// `objectid_from_time` and `objectid_max_from_time`.
fn boundary_oid(seconds: i64, fill: u8) -> Result<bson::oid::ObjectId> {
    let secs = u32::try_from(seconds)
        .map_err(|_| anyhow!("timestamp out of ObjectId range (0..=4294967295s): {seconds}"))?;
    let mut bytes = [fill; 12];
    bytes[..4].copy_from_slice(&secs.to_be_bytes());
    Ok(bson::oid::ObjectId::from_bytes(bytes))
}

/// Build a boundary ObjectId from a timestamp — the official driver's
/// `ObjectId.createFromTime`: the first 4 bytes carry the second-precision
/// timestamp (big-endian) and the remaining 8 bytes are zero, giving the
/// smallest ObjectId for that second. Used for `_id` range queries by creation
/// time (`{_id: {$gte: createFromTime(t)}}`). opts: one of `epoch_seconds`,
/// `epoch_millis`, or `iso` (RFC 3339). Returns `{oid, epoch_seconds}`. Inverse
/// (at second precision) of `objectid_timestamp`. Pure.
fn op_objectid_from_time(opts: Value) -> Result<Value> {
    let seconds = resolve_epoch_seconds(&opts)?;
    let oid = boundary_oid(seconds, 0x00)?;
    Ok(json!({ "oid": oid.to_hex(), "epoch_seconds": seconds }))
}

/// Build the LARGEST ObjectId for a timestamp — the max-boundary companion to
/// `objectid_from_time`'s min boundary. The first 4 bytes carry the
/// second-precision timestamp; the remaining 8 are `0xFF`, so it sorts after
/// every real ObjectId generated during that second. Pairs with
/// `objectid_from_time` for an inclusive `_id` time-range query:
/// `{_id: {$gte: from_time(t1), $lte: max_from_time(t2)}}`. opts: one of
/// `epoch_seconds`, `epoch_millis`, or `iso` (RFC 3339). Returns `{oid,
/// epoch_seconds}`. Pure.
fn op_objectid_max_from_time(opts: Value) -> Result<Value> {
    let seconds = resolve_epoch_seconds(&opts)?;
    let oid = boundary_oid(seconds, 0xFF)?;
    Ok(json!({ "oid": oid.to_hex(), "epoch_seconds": seconds }))
}

/// Build the inclusive `_id` boundary pair for a creation-time window — the
/// idiomatic way to query documents created between two times without a separate
/// timestamp field. `min` is the smallest ObjectId in the `start` second (its
/// `objectid_from_time`) and `max` is the largest in the `end` second (its
/// `objectid_max_from_time`), so `{_id: {$gte: min, $lte: max}}` selects exactly
/// the documents created in `[start, end]`. opts: `start` and `end`, each an
/// `{epoch_seconds | epoch_millis | iso}` object; `end` must not precede `start`.
/// Returns `{start_epoch_seconds, end_epoch_seconds, min, max}` (hex). Pure.
fn op_objectid_range(opts: Value) -> Result<Value> {
    let start_opts = opts
        .get("start")
        .ok_or_else(|| anyhow!("missing start (an {{epoch_seconds|epoch_millis|iso}} object)"))?;
    let end_opts = opts
        .get("end")
        .ok_or_else(|| anyhow!("missing end (an {{epoch_seconds|epoch_millis|iso}} object)"))?;
    let start = resolve_epoch_seconds(start_opts)?;
    let end = resolve_epoch_seconds(end_opts)?;
    if end < start {
        return Err(anyhow!("end ({end}) is before start ({start})"));
    }
    let min = boundary_oid(start, 0x00)?;
    let max = boundary_oid(end, 0xFF)?;
    Ok(json!({
        "start_epoch_seconds": start,
        "end_epoch_seconds": end,
        "min": min.to_hex(),
        "max": max.to_hex(),
    }))
}

/// Combine several filter documents into one. opts: `filters` (array of
/// objects). When no two filters share a key the result is their shallow merge
/// (later filters override earlier on a literal key clash within that fast
/// path); when ANY key appears in more than one filter — the typical case for
/// composing reusable predicates — the result is `{"$and": [f1, f2, …]}` so no
/// clause is silently dropped. An empty `filters` array yields `{}` (match
/// everything); a single filter is returned unchanged. Pure.
fn op_merge_filters(opts: Value) -> Result<Value> {
    let filters = opts
        .get("filters")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing filters (array of objects)"))?;
    let mut docs: Vec<&serde_json::Map<String, Value>> = Vec::with_capacity(filters.len());
    for f in filters {
        let m = f
            .as_object()
            .ok_or_else(|| anyhow!("each filter must be an object"))?;
        docs.push(m);
    }
    match docs.len() {
        0 => return Ok(json!({ "filter": {} })),
        1 => return Ok(json!({ "filter": filters[0] })),
        _ => {}
    }
    // A key shared across two filters means a plain merge would lose a clause —
    // fall back to $and. Otherwise the shallow merge is the more readable form.
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut collision = false;
    for m in &docs {
        for k in m.keys() {
            if !seen.insert(k.as_str()) {
                collision = true;
                break;
            }
        }
        if collision {
            break;
        }
    }
    if collision {
        Ok(json!({ "filter": { "$and": filters } }))
    } else {
        let mut merged = serde_json::Map::new();
        for m in &docs {
            for (k, v) in m.iter() {
                merged.insert(k.clone(), v.clone());
            }
        }
        Ok(json!({ "filter": Value::Object(merged) }))
    }
}

/// Assemble a MongoDB update document from the common operator buckets. opts:
/// `set` (object → `$set`), `unset` (array of field names or object → `$unset`,
/// each value normalized to `""`), and `inc` (object → `$inc`). At least one
/// must be present and non-empty. The output is `{ "$set": …, "$unset": …,
/// "$inc": … }` carrying only the buckets supplied — pass it straight to
/// `update_one` / `update_many`. Returns `{update}`. Pure.
fn op_build_update(opts: Value) -> Result<Value> {
    let mut update = serde_json::Map::new();
    if let Some(set) = opts.get("set").and_then(Value::as_object) {
        if !set.is_empty() {
            update.insert("$set".into(), Value::Object(set.clone()));
        }
    }
    if let Some(inc) = opts.get("inc").and_then(Value::as_object) {
        if !inc.is_empty() {
            update.insert("$inc".into(), Value::Object(inc.clone()));
        }
    }
    // $unset accepts either an array of field names or an object; mongo ignores
    // the values, so normalize every field to "" for a canonical shape.
    let unset = match opts.get("unset") {
        Some(Value::Array(a)) => {
            let mut m = serde_json::Map::new();
            for f in a {
                let name = f
                    .as_str()
                    .ok_or_else(|| anyhow!("unset array entries must be field-name strings"))?;
                m.insert(name.to_string(), json!(""));
            }
            m
        }
        Some(Value::Object(o)) => o.keys().map(|k| (k.clone(), json!(""))).collect(),
        Some(Value::Null) | None => serde_json::Map::new(),
        Some(other) => {
            return Err(anyhow!(
                "unset must be an array of field names or an object, got {other}"
            ))
        }
    };
    if !unset.is_empty() {
        update.insert("$unset".into(), Value::Object(unset));
    }
    if update.is_empty() {
        return Err(anyhow!(
            "build_update needs at least one non-empty of set, unset, inc"
        ));
    }
    Ok(json!({ "update": Value::Object(update) }))
}

/// Normalize a sort specification into an ordered MongoDB sort document. opts:
/// `fields` (array). Each entry is either a `[field, direction]` two-element
/// array (direction `1`/`-1`, or `"asc"`/`"desc"`) or a bare string field with
/// an optional leading `-` for descending (`"-age"` → `{age: -1}`). Insertion
/// order is preserved (sort is order-sensitive). Returns `{sort}`. Pure.
fn op_build_sort(opts: Value) -> Result<Value> {
    let fields = opts
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing fields (array)"))?;
    let mut sort = serde_json::Map::new();
    for f in fields {
        let (name, dir): (String, i32) = match f {
            Value::String(s) => {
                if let Some(stripped) = s.strip_prefix('-') {
                    (stripped.to_string(), -1)
                } else {
                    (s.clone(), 1)
                }
            }
            Value::Array(pair) if pair.len() == 2 => {
                let name = pair[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("sort field name must be a string"))?
                    .to_string();
                let dir = match &pair[1] {
                    Value::Number(n) if n.as_i64() == Some(1) => 1,
                    Value::Number(n) if n.as_i64() == Some(-1) => -1,
                    Value::String(s) if s == "asc" => 1,
                    Value::String(s) if s == "desc" => -1,
                    other => {
                        return Err(anyhow!("sort direction must be 1|-1|asc|desc, got {other}"))
                    }
                };
                (name, dir)
            }
            other => {
                return Err(anyhow!(
                    "each field must be \"name\", \"-name\", or [name, dir], got {other}"
                ))
            }
        };
        if name.is_empty() {
            return Err(anyhow!("sort field name must not be empty"));
        }
        sort.insert(name, json!(dir));
    }
    if sort.is_empty() {
        return Err(anyhow!("fields must be non-empty"));
    }
    Ok(json!({ "sort": Value::Object(sort) }))
}

/// Build a MongoDB projection document from field lists. opts: exactly one of
/// `include` (array of fields → `{f: 1}`) or `exclude` (array → `{f: 0}`); the
/// `_id` field may be combined with the other mode (mongo's one documented
/// exception). `id` (bool, default keep) controls `_id` explicitly. Mixing
/// include and exclude (other than `_id`) is rejected, matching the server.
/// Returns `{projection}`. Pure.
fn op_build_projection(opts: Value) -> Result<Value> {
    let include = opts.get("include").and_then(Value::as_array);
    let exclude = opts.get("exclude").and_then(Value::as_array);
    if include.is_some() && exclude.is_some() {
        return Err(anyhow!(
            "cannot mix include and exclude (except for _id via the `id` flag)"
        ));
    }
    let mut proj = serde_json::Map::new();
    let mode_value = if exclude.is_some() { 0 } else { 1 };
    if let Some(list) = include.or(exclude) {
        for f in list {
            let name = f
                .as_str()
                .ok_or_else(|| anyhow!("projection field names must be strings"))?;
            proj.insert(name.to_string(), json!(mode_value));
        }
    }
    // Explicit _id control: default is mongo's (kept in include mode, dropped in
    // exclude mode), an `id` flag overrides it.
    if let Some(keep) = opt_truthy(&opts, "id") {
        proj.insert("_id".into(), json!(if keep { 1 } else { 0 }));
    }
    if proj.is_empty() {
        return Err(anyhow!(
            "need a non-empty include or exclude (or an id flag)"
        ));
    }
    Ok(json!({ "projection": Value::Object(proj) }))
}

/// Normalize an index-key specification into the document `create_index` /
/// `create_indexes` expect. opts: `keys` (array). Each entry is either a
/// `[field, type]` two-element array (type `1`/`-1` for ascending/descending, or
/// a string like `"2dsphere"`, `"text"`, `"hashed"`) or a bare string field with
/// an optional leading `-` for descending (`"-created"` → `{created: -1}`).
/// Insertion order is preserved (compound-index key order is significant).
/// Returns `{keys}`. The inverse direction parsing matches `build_sort`. Pure.
fn op_normalize_index_keys(opts: Value) -> Result<Value> {
    let entries = opts
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing keys (array)"))?;
    let mut keys = serde_json::Map::new();
    for e in entries {
        let (name, val): (String, Value) = match e {
            Value::String(s) => {
                if let Some(stripped) = s.strip_prefix('-') {
                    (stripped.to_string(), json!(-1))
                } else {
                    (s.clone(), json!(1))
                }
            }
            Value::Array(pair) if pair.len() == 2 => {
                let name = pair[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("index field name must be a string"))?
                    .to_string();
                let val = match &pair[1] {
                    Value::Number(n) if n.as_i64() == Some(1) => json!(1),
                    Value::Number(n) if n.as_i64() == Some(-1) => json!(-1),
                    // String index types (2dsphere/text/hashed/…) pass through verbatim.
                    Value::String(s) if !s.is_empty() => json!(s),
                    other => {
                        return Err(anyhow!(
                            "index key type must be 1|-1 or a non-empty type string, got {other}"
                        ))
                    }
                };
                (name, val)
            }
            other => {
                return Err(anyhow!(
                    "each key must be \"field\", \"-field\", or [field, type], got {other}"
                ))
            }
        };
        if name.is_empty() {
            return Err(anyhow!("index field name must not be empty"));
        }
        keys.insert(name, val);
    }
    if keys.is_empty() {
        return Err(anyhow!("keys must be non-empty"));
    }
    Ok(json!({ "keys": Value::Object(keys) }))
}

/// Build an `$in` (or `$nin`) filter for a field. opts: `field` (required),
/// `values` (array, may be empty), and `negate` (bool → use `$nin`). Returns
/// `{filter}` shaped `{ field: { "$in": [...] } }`. A small intention-revealing
/// builder so callers don't hand-assemble the operator object. Pure.
fn op_in_filter(opts: Value) -> Result<Value> {
    let field = opts
        .get("field")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing field"))?;
    let values = opts
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing values (array)"))?;
    let op = if opt_truthy(&opts, "negate") == Some(true) {
        "$nin"
    } else {
        "$in"
    };
    Ok(json!({ "filter": { field: { op: values } } }))
}

/// Build a range filter for a field from optional bounds. opts: `field`
/// (required); `gte`/`gt` (lower bound, inclusive/exclusive) and `lte`/`lt`
/// (upper bound) — supply any subset, but a bound and its strict variant on the
/// same side are mutually exclusive. At least one bound is required. Returns
/// `{filter}` shaped `{ field: { "$gte": …, "$lt": … } }`. Pure.
fn op_between_filter(opts: Value) -> Result<Value> {
    let field = opts
        .get("field")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing field"))?;
    let mut range = serde_json::Map::new();
    let mut put = |gte: &str, gt: &str| -> Result<()> {
        let inc = opts.get(gte).filter(|v| !v.is_null());
        let exc = opts.get(gt).filter(|v| !v.is_null());
        if inc.is_some() && exc.is_some() {
            return Err(anyhow!("{gte} and {gt} are mutually exclusive"));
        }
        if let Some(v) = inc {
            range.insert(format!("${gte}"), v.clone());
        } else if let Some(v) = exc {
            range.insert(format!("${gt}"), v.clone());
        }
        Ok(())
    };
    put("gte", "gt")?;
    put("lte", "lt")?;
    if range.is_empty() {
        return Err(anyhow!("need at least one of gte, gt, lte, lt"));
    }
    Ok(json!({ "filter": { field: Value::Object(range) } }))
}

/// Build an anchored, optionally case-insensitive `$regex` filter that matches a
/// LITERAL substring/prefix/suffix — the user input is regex-escaped so no
/// metacharacter is interpreted. opts: `field` (required), `value` (required
/// literal text), `anchor` (`"prefix"` → `^v`, `"suffix"` → `v$`, `"exact"` →
/// `^v$`, or `"contains"` / absent → unanchored), and `ignore_case` (bool →
/// adds the `i` option). Returns `{filter}` shaped `{ field: { "$regex": …,
/// "$options": … } }`. Pure.
fn op_build_regex_filter(opts: Value) -> Result<Value> {
    let field = opts
        .get("field")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing field"))?;
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let escaped = op_escape_regex(json!({ "value": value }))?["escaped"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let pattern = match opts.get("anchor").and_then(Value::as_str) {
        Some("prefix") => format!("^{escaped}"),
        Some("suffix") => format!("{escaped}$"),
        Some("exact") => format!("^{escaped}$"),
        Some("contains") | None => escaped,
        Some(other) => {
            return Err(anyhow!(
                "anchor must be prefix|suffix|exact|contains, got `{other}`"
            ))
        }
    };
    let mut regex = serde_json::Map::new();
    regex.insert("$regex".into(), json!(pattern));
    if opt_truthy(&opts, "ignore_case") == Some(true) {
        regex.insert("$options".into(), json!("i"));
    }
    Ok(json!({ "filter": { field: Value::Object(regex) } }))
}

/// Combine `filters` (an array of filter objects) into a single `$or` filter —
/// matches a document when ANY clause matches. This is the disjunctive
/// counterpart to `merge_filters` (which produces `$and`/shallow-merge). An empty
/// array yields `{}` (match all); a single filter is returned unchanged (the
/// `$or` wrapper would be redundant). opts: `filters` (array). Returns `{filter}`
/// shaped `{ "$or": [ … ] }`. Pure.
fn op_or_filter(opts: Value) -> Result<Value> {
    let filters = opts
        .get("filters")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing filters (array of objects)"))?;
    for f in filters {
        if !f.is_object() {
            return Err(anyhow!("each filter must be an object"));
        }
    }
    match filters.len() {
        0 => Ok(json!({ "filter": {} })),
        1 => Ok(json!({ "filter": filters[0] })),
        _ => Ok(json!({ "filter": { "$or": filters } })),
    }
}

/// Build an `$exists` filter for a field. opts: `field` (required), `exists`
/// (bool, default `true`). `{ field: { "$exists": true } }` selects documents
/// that have the field (even when null); `false` selects documents missing it.
/// Returns `{filter}`. Pure.
fn op_exists_filter(opts: Value) -> Result<Value> {
    let field = opts
        .get("field")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing field"))?;
    let exists = opt_truthy(&opts, "exists").unwrap_or(true);
    Ok(json!({ "filter": { field: { "$exists": exists } } }))
}

/// Build an `$elemMatch` filter — matches documents where at least one element of
/// the array `field` satisfies the whole `query` object. Use this when several
/// conditions must hold on the SAME array element (a plain `{ "field.a": …,
/// "field.b": … }` would let different elements satisfy each condition). opts:
/// `field` (required), `query` (required object, the per-element predicate).
/// Returns `{filter}` shaped `{ field: { "$elemMatch": { … } } }`. Pure.
fn op_elem_match_filter(opts: Value) -> Result<Value> {
    let field = opts
        .get("field")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing field"))?;
    let query = opts
        .get("query")
        .filter(|v| v.is_object())
        .ok_or_else(|| anyhow!("missing query (object)"))?;
    Ok(json!({ "filter": { field: { "$elemMatch": query } } }))
}

/// Build a `$text` full-text search filter — requires a text index on the
/// collection. opts: `search` (required search string), `language` (override the
/// index's default stemming language), `case_sensitive` (bool), and
/// `diacritic_sensitive` (bool). Returns `{filter}` shaped `{ "$text": {
/// "$search": …, … } }`. Pure.
fn op_text_filter(opts: Value) -> Result<Value> {
    let search = opts
        .get("search")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing search (string)"))?;
    let mut text = serde_json::Map::new();
    text.insert("$search".into(), json!(search));
    if let Some(lang) = opts.get("language").and_then(Value::as_str) {
        text.insert("$language".into(), json!(lang));
    }
    if let Some(cs) = opt_truthy(&opts, "case_sensitive") {
        text.insert("$caseSensitive".into(), json!(cs));
    }
    if let Some(ds) = opt_truthy(&opts, "diacritic_sensitive") {
        text.insert("$diacriticSensitive".into(), json!(ds));
    }
    Ok(json!({ "filter": { "$text": Value::Object(text) } }))
}

/// Negate a single-field operator expression with `$not`. `expr` is an operator
/// object such as `{ "$gt": 5 }` or `{ "$regex": "^a" }` — `$not` wraps it so the
/// field matches documents where the inner expression does NOT hold (including
/// documents missing the field). opts: `field` (required), `expr` (required
/// operator object). A plain value (`5`) or a logical-operator object (`$or`)
/// cannot be negated this way and is rejected. Returns `{filter}` shaped
/// `{ field: { "$not": { … } } }`. Pure.
fn op_not_filter(opts: Value) -> Result<Value> {
    let field = opts
        .get("field")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing field"))?;
    let expr = opts
        .get("expr")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("missing expr (an operator object like {{\"$gt\": 5}})"))?;
    if expr.is_empty() {
        return Err(anyhow!("expr must be a non-empty operator object"));
    }
    // $not only wraps operator expressions; every key must be an operator ($…).
    // A bare value or a logical operator ($or/$and/$nor) is not a valid $not arg.
    for k in expr.keys() {
        if !k.starts_with('$') {
            return Err(anyhow!(
                "expr key `{k}` is not an operator — $not wraps operator expressions like {{\"$gt\": 5}}"
            ));
        }
        if matches!(k.as_str(), "$or" | "$and" | "$nor") {
            return Err(anyhow!("$not cannot wrap the logical operator `{k}`"));
        }
    }
    Ok(json!({ "filter": { field: { "$not": expr } } }))
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
pub extern "C" fn mongo__create_indexes(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_create_indexes)
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
pub extern "C" fn mongo__collection_specs(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_collection_specs)
}

#[no_mangle]
pub extern "C" fn mongo__validate(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_validate)
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
pub extern "C" fn mongo__drop_database(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_drop_database)
}

#[no_mangle]
pub extern "C" fn mongo__drop_indexes(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_drop_indexes)
}

#[no_mangle]
pub extern "C" fn mongo__aggregate_db(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_aggregate_db)
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

#[no_mangle]
pub extern "C" fn mongo__parse_connection_string(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_connection_string(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__build_connection_string(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_connection_string(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__valid_connection_string(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_connection_string(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__redact_connection_string(args: *const c_char) -> *const c_char {
    ffi_call_async(
        args,
        |opts| async move { op_redact_connection_string(opts) },
    )
}

#[no_mangle]
pub extern "C" fn mongo__parse_namespace(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_namespace(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__build_namespace(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_namespace(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__valid_collection_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_collection_name(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__valid_database_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_database_name(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__valid_namespace(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_namespace(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__valid_field_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_field_name(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__escape_regex(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_escape_regex(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__unescape_regex(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_unescape_regex(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__is_valid_objectid(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_is_valid_objectid(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__new_objectid(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_new_objectid(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__objectid_timestamp(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_objectid_timestamp(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__parse_objectid(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_objectid(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__objectid_compare(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_objectid_compare(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__build_objectid(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_objectid(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__objectid_from_time(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_objectid_from_time(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__objectid_max_from_time(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_objectid_max_from_time(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__objectid_range(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_objectid_range(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__merge_filters(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_merge_filters(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__build_update(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_update(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__build_sort(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_sort(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__build_projection(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_projection(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__normalize_index_keys(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_normalize_index_keys(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__in_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_in_filter(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__between_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_between_filter(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__build_regex_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_regex_filter(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__or_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_or_filter(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__exists_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_exists_filter(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__elem_match_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_elem_match_filter(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__text_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_text_filter(opts) })
}

#[no_mangle]
pub extern "C" fn mongo__not_filter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_not_filter(opts) })
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

    // ── pure helpers (no connection) ─────────────────────────────────────────

    #[test]
    fn parse_connection_string_single_host_with_auth_and_opts() {
        let v = op_parse_connection_string(json!({
            "uri": "mongodb://app:s3cret@db.example.com:27018/shop?authSource=admin&retryWrites=true"
        }))
        .unwrap();
        assert_eq!(v["scheme"], json!("mongodb"));
        assert_eq!(v["srv"], json!(false));
        assert_eq!(v["user"], json!("app"));
        assert_eq!(v["password"], json!("s3cret"));
        let hosts = v["hosts"].as_array().unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0]["host"], json!("db.example.com"));
        assert_eq!(hosts[0]["port"], json!(27018));
        assert_eq!(v["database"], json!("shop"));
        assert_eq!(v["params"]["authSource"], json!("admin"));
    }

    #[test]
    fn parse_connection_string_multi_host_replica_set() {
        // The distinguishing MongoDB feature: a comma-separated host list, ports
        // optional per host.
        let v = op_parse_connection_string(json!({
            "uri": "mongodb://h1:27017,h2:27017,h3/rs?replicaSet=rs0"
        }))
        .unwrap();
        let hosts = v["hosts"].as_array().unwrap();
        assert_eq!(hosts.len(), 3, "three replica-set members");
        assert_eq!(hosts[0]["port"], json!(27017));
        assert_eq!(hosts[2]["host"], json!("h3"));
        assert_eq!(hosts[2]["port"], Value::Null, "portless host → null port");
        assert_eq!(v["params"]["replicaSet"], json!("rs0"));
    }

    #[test]
    fn parse_connection_string_srv_and_percent_decoded_password() {
        let v = op_parse_connection_string(json!({
            "uri": "mongodb+srv://u:p%40ss@cluster0.mongodb.net/db"
        }))
        .unwrap();
        assert_eq!(v["scheme"], json!("mongodb+srv"));
        assert_eq!(v["srv"], json!(true));
        assert_eq!(v["password"], json!("p@ss"), "userinfo percent-decoded");
    }

    #[test]
    fn parse_connection_string_rejects_bad_scheme_and_non_uri() {
        assert!(op_parse_connection_string(json!({"uri": "postgres://localhost/x"})).is_err());
        assert!(op_parse_connection_string(json!({"uri": "h1:27017"})).is_err());
        assert!(op_parse_connection_string(json!({})).is_err());
    }

    #[test]
    fn valid_connection_string_enforces_host_rules() {
        let v = |uri: &str| op_valid_connection_string(json!({ "uri": uri })).unwrap();
        // Standard URIs with one or many hosts are valid.
        assert_eq!(v("mongodb://h1:27017/db")["valid"], json!(true));
        assert_eq!(
            v("mongodb://u:p@h1:27017,h2:27018/db")["valid"],
            json!(true)
        );
        // SRV with exactly one host and no port is valid.
        assert_eq!(
            v("mongodb+srv://user:pw@server.example.com/db")["valid"],
            json!(true)
        );
        // A bad scheme / missing `://` fails the underlying parse.
        assert_eq!(v("postgres://localhost/x")["valid"], json!(false));
        assert_eq!(v("h1:27017")["valid"], json!(false));
        // SRV with a port is rejected.
        let port = v("mongodb+srv://server.example.com:27017/db");
        assert_eq!(port["valid"], json!(false));
        assert!(port["reason"].as_str().unwrap().contains("port"));
        // SRV with two hosts is rejected.
        let two = v("mongodb+srv://a.example.com,b.example.com/db");
        assert_eq!(two["valid"], json!(false));
        assert!(two["reason"].as_str().unwrap().contains("exactly one host"));
        // A standard URI with no host is rejected.
        let none = v("mongodb:///db");
        assert_eq!(none["valid"], json!(false));
        assert!(none["reason"]
            .as_str()
            .unwrap()
            .contains("at least one host"));
        // Missing uri is an error (not a verdict).
        assert!(op_valid_connection_string(json!({})).is_err());
    }

    #[test]
    fn build_connection_string_inverts_parse_connection_string() {
        // Bare single host.
        assert_eq!(
            op_build_connection_string(json!({"hosts": [{"host": "localhost", "port": 27017}]}))
                .unwrap()["uri"],
            json!("mongodb://localhost:27017")
        );
        // Multi-host replica set with auth, db, and options.
        let uri = op_build_connection_string(json!({
            "user": "admin",
            "password": "p@ss",
            "hosts": [{"host": "h1", "port": 27017}, {"host": "h2", "port": 27018}],
            "database": "shop",
            "params": {"replicaSet": "rs0", "authSource": "admin"}
        }))
        .unwrap()["uri"]
            .as_str()
            .unwrap()
            .to_string();
        // Password percent-encoded; params sorted (authSource before replicaSet).
        assert_eq!(
            uri,
            "mongodb://admin:p%40ss@h1:27017,h2:27018/shop?authSource=admin&replicaSet=rs0"
        );
        // Round-trips through parse_connection_string.
        let back = op_parse_connection_string(json!({ "uri": uri })).unwrap();
        assert_eq!(back["user"], json!("admin"));
        assert_eq!(back["password"], json!("p@ss"), "percent-decoded back");
        assert_eq!(back["database"], json!("shop"));
        assert_eq!(back["hosts"].as_array().unwrap().len(), 2);
        assert_eq!(back["params"]["replicaSet"], json!("rs0"));
        // SRV scheme; options with no database still get the `/` separator.
        assert_eq!(
            op_build_connection_string(json!({
                "srv": true,
                "hosts": ["cluster0.mongodb.net"],
                "params": {"retryWrites": "true"}
            }))
            .unwrap()["uri"],
            json!("mongodb+srv://cluster0.mongodb.net/?retryWrites=true")
        );
        // Missing/empty hosts reject.
        assert!(op_build_connection_string(json!({})).is_err());
        assert!(op_build_connection_string(json!({"hosts": []})).is_err());
    }

    #[test]
    fn redact_connection_string_masks_only_the_password() {
        let red = |u: &str| op_redact_connection_string(json!({ "uri": u })).unwrap();
        // Password masked; everything else byte-for-byte identical.
        let v = red("mongodb://app:s3cret@h1:27017/shop");
        assert_eq!(v["redacted"], json!("mongodb://app:***@h1:27017/shop"));
        assert_eq!(v["had_password"], json!(true));
        // srv + query options preserved exactly (no param reordering/re-encoding).
        assert_eq!(
            red("mongodb+srv://u:p@cluster0.mongodb.net/db?retryWrites=true&w=majority")
                ["redacted"],
            json!("mongodb+srv://u:***@cluster0.mongodb.net/db?retryWrites=true&w=majority")
        );
        // Multiple hosts.
        assert_eq!(
            red("mongodb://u:p@h1:27017,h2:27018/db")["redacted"],
            json!("mongodb://u:***@h1:27017,h2:27018/db")
        );
        // No password, or no userinfo at all → returned unchanged.
        let np = red("mongodb://app@h1/db");
        assert_eq!(np["redacted"], json!("mongodb://app@h1/db"));
        assert_eq!(np["had_password"], json!(false));
        assert_eq!(
            red("mongodb://h1:27017/shop")["redacted"],
            json!("mongodb://h1:27017/shop")
        );
        // A custom mask is honored.
        assert_eq!(
            op_redact_connection_string(json!({"uri": "mongodb://a:b@h/db", "mask": "REDACTED"}))
                .unwrap()["redacted"],
            json!("mongodb://a:REDACTED@h/db")
        );
        // Not a URI / missing uri error.
        assert!(op_redact_connection_string(json!({"uri": "no-scheme"})).is_err());
        assert!(op_redact_connection_string(json!({})).is_err());
    }

    #[test]
    fn parse_namespace_splits_on_first_dot_only() {
        let v = op_parse_namespace(json!({"namespace": "shop.orders.2025"})).unwrap();
        assert_eq!(v["db"], json!("shop"));
        assert_eq!(
            v["collection"],
            json!("orders.2025"),
            "collection keeps later dots"
        );
        assert!(op_parse_namespace(json!({"namespace": "no_dot"})).is_err());
    }

    #[test]
    fn build_namespace_inverts_parse_namespace() {
        // db + collection (with later dots) → namespace, round-trips through parse.
        let ns = op_build_namespace(json!({"db": "shop", "collection": "orders.2025"})).unwrap()
            ["namespace"]
            .clone();
        assert_eq!(ns, json!("shop.orders.2025"));
        let back = op_parse_namespace(json!({"namespace": ns})).unwrap();
        assert_eq!(back["db"], json!("shop"));
        assert_eq!(back["collection"], json!("orders.2025"));
        // A db with a dot is rejected (would break the first-dot split).
        assert!(op_build_namespace(json!({"db": "a.b", "collection": "c"})).is_err());
        // Missing/empty parts error.
        assert!(op_build_namespace(json!({"db": "shop"})).is_err());
        assert!(op_build_namespace(json!({"db": "", "collection": "c"})).is_err());
    }

    #[test]
    fn valid_collection_name_enforces_mongodb_hard_rules() {
        let ok = |name: &str| {
            op_valid_collection_name(json!({ "name": name })).unwrap()["valid"]
                .as_bool()
                .unwrap()
        };
        // Ordinary names — including a leading digit (server allows it) and dots.
        assert!(ok("orders"));
        assert!(ok("orders.2025"), "dots are allowed in collection names");
        assert!(ok("123abc"), "leading digit is not a hard rule");
        assert!(ok("_private"));
        // Hard rejections.
        for (name, want) in [
            ("", "empty"),
            ("with$dollar", "'$'"),
            ("system.users", "system."),
        ] {
            let v = op_valid_collection_name(json!({ "name": name })).unwrap();
            assert_eq!(v["valid"], json!(false), "{name} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{name}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        // Null character is rejected.
        assert_eq!(
            op_valid_collection_name(json!({"name": "a\0b"})).unwrap()["valid"],
            json!(false)
        );
        // Namespace length is checked only when a db is supplied. "mydb" + "." +
        // 260 chars = 265 bytes, over the 255-byte limit.
        let long = "c".repeat(260);
        assert!(
            !op_valid_collection_name(json!({"name": long, "db": "mydb"})).unwrap()["valid"]
                .as_bool()
                .unwrap()
        );
        assert!(
            ok(&long),
            "the same long name is fine without a db (collection-name-only check)"
        );
    }

    #[test]
    fn valid_database_name_enforces_mongodb_rules() {
        let ok = |name: &str| {
            op_valid_database_name(json!({ "name": name })).unwrap()["valid"]
                .as_bool()
                .unwrap()
        };
        assert!(ok("myapp"));
        assert!(ok("reporting_2025"));
        // Unlike collections, a `.` is forbidden (and so is a space).
        for (name, want) in [
            ("", "empty"),
            ("has.dot", "/ \\ ."),
            ("has space", "space"),
            ("with$dollar", "$"),
            ("a/b", "/ \\ ."),
            ("pipe|name", "|"),
        ] {
            let v = op_valid_database_name(json!({ "name": name })).unwrap();
            assert_eq!(v["valid"], json!(false), "{name} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{name}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        // Null character rejected; 63 chars ok, 64 fails (fewer than 64).
        assert!(!ok("a\0b"));
        assert!(ok(&"a".repeat(63)));
        let long = op_valid_database_name(json!({"name": "a".repeat(64)})).unwrap();
        assert_eq!(long["valid"], json!(false));
        assert!(long["reason"].as_str().unwrap().contains("64"));
        assert!(op_valid_database_name(json!({})).is_err());
    }

    #[test]
    fn valid_field_name_enforces_bson_key_rules() {
        let ok = |name: &str| {
            op_valid_field_name(json!({ "name": name })).unwrap()["valid"]
                .as_bool()
                .unwrap()
        };
        assert!(ok("name"));
        assert!(ok("user_id"));
        assert!(ok("a-b"));
        for (name, want) in [("", "empty"), ("$set", "$"), ("a.b", "."), ("a\0b", "null")] {
            let v = op_valid_field_name(json!({ "name": name })).unwrap();
            assert_eq!(v["valid"], json!(false), "{name} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{name}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        assert!(op_valid_field_name(json!({})).is_err());
    }

    #[test]
    fn valid_namespace_splits_and_validates_both_parts() {
        // A legal namespace, with the parts surfaced.
        let v = op_valid_namespace(json!({"namespace": "myapp.users"})).unwrap();
        assert_eq!(v["valid"], json!(true));
        assert_eq!(v["database"], json!("myapp"));
        assert_eq!(v["collection"], json!("users"));
        // The collection part may itself contain dots; the split is on the FIRST dot.
        let dotted = op_valid_namespace(json!({"namespace": "myapp.sub.coll"})).unwrap();
        assert_eq!(dotted["valid"], json!(true));
        assert_eq!(dotted["database"], json!("myapp"));
        assert_eq!(dotted["collection"], json!("sub.coll"));
        // No dot at all → not a namespace.
        let nodot = op_valid_namespace(json!({"namespace": "justdb"})).unwrap();
        assert_eq!(nodot["valid"], json!(false));
        assert!(nodot["reason"]
            .as_str()
            .unwrap()
            .contains("database.collection"));
        // A bad database part is reported as such.
        let baddb = op_valid_namespace(json!({"namespace": "has space.users"})).unwrap();
        assert_eq!(baddb["valid"], json!(false));
        assert!(baddb["reason"].as_str().unwrap().contains("database name"));
        // A bad collection part ($) is reported as such.
        let badcoll = op_valid_namespace(json!({"namespace": "myapp.us$ers"})).unwrap();
        assert_eq!(badcoll["valid"], json!(false));
        assert!(badcoll["reason"]
            .as_str()
            .unwrap()
            .contains("collection name"));
        // The combined 255-byte namespace limit (enforced via valid_collection_name).
        let toolong = format!("db.{}", "c".repeat(255));
        assert_eq!(
            op_valid_namespace(json!({ "namespace": toolong })).unwrap()["valid"],
            json!(false)
        );
        assert!(op_valid_namespace(json!({})).is_err());
    }

    #[test]
    fn escape_regex_escapes_pcre_metacharacters() {
        let e = |s: &str| {
            op_escape_regex(json!({ "value": s })).unwrap()["escaped"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Plain text is unchanged.
        assert_eq!(e("hello"), "hello");
        // Every metacharacter is backslash-escaped.
        assert_eq!(e("a.b"), "a\\.b");
        assert_eq!(e("1+1=2"), "1\\+1=2");
        assert_eq!(e("(x)[y]{z}"), "\\(x\\)\\[y\\]\\{z\\}");
        assert_eq!(e("^start$"), "\\^start\\$");
        assert_eq!(e("a|b*c?"), "a\\|b\\*c\\?");
        // A literal backslash is doubled.
        assert_eq!(e("a\\b"), "a\\\\b");
        // Realistic: a price string used as a literal $regex.
        assert_eq!(e("$9.99 (USD)"), "\\$9\\.99 \\(USD\\)");
        assert_eq!(e(""), "");
        assert!(op_escape_regex(json!({})).is_err());
    }

    #[test]
    fn unescape_regex_inverts_escape_regex() {
        let u = |s: &str| {
            op_unescape_regex(json!({ "escaped": s })).unwrap()["value"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Each escaped metacharacter collapses back.
        assert_eq!(u("hello"), "hello");
        assert_eq!(u("a\\.b"), "a.b");
        assert_eq!(u("\\(x\\)\\[y\\]\\{z\\}"), "(x)[y]{z}");
        assert_eq!(
            u("a\\\\b"),
            "a\\b",
            "a doubled backslash is one literal backslash"
        );
        // Round-trips escape_regex for every metacharacter and a realistic string.
        for s in [
            "hello",
            "a.b",
            "1+1=2",
            "(x)[y]{z}",
            "^start$",
            "a|b*c?",
            "a\\b",
            "$9.99 (USD)",
        ] {
            let esc = op_escape_regex(json!({ "value": s })).unwrap()["escaped"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(u(&esc), s, "round-trips `{s}`");
        }
        // A real regex (unescaped metacharacter), an invalid escape, and a
        // dangling backslash are all rejected.
        assert!(op_unescape_regex(json!({"escaped": "a.b"})).is_err());
        assert!(op_unescape_regex(json!({"escaped": "\\d+"})).is_err());
        assert!(op_unescape_regex(json!({"escaped": "abc\\"})).is_err());
        assert!(op_unescape_regex(json!({})).is_err());
    }

    #[test]
    fn is_valid_objectid_matches_bson() {
        let real = bson::oid::ObjectId::new().to_hex();
        assert_eq!(
            op_is_valid_objectid(json!({"id": real})).unwrap()["valid"],
            json!(true)
        );
        assert_eq!(
            op_is_valid_objectid(json!({"id": "not-an-oid"})).unwrap()["valid"],
            json!(false)
        );
        // 23 chars (one short) must be rejected.
        assert_eq!(
            op_is_valid_objectid(json!({"id": "5f43a1b2c3d4e5f6a7b8c9d0"[..23].to_string()}))
                .unwrap()["valid"],
            json!(false)
        );
    }

    #[test]
    fn new_objectid_is_a_valid_24_hex_string() {
        let oid = op_new_objectid(json!({})).unwrap();
        let s = oid["oid"].as_str().unwrap();
        assert_eq!(s.len(), 24, "ObjectId hex is 24 chars");
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
        // And it round-trips through the validator.
        assert_eq!(
            op_is_valid_objectid(json!({"id": s})).unwrap()["valid"],
            json!(true)
        );
    }

    #[test]
    fn objectid_timestamp_decodes_leading_four_bytes() {
        // First 4 bytes `4d88e15b` = 0x4d88e15b = 1300816219 seconds since epoch.
        let v = op_objectid_timestamp(json!({"id": "4d88e15b60f486e428412dc9"})).unwrap();
        assert_eq!(v["epoch_seconds"], json!(1_300_816_219i64));
        assert_eq!(v["epoch_millis"], json!(1_300_816_219_000i64));
        assert_eq!(
            v["iso"], "2011-03-22T17:50:19Z",
            "RFC3339 string of the embedded timestamp"
        );
        // The all-zero ObjectId decodes to the Unix epoch.
        let zero = op_objectid_timestamp(json!({"id": "000000000000000000000000"})).unwrap();
        assert_eq!(zero["epoch_seconds"], json!(0));
        // A freshly generated id's timestamp is well after 2020 (1.5e9 s).
        let fresh = op_new_objectid(json!({})).unwrap()["oid"]
            .as_str()
            .unwrap()
            .to_string();
        let ts = op_objectid_timestamp(json!({"id": fresh})).unwrap();
        assert!(
            ts["epoch_seconds"].as_i64().unwrap() > 1_500_000_000,
            "new ObjectId carries a recent timestamp"
        );
        assert!(op_objectid_timestamp(json!({"id": "nothex"})).is_err());
    }

    #[test]
    fn parse_objectid_decomposes_all_three_fields() {
        // 4d88e15b | 60f486e428 | 412dc9 → timestamp, 5-byte random, 3-byte counter.
        let v = op_parse_objectid(json!({"id": "4d88e15b60f486e428412dc9"})).unwrap();
        assert_eq!(v["hex"], json!("4d88e15b60f486e428412dc9"));
        assert_eq!(v["epoch_seconds"], json!(1_300_816_219u32));
        assert_eq!(v["iso"], "2011-03-22T17:50:19Z");
        assert_eq!(v["random"], json!("60f486e428"), "bytes 4-8 as hex");
        assert_eq!(v["counter"], json!(0x412dc9u32), "bytes 9-11 big-endian");
        // The timestamp agrees with objectid_timestamp on the same id.
        let ts = op_objectid_timestamp(json!({"id": "4d88e15b60f486e428412dc9"})).unwrap();
        assert_eq!(v["epoch_seconds"].as_i64(), ts["epoch_seconds"].as_i64());
        // All-zero id → zero fields.
        let zero = op_parse_objectid(json!({"id": "000000000000000000000000"})).unwrap();
        assert_eq!(zero["counter"], json!(0));
        assert_eq!(zero["random"], json!("0000000000"));
        assert!(op_parse_objectid(json!({"id": "nothex"})).is_err());
        assert!(op_parse_objectid(json!({})).is_err());
    }

    #[test]
    fn objectid_compare_orders_by_the_twelve_bytes() {
        let c = |a: &str, b: &str| {
            op_objectid_compare(json!({"a": a, "b": b})).unwrap()["cmp"]
                .as_i64()
                .unwrap()
        };
        // Earlier timestamp sorts first (a's leading bytes are smaller).
        let early = "4d88e15b000000000000000a";
        let late = "5d88e15b000000000000000a";
        assert_eq!(c(early, late), -1);
        assert_eq!(c(late, early), 1);
        assert_eq!(c(early, early), 0);
        // Same timestamp, different counter → the counter breaks the tie (finer
        // than objectid_timestamp, which is second-resolution).
        let t = "4d88e15b6000000000000000";
        let same_sec_a = "4d88e15b0000000000000001";
        let same_sec_b = "4d88e15b0000000000000002";
        assert_eq!(
            op_objectid_timestamp(json!({"id": same_sec_a})).unwrap()["epoch_seconds"],
            op_objectid_timestamp(json!({"id": same_sec_b})).unwrap()["epoch_seconds"],
            "same second"
        );
        assert_eq!(
            c(same_sec_a, same_sec_b),
            -1,
            "counter breaks the same-second tie"
        );
        let _ = t;
        // `older`/`equal` fields and the input/null cases.
        let v = op_objectid_compare(json!({"a": late, "b": early})).unwrap();
        assert_eq!(v["older"], json!(early));
        assert_eq!(v["equal"], json!(false));
        let eq = op_objectid_compare(json!({"a": early, "b": early})).unwrap();
        assert_eq!(eq["older"], Value::Null);
        assert_eq!(eq["equal"], json!(true));
        // Invalid ids and missing args error.
        assert!(op_objectid_compare(json!({"a": "nothex", "b": early})).is_err());
        assert!(op_objectid_compare(json!({"a": early})).is_err());
        assert!(op_objectid_compare(json!({})).is_err());
    }

    #[test]
    fn build_objectid_inverts_parse_objectid() {
        // Reassemble the canonical test vector from its three parts.
        let v = op_build_objectid(json!({
            "epoch_seconds": 1_300_816_219i64,
            "random": "60f486e428",
            "counter": 4_271_561i64,
        }))
        .unwrap();
        assert_eq!(v["oid"], json!("4d88e15b60f486e428412dc9"));
        // Round-trips parse_objectid for several ids (including all-zero).
        for id in [
            "4d88e15b60f486e428412dc9",
            "000000000000000000000000",
            "ffffffffffffffffffffffff",
        ] {
            let p = op_parse_objectid(json!({ "id": id })).unwrap();
            let rebuilt = op_build_objectid(json!({
                "epoch_seconds": p["epoch_seconds"],
                "random": p["random"],
                "counter": p["counter"],
            }))
            .unwrap();
            assert_eq!(rebuilt["oid"], json!(id), "round-trip for {id}");
        }
        // Errors: random not 10 hex chars, counter out of range, epoch overflow.
        assert!(
            op_build_objectid(json!({"epoch_seconds": 1, "random": "abc", "counter": 0})).is_err()
        );
        assert!(op_build_objectid(
            json!({"epoch_seconds": 1, "random": "zzzzzzzzzz", "counter": 0})
        )
        .is_err());
        assert!(op_build_objectid(
            json!({"epoch_seconds": 1, "random": "0000000000", "counter": 16_777_216i64})
        )
        .is_err());
        assert!(op_build_objectid(
            json!({"epoch_seconds": 5_000_000_000i64, "random": "0000000000", "counter": 0})
        )
        .is_err());
        assert!(op_build_objectid(json!({})).is_err());
    }

    #[test]
    fn objectid_from_time_builds_boundary_id_and_inverts_timestamp() {
        // createFromTime(1300816219) — timestamp in first 4 bytes, rest zero.
        let v = op_objectid_from_time(json!({"epoch_seconds": 1_300_816_219i64})).unwrap();
        assert_eq!(v["oid"], json!("4d88e15b0000000000000000"));
        // Round-trips with objectid_timestamp at second precision.
        let back = op_objectid_timestamp(json!({"id": v["oid"].clone()})).unwrap();
        assert_eq!(back["epoch_seconds"], json!(1_300_816_219i64));
        // epoch_millis is floored to whole seconds.
        assert_eq!(
            op_objectid_from_time(json!({"epoch_millis": 1_300_816_219_999i64})).unwrap()["oid"],
            json!("4d88e15b0000000000000000")
        );
        // ISO input agrees with the epoch form.
        assert_eq!(
            op_objectid_from_time(json!({"iso": "2011-03-22T17:50:19Z"})).unwrap()["oid"],
            json!("4d88e15b0000000000000000")
        );
        // Epoch zero → all-zero id.
        assert_eq!(
            op_objectid_from_time(json!({"epoch_seconds": 0})).unwrap()["oid"],
            json!("000000000000000000000000")
        );
        // Out-of-range and missing inputs reject.
        assert!(op_objectid_from_time(json!({"epoch_seconds": 5_000_000_000i64})).is_err());
        assert!(op_objectid_from_time(json!({})).is_err());
    }

    #[test]
    fn objectid_max_from_time_builds_the_upper_boundary() {
        // Same timestamp prefix as from_time, but the trailing 8 bytes are 0xFF.
        let v = op_objectid_max_from_time(json!({"epoch_seconds": 1_300_816_219i64})).unwrap();
        assert_eq!(v["oid"], json!("4d88e15bffffffffffffffff"));
        // It shares the timestamp with the min boundary and sorts strictly after it.
        let min = op_objectid_from_time(json!({"epoch_seconds": 1_300_816_219i64})).unwrap()["oid"]
            .as_str()
            .unwrap()
            .to_string();
        let max = v["oid"].as_str().unwrap().to_string();
        assert!(max > min, "max boundary {max} must sort after min {min}");
        // The max boundary still decodes back to the same second.
        assert_eq!(
            op_objectid_timestamp(json!({ "id": max })).unwrap()["epoch_seconds"],
            json!(1_300_816_219i64)
        );
        // Same time-input forms as from_time, and epoch zero → ts-zero + all-FF tail.
        assert_eq!(
            op_objectid_max_from_time(json!({"iso": "2011-03-22T17:50:19Z"})).unwrap()["oid"],
            json!("4d88e15bffffffffffffffff")
        );
        assert_eq!(
            op_objectid_max_from_time(json!({"epoch_seconds": 0})).unwrap()["oid"],
            json!("00000000ffffffffffffffff")
        );
        // Out-of-range and missing inputs reject like from_time.
        assert!(op_objectid_max_from_time(json!({"epoch_seconds": 5_000_000_000i64})).is_err());
        assert!(op_objectid_max_from_time(json!({})).is_err());
    }

    #[test]
    fn objectid_range_builds_the_inclusive_id_window() {
        let v = op_objectid_range(json!({
            "start": {"epoch_seconds": 1_300_816_219i64},
            "end": {"epoch_seconds": 1_300_902_619i64},
        }))
        .unwrap();
        // min = from_time(start) (all-zero tail); max = max_from_time(end) (all-FF tail).
        assert_eq!(v["min"], json!("4d88e15b0000000000000000"));
        assert_eq!(
            v["max"],
            op_objectid_max_from_time(json!({"epoch_seconds": 1_300_902_619i64})).unwrap()["oid"]
        );
        assert_eq!(v["start_epoch_seconds"], json!(1_300_816_219i64));
        assert_eq!(v["end_epoch_seconds"], json!(1_300_902_619i64));
        assert!(v["max"].as_str().unwrap() > v["min"].as_str().unwrap());
        // iso forms; a single-second window (start == end) is valid (min<max via fill bytes).
        let same = op_objectid_range(json!({
            "start": {"iso": "2011-03-22T17:50:19Z"},
            "end": {"iso": "2011-03-22T17:50:19Z"},
        }))
        .unwrap();
        assert!(same["max"].as_str().unwrap() > same["min"].as_str().unwrap());
        // end before start, and a missing endpoint, error.
        assert!(op_objectid_range(
            json!({"start": {"epoch_seconds": 100}, "end": {"epoch_seconds": 50}})
        )
        .is_err());
        assert!(op_objectid_range(json!({"start": {"epoch_seconds": 100}})).is_err());
        assert!(op_objectid_range(json!({})).is_err());
    }

    // ── pure query-builder helpers ───────────────────────────────────────────

    /// `merge_filters` shallow-merges when keys are disjoint but falls back to
    /// `$and` the moment any key is shared, so no clause is silently lost. Pin
    /// both paths plus the empty/single shortcuts — a refactor that always
    /// merged would drop the second `age` clause here, matching the wrong docs.
    #[test]
    fn merge_filters_disjoint_merges_shared_keys_use_and() {
        // Disjoint keys → shallow merge into one object.
        let merged = op_merge_filters(json!({
            "filters": [{"status": "active"}, {"age": {"$gte": 18}}]
        }))
        .unwrap();
        assert_eq!(merged["filter"]["status"], json!("active"));
        assert_eq!(merged["filter"]["age"]["$gte"], json!(18));
        assert!(
            merged["filter"].get("$and").is_none(),
            "disjoint filters must NOT wrap in $and"
        );

        // Shared key (`age` twice) → $and so both clauses survive.
        let anded = op_merge_filters(json!({
            "filters": [{"age": {"$gte": 18}}, {"age": {"$lt": 65}}]
        }))
        .unwrap();
        let and = anded["filter"]["$and"]
            .as_array()
            .expect("shared-key merge must produce $and");
        assert_eq!(and.len(), 2, "both age clauses must be preserved");
        assert_eq!(and[0]["age"]["$gte"], json!(18));
        assert_eq!(and[1]["age"]["$lt"], json!(65));

        // Empty → match-all; single → unchanged.
        assert_eq!(
            op_merge_filters(json!({"filters": []})).unwrap()["filter"],
            json!({})
        );
        assert_eq!(
            op_merge_filters(json!({"filters": [{"x": 1}]})).unwrap()["filter"],
            json!({"x": 1})
        );
        assert!(op_merge_filters(json!({})).is_err());
        assert!(op_merge_filters(json!({"filters": [42]})).is_err());
    }

    /// `build_update` collects only the buckets supplied and normalizes `$unset`
    /// (array OR object) to the canonical `{field: ""}` shape mongo expects.
    /// Empty everything must error rather than emit an empty update (which mongo
    /// rejects with a confusing wire error).
    #[test]
    fn build_update_assembles_buckets_and_normalizes_unset() {
        let u = op_build_update(json!({
            "set": {"role": "admin"},
            "inc": {"logins": 1},
            "unset": ["tmp", "old"],
        }))
        .unwrap();
        assert_eq!(u["update"]["$set"]["role"], json!("admin"));
        assert_eq!(u["update"]["$inc"]["logins"], json!(1));
        // Array form normalized to {field: ""}.
        assert_eq!(u["update"]["$unset"]["tmp"], json!(""));
        assert_eq!(u["update"]["$unset"]["old"], json!(""));

        // Object form of unset normalizes the same way (values discarded).
        let u2 = op_build_update(json!({"unset": {"a": 1, "b": "anything"}})).unwrap();
        assert_eq!(u2["update"]["$unset"]["a"], json!(""));
        assert_eq!(u2["update"]["$unset"]["b"], json!(""));
        assert!(
            u2["update"].get("$set").is_none(),
            "absent buckets must not appear"
        );

        // Nothing supplied → error (never an empty {} update).
        assert!(op_build_update(json!({})).is_err());
        assert!(op_build_update(json!({"set": {}, "inc": {}})).is_err());
        // Non-string array entry in unset → error.
        assert!(op_build_update(json!({"unset": [1]})).is_err());
    }

    /// `build_sort` accepts `"-field"` shorthand and `[field, dir]` pairs and —
    /// the load-bearing property — preserves insertion order, since sort is
    /// order-sensitive. Pin the order with a reversed-vs-alphabetical key set so
    /// an accidental map sort is unambiguously caught.
    #[test]
    fn build_sort_preserves_order_and_parses_directions() {
        let s = op_build_sort(json!({
            "fields": ["-age", ["name", 1], ["score", "desc"], "city"]
        }))
        .unwrap();
        assert_eq!(s["sort"]["age"], json!(-1));
        assert_eq!(s["sort"]["name"], json!(1));
        assert_eq!(s["sort"]["score"], json!(-1));
        assert_eq!(s["sort"]["city"], json!(1));
        let keys: Vec<&str> = s["sort"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            vec!["age", "name", "score", "city"],
            "sort key order was not preserved — a sorting map crept in"
        );
        assert!(op_build_sort(json!({"fields": []})).is_err());
        assert!(op_build_sort(json!({"fields": [["f", 2]]})).is_err());
        assert!(op_build_sort(json!({"fields": [["", 1]]})).is_err());
    }

    /// `build_projection` rejects mixing include+exclude (mongo's rule) but lets
    /// `_id` ride alongside the opposite mode via the `id` flag. Pin both the
    /// inclusion/exclusion modes and the `_id` exception.
    #[test]
    fn build_projection_modes_and_id_exception() {
        assert_eq!(
            op_build_projection(json!({"include": ["a", "b"]})).unwrap()["projection"],
            json!({"a": 1, "b": 1})
        );
        assert_eq!(
            op_build_projection(json!({"exclude": ["secret"]})).unwrap()["projection"],
            json!({"secret": 0})
        );
        // _id may be dropped while including other fields (the documented exception).
        let p = op_build_projection(json!({"include": ["name"], "id": false})).unwrap();
        assert_eq!(p["projection"]["name"], json!(1));
        assert_eq!(p["projection"]["_id"], json!(0));
        // Mixing include+exclude (other than _id) is rejected.
        assert!(op_build_projection(json!({"include": ["a"], "exclude": ["b"]})).is_err());
        assert!(op_build_projection(json!({})).is_err());
    }

    /// `normalize_index_keys` mirrors `build_sort`'s direction parsing but also
    /// passes string index types (`2dsphere`, `text`, `hashed`) through
    /// verbatim, and preserves compound-key order. Pin the string-type path —
    /// a refactor that coerced every value to ±1 would silently break geo/text
    /// indexes.
    #[test]
    fn normalize_index_keys_directions_string_types_and_order() {
        let k = op_normalize_index_keys(json!({
            "keys": ["-created", ["loc", "2dsphere"], ["name", 1]]
        }))
        .unwrap();
        assert_eq!(k["keys"]["created"], json!(-1));
        assert_eq!(
            k["keys"]["loc"],
            json!("2dsphere"),
            "string index type was coerced — geo/text indexes would break"
        );
        assert_eq!(k["keys"]["name"], json!(1));
        let order: Vec<&str> = k["keys"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(order, vec!["created", "loc", "name"]);
        assert!(op_normalize_index_keys(json!({"keys": []})).is_err());
        assert!(op_normalize_index_keys(json!({"keys": [["f", 2]]})).is_err());
    }

    /// `in_filter` builds `$in`/`$nin` and `between_filter` builds a one-sided or
    /// two-sided range, rejecting a bound + its strict variant on the same side.
    /// Pin both builders' shapes — they exist so callers stop hand-assembling
    /// operator objects (and getting them subtly wrong).
    #[test]
    fn in_filter_and_between_filter_shapes() {
        assert_eq!(
            op_in_filter(json!({"field": "status", "values": ["a", "b"]})).unwrap()["filter"],
            json!({"status": {"$in": ["a", "b"]}})
        );
        assert_eq!(
            op_in_filter(json!({"field": "status", "values": ["x"], "negate": true})).unwrap()
                ["filter"],
            json!({"status": {"$nin": ["x"]}})
        );
        // Empty values is legal ($in: [] matches nothing — a valid query).
        assert_eq!(
            op_in_filter(json!({"field": "f", "values": []})).unwrap()["filter"],
            json!({"f": {"$in": []}})
        );
        assert!(op_in_filter(json!({"field": "", "values": []})).is_err());
        assert!(op_in_filter(json!({"field": "f"})).is_err());

        // Inclusive + exclusive two-sided range.
        assert_eq!(
            op_between_filter(json!({"field": "age", "gte": 18, "lt": 65})).unwrap()["filter"],
            json!({"age": {"$gte": 18, "$lt": 65}})
        );
        // One-sided is fine.
        assert_eq!(
            op_between_filter(json!({"field": "n", "gt": 0})).unwrap()["filter"],
            json!({"n": {"$gt": 0}})
        );
        // gte + gt on the same side is contradictory → error.
        assert!(op_between_filter(json!({"field": "x", "gte": 1, "gt": 2})).is_err());
        // No bound at all → error.
        assert!(op_between_filter(json!({"field": "x"})).is_err());
    }

    /// `build_regex_filter` escapes the literal input so metacharacters never act
    /// as a pattern, then anchors per the `anchor` opt and adds the `i` option on
    /// request. The escaping is the security-relevant property: user-supplied
    /// `.` / `^` / `$` must NOT broaden the match. Pin escaping + every anchor.
    #[test]
    fn build_regex_filter_escapes_and_anchors() {
        // Literal `a.b` must be escaped so `.` is not "any char".
        let exact = op_build_regex_filter(json!({
            "field": "name", "value": "a.b", "anchor": "exact"
        }))
        .unwrap();
        assert_eq!(exact["filter"]["name"]["$regex"], json!("^a\\.b$"));

        let prefix = op_build_regex_filter(json!({
            "field": "name", "value": "foo", "anchor": "prefix", "ignore_case": true
        }))
        .unwrap();
        assert_eq!(prefix["filter"]["name"]["$regex"], json!("^foo"));
        assert_eq!(prefix["filter"]["name"]["$options"], json!("i"));

        let suffix =
            op_build_regex_filter(json!({"field": "f", "value": "x", "anchor": "suffix"})).unwrap();
        assert_eq!(suffix["filter"]["f"]["$regex"], json!("x$"));

        // Default / contains → unanchored, no options key.
        let contains = op_build_regex_filter(json!({"field": "f", "value": "y"})).unwrap();
        assert_eq!(contains["filter"]["f"]["$regex"], json!("y"));
        assert!(contains["filter"]["f"].get("$options").is_none());

        assert!(
            op_build_regex_filter(json!({"field": "f", "value": "x", "anchor": "bad"})).is_err()
        );
        assert!(op_build_regex_filter(json!({"field": "", "value": "x"})).is_err());
    }

    /// `or_filter` is the disjunctive counterpart to `merge_filters`: 0 → {},
    /// 1 → pass-through (the `$or` wrapper would be redundant and changes
    /// nothing semantically), 2+ → `{ "$or": [...] }`. Pin all three arities
    /// plus the non-object rejection, since a stray scalar in the array would
    /// otherwise produce a query mongo silently mishandles.
    #[test]
    fn or_filter_arities_and_rejects_non_object() {
        assert_eq!(
            op_or_filter(json!({"filters": []})).unwrap()["filter"],
            json!({})
        );
        // Single filter returned unchanged — no redundant $or wrapper.
        assert_eq!(
            op_or_filter(json!({"filters": [{"a": 1}]})).unwrap()["filter"],
            json!({"a": 1})
        );
        assert_eq!(
            op_or_filter(json!({"filters": [{"a": 1}, {"b": 2}]})).unwrap()["filter"],
            json!({"$or": [{"a": 1}, {"b": 2}]})
        );
        assert!(op_or_filter(json!({})).is_err());
        assert!(op_or_filter(json!({"filters": [42]})).is_err());
    }

    /// `exists_filter` defaults to `true` (the common "has this field" case) and
    /// honors an explicit `false`. The default is the load-bearing bit — a caller
    /// who omits `exists` must get `$exists: true`, not `$exists: false`.
    #[test]
    fn exists_filter_default_true_and_explicit_false() {
        assert_eq!(
            op_exists_filter(json!({"field": "email"})).unwrap()["filter"],
            json!({"email": {"$exists": true}})
        );
        assert_eq!(
            op_exists_filter(json!({"field": "email", "exists": false})).unwrap()["filter"],
            json!({"email": {"$exists": false}})
        );
        // exists carried as 0/1 numbers (stryke's to_json shape) is honored too.
        assert_eq!(
            op_exists_filter(json!({"field": "x", "exists": 0})).unwrap()["filter"],
            json!({"x": {"$exists": false}})
        );
        assert!(op_exists_filter(json!({"field": ""})).is_err());
    }

    /// `elem_match_filter` wraps the WHOLE query under `$elemMatch` so multiple
    /// conditions bind to the same array element — the distinction from a plain
    /// dotted-key filter. Pin the shape and the non-object query rejection.
    #[test]
    fn elem_match_filter_shape_and_rejects_non_object() {
        assert_eq!(
            op_elem_match_filter(json!({
                "field": "scores", "query": {"$gte": 80, "$lt": 90}
            }))
            .unwrap()["filter"],
            json!({"scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}})
        );
        assert!(op_elem_match_filter(json!({"field": "f"})).is_err());
        assert!(op_elem_match_filter(json!({"field": "f", "query": 5})).is_err());
        assert!(op_elem_match_filter(json!({"field": "", "query": {}})).is_err());
    }

    /// `text_filter` always emits `$search` and only adds the optional knobs when
    /// supplied — an unconfigured call must be exactly `{$text: {$search: …}}`,
    /// not carry phantom `$caseSensitive`/`$language` keys that change behavior.
    #[test]
    fn text_filter_minimal_and_full() {
        assert_eq!(
            op_text_filter(json!({"search": "coffee shop"})).unwrap()["filter"],
            json!({"$text": {"$search": "coffee shop"}})
        );
        let full = op_text_filter(json!({
            "search": "café", "language": "fr",
            "case_sensitive": true, "diacritic_sensitive": false
        }))
        .unwrap();
        assert_eq!(
            full["filter"],
            json!({"$text": {
                "$search": "café",
                "$language": "fr",
                "$caseSensitive": true,
                "$diacriticSensitive": false
            }})
        );
        assert!(op_text_filter(json!({})).is_err());
    }

    /// `not_filter` wraps an operator expression under `$not`. The guard is the
    /// load-bearing part: `$not` is only valid around operator expressions, so a
    /// bare value, an empty object, or a logical operator ($or/$and/$nor) must be
    /// rejected rather than producing a query the server errors on at runtime.
    #[test]
    fn not_filter_wraps_operator_and_rejects_invalid() {
        assert_eq!(
            op_not_filter(json!({"field": "age", "expr": {"$gt": 5}})).unwrap()["filter"],
            json!({"age": {"$not": {"$gt": 5}}})
        );
        // Missing/empty expr.
        assert!(op_not_filter(json!({"field": "f"})).is_err());
        assert!(op_not_filter(json!({"field": "f", "expr": {}})).is_err());
        // Bare value is not an operator expression.
        assert!(op_not_filter(json!({"field": "f", "expr": 5})).is_err());
        // A field-key (non-$) inside expr is invalid for $not.
        assert!(op_not_filter(json!({"field": "f", "expr": {"a": 1}})).is_err());
        // Logical operators cannot be wrapped by $not.
        assert!(op_not_filter(json!({"field": "f", "expr": {"$or": [{"a": 1}]}})).is_err());
        assert!(op_not_filter(json!({"field": "", "expr": {"$gt": 1}})).is_err());
    }
}
