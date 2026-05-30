//! `stryke-mongo-helper` — MongoDB bridge binary.
//!
//! Wraps the official `mongodb` Rust driver. Output is NDJSON for streams
//! (find, aggregate, list-collections) or a single JSON object otherwise.
//! BSON converts to relaxed-extended JSON so `ObjectId`, dates, and
//! `NumberLong` round-trip cleanly.

use std::io::{self, BufRead, BufReader, BufWriter, Write};

use anyhow::{anyhow, bail, Context, Result};
use bson::{doc, Bson, Document};
use clap::{Args, Parser, Subcommand};
use futures_util::stream::TryStreamExt;
use mongodb::options::{ClientOptions, FindOneOptions, FindOptions, IndexOptions};
use mongodb::{Client, IndexModel};
use serde_json::Value as JsonValue;

#[derive(Parser, Debug)]
#[command(
    name = "stryke-mongo-helper",
    version,
    about = "MongoDB client for the stryke `mongo` package"
)]
struct Cli {
    #[command(flatten)]
    conn: Conn,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Debug, Clone)]
struct Conn {
    /// `mongodb://user:pass@host:port/db?…` or `mongodb+srv://…`.
    #[arg(long, short = 'u', env = "MONGODB_URI", global = true)]
    uri: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Find documents. `target` is `DB/COLLECTION`.
    Find {
        target: String,
        #[arg(long, default_value = "{}")]
        filter: String,
        #[arg(long)]
        projection: Option<String>,
        #[arg(long)]
        sort: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long)]
        skip: Option<u64>,
    },
    /// Find at most one matching document.
    FindOne {
        target: String,
        #[arg(long, default_value = "{}")]
        filter: String,
        #[arg(long)]
        projection: Option<String>,
    },
    /// Insert one document.
    InsertOne {
        target: String,
        #[arg(long)]
        doc: String,
    },
    /// Insert NDJSON from stdin, one document per line.
    InsertMany { target: String },
    /// Update at most one document.
    UpdateOne {
        target: String,
        #[arg(long)]
        filter: String,
        #[arg(long)]
        update: String,
        #[arg(long)]
        upsert: bool,
    },
    /// Update every matching document.
    UpdateMany {
        target: String,
        #[arg(long)]
        filter: String,
        #[arg(long)]
        update: String,
    },
    /// Replace a whole document (no `$set` operators — full doc replacement).
    ReplaceOne {
        target: String,
        #[arg(long)]
        filter: String,
        #[arg(long)]
        doc: String,
        #[arg(long)]
        upsert: bool,
    },
    /// Delete at most one document.
    DeleteOne {
        target: String,
        #[arg(long)]
        filter: String,
    },
    /// Delete every matching document.
    DeleteMany {
        target: String,
        #[arg(long)]
        filter: String,
    },
    /// Count documents matching a filter.
    Count {
        target: String,
        #[arg(long, default_value = "{}")]
        filter: String,
    },
    /// Run an aggregation pipeline. `--pipeline` is a JSON array.
    Aggregate {
        target: String,
        #[arg(long)]
        pipeline: String,
    },
    /// List all databases.
    ListDatabases,
    /// List collections in DB.
    ListCollections { db: String },
    /// Create an index. `--keys` is the key spec (`{"name":1}`).
    CreateIndex {
        target: String,
        #[arg(long)]
        keys: String,
        #[arg(long)]
        unique: bool,
        #[arg(long)]
        name: Option<String>,
    },
    /// Drop an index by name.
    DropIndex { target: String, name: String },
    /// List indexes on a collection.
    Indexes { target: String },
    /// `db.runCommand({ping:1})`.
    Ping,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("stryke-mongo-helper: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let client = make_client(&cli.conn).await?;
    match cli.cmd {
        Cmd::Find {
            target,
            filter,
            projection,
            sort,
            limit,
            skip,
        } => {
            cmd_find(
                &client,
                &target,
                &filter,
                projection.as_deref(),
                sort.as_deref(),
                limit,
                skip,
            )
            .await
        }
        Cmd::FindOne {
            target,
            filter,
            projection,
        } => cmd_find_one(&client, &target, &filter, projection.as_deref()).await,
        Cmd::InsertOne { target, doc } => cmd_insert_one(&client, &target, &doc).await,
        Cmd::InsertMany { target } => cmd_insert_many(&client, &target).await,
        Cmd::UpdateOne {
            target,
            filter,
            update,
            upsert,
        } => cmd_update(&client, &target, &filter, &update, false, upsert).await,
        Cmd::UpdateMany {
            target,
            filter,
            update,
        } => cmd_update(&client, &target, &filter, &update, true, false).await,
        Cmd::ReplaceOne {
            target,
            filter,
            doc,
            upsert,
        } => cmd_replace_one(&client, &target, &filter, &doc, upsert).await,
        Cmd::DeleteOne { target, filter } => cmd_delete(&client, &target, &filter, false).await,
        Cmd::DeleteMany { target, filter } => cmd_delete(&client, &target, &filter, true).await,
        Cmd::Count { target, filter } => cmd_count(&client, &target, &filter).await,
        Cmd::Aggregate { target, pipeline } => cmd_aggregate(&client, &target, &pipeline).await,
        Cmd::ListDatabases => cmd_list_databases(&client).await,
        Cmd::ListCollections { db } => cmd_list_collections(&client, &db).await,
        Cmd::CreateIndex {
            target,
            keys,
            unique,
            name,
        } => cmd_create_index(&client, &target, &keys, unique, name.as_deref()).await,
        Cmd::DropIndex { target, name } => cmd_drop_index(&client, &target, &name).await,
        Cmd::Indexes { target } => cmd_list_indexes(&client, &target).await,
        Cmd::Ping => cmd_ping(&client).await,
    }
}

/* ------------------------------------------------------------------------- */
/* connection + helpers                                                      */
/* ------------------------------------------------------------------------- */

async fn make_client(c: &Conn) -> Result<Client> {
    let uri = c
        .uri
        .clone()
        .unwrap_or_else(|| "mongodb://127.0.0.1:27017".to_string());
    let mut opts = ClientOptions::parse(&uri)
        .await
        .with_context(|| format!("parsing URI {uri}"))?;
    opts.app_name = Some("stryke-mongo-helper".to_string());
    Client::with_options(opts).context("creating Mongo client")
}

fn parse_target(t: &str) -> Result<(String, String)> {
    let (db, coll) = t
        .split_once('/')
        .or_else(|| t.split_once('.'))
        .ok_or_else(|| anyhow!("target must be `DB/COLLECTION` (got `{t}`)"))?;
    Ok((db.to_string(), coll.to_string()))
}

fn parse_doc(s: &str) -> Result<Document> {
    let v: JsonValue = serde_json::from_str(s).context("parsing JSON document")?;
    let bson_doc: Bson = Bson::try_from(v).context("converting JSON to BSON")?;
    let Bson::Document(d) = bson_doc else {
        bail!("expected a JSON object, got a non-object value");
    };
    Ok(d)
}

fn doc_to_json(d: &Document) -> JsonValue {
    Bson::Document(d.clone()).into_relaxed_extjson()
}

fn bson_to_json(b: &Bson) -> JsonValue {
    b.clone().into_relaxed_extjson()
}

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn emit_ndjson<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* commands                                                                  */
/* ------------------------------------------------------------------------- */

#[allow(clippy::too_many_arguments)]
async fn cmd_find(
    client: &Client,
    target: &str,
    filter: &str,
    projection: Option<&str>,
    sort: Option<&str>,
    limit: Option<i64>,
    skip: Option<u64>,
) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let f = parse_doc(filter)?;

    let mut opts = FindOptions::default();
    if let Some(p) = projection {
        opts.projection = Some(parse_doc(p)?);
    }
    if let Some(s) = sort {
        opts.sort = Some(parse_doc(s)?);
    }
    opts.limit = limit;
    opts.skip = skip;

    let mut cursor = collection
        .find(f)
        .with_options(opts)
        .await
        .context("find")?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(d) = cursor.try_next().await.context("cursor")? {
        emit_ndjson(&mut out, &doc_to_json(&d))?;
    }
    Ok(())
}

async fn cmd_find_one(
    client: &Client,
    target: &str,
    filter: &str,
    projection: Option<&str>,
) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let f = parse_doc(filter)?;
    let mut q = collection.find_one(f);
    if let Some(p) = projection {
        let mut o = FindOneOptions::default();
        o.projection = Some(parse_doc(p)?);
        q = q.with_options(o);
    }
    match q.await.context("find_one")? {
        Some(d) => emit_json(&doc_to_json(&d)),
        None => emit_json(&JsonValue::Null),
    }
}

async fn cmd_insert_one(client: &Client, target: &str, doc: &str) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let d = parse_doc(doc)?;
    let r = collection.insert_one(d).await.context("insert_one")?;
    emit_json(&serde_json::json!({
        "inserted_id": bson_to_json(&r.inserted_id),
    }))
}

async fn cmd_insert_many(client: &Client, target: &str) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let stdin = io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut docs: Vec<Document> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        docs.push(parse_doc(&line)?);
    }
    if docs.is_empty() {
        return emit_json(&serde_json::json!({ "inserted": 0, "ids": [] }));
    }
    let r = collection.insert_many(&docs).await.context("insert_many")?;
    let ids: Vec<JsonValue> = r.inserted_ids.values().map(bson_to_json).collect();
    emit_json(&serde_json::json!({
        "inserted": ids.len(),
        "ids": ids,
    }))
}

async fn cmd_update(
    client: &Client,
    target: &str,
    filter: &str,
    update: &str,
    many: bool,
    upsert: bool,
) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let f = parse_doc(filter)?;
    let u = parse_doc(update)?;
    let r = if many {
        collection
            .update_many(f, u)
            .upsert(upsert)
            .await
            .context("update_many")?
    } else {
        collection
            .update_one(f, u)
            .upsert(upsert)
            .await
            .context("update_one")?
    };
    emit_json(&serde_json::json!({
        "matched": r.matched_count,
        "modified": r.modified_count,
        "upserted_id": r.upserted_id.as_ref().map(bson_to_json),
    }))
}

async fn cmd_replace_one(
    client: &Client,
    target: &str,
    filter: &str,
    doc: &str,
    upsert: bool,
) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let f = parse_doc(filter)?;
    let d = parse_doc(doc)?;
    let r = collection
        .replace_one(f, d)
        .upsert(upsert)
        .await
        .context("replace_one")?;
    emit_json(&serde_json::json!({
        "matched": r.matched_count,
        "modified": r.modified_count,
        "upserted_id": r.upserted_id.as_ref().map(bson_to_json),
    }))
}

async fn cmd_delete(client: &Client, target: &str, filter: &str, many: bool) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let f = parse_doc(filter)?;
    let r = if many {
        collection.delete_many(f).await.context("delete_many")?
    } else {
        collection.delete_one(f).await.context("delete_one")?
    };
    emit_json(&serde_json::json!({ "deleted": r.deleted_count }))
}

async fn cmd_count(client: &Client, target: &str, filter: &str) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let f = parse_doc(filter)?;
    let n = collection
        .count_documents(f)
        .await
        .context("count_documents")?;
    emit_json(&serde_json::json!({ "count": n }))
}

async fn cmd_aggregate(client: &Client, target: &str, pipeline: &str) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let v: JsonValue = serde_json::from_str(pipeline).context("parsing --pipeline")?;
    let JsonValue::Array(stages) = v else {
        bail!("--pipeline must be a JSON array of stage objects");
    };
    let stage_docs: Vec<Document> = stages
        .into_iter()
        .map(|s| {
            let b = Bson::try_from(s).context("stage to BSON")?;
            match b {
                Bson::Document(d) => Ok(d),
                _ => Err(anyhow!("each pipeline stage must be a JSON object")),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let mut cursor = collection
        .aggregate(stage_docs)
        .await
        .context("aggregate")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(d) = cursor.try_next().await.context("cursor")? {
        emit_ndjson(&mut out, &doc_to_json(&d))?;
    }
    Ok(())
}

async fn cmd_list_databases(client: &Client) -> Result<()> {
    let dbs = client.list_databases().await.context("list_databases")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for d in dbs {
        emit_ndjson(
            &mut out,
            &serde_json::json!({
                "name": d.name,
                "size_on_disk": d.size_on_disk,
                "empty": d.empty,
            }),
        )?;
    }
    Ok(())
}

async fn cmd_list_collections(client: &Client, db: &str) -> Result<()> {
    let names = client
        .database(db)
        .list_collection_names()
        .await
        .context("list_collection_names")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for n in names {
        emit_ndjson(&mut out, &serde_json::json!({ "name": n }))?;
    }
    Ok(())
}

async fn cmd_create_index(
    client: &Client,
    target: &str,
    keys: &str,
    unique: bool,
    name: Option<&str>,
) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let key_doc = parse_doc(keys)?;
    let mut opts = IndexOptions::builder().unique(unique).build();
    if let Some(n) = name {
        opts.name = Some(n.to_string());
    }
    let model = IndexModel::builder().keys(key_doc).options(opts).build();
    let r = collection
        .create_index(model)
        .await
        .context("create_index")?;
    emit_json(&serde_json::json!({ "name": r.index_name }))
}

async fn cmd_drop_index(client: &Client, target: &str, name: &str) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    collection.drop_index(name).await.context("drop_index")?;
    emit_json(&serde_json::json!({ "name": name, "dropped": true }))
}

async fn cmd_list_indexes(client: &Client, target: &str) -> Result<()> {
    let (db, coll) = parse_target(target)?;
    let collection = client.database(&db).collection::<Document>(&coll);
    let mut cursor = collection.list_indexes().await.context("list_indexes")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(model) = cursor.try_next().await.context("cursor")? {
        let name = model
            .options
            .as_ref()
            .and_then(|o| o.name.clone())
            .unwrap_or_default();
        let unique = model
            .options
            .as_ref()
            .and_then(|o| o.unique)
            .unwrap_or(false);
        emit_ndjson(
            &mut out,
            &serde_json::json!({
                "name": name,
                "keys": doc_to_json(&model.keys),
                "unique": unique,
            }),
        )?;
    }
    Ok(())
}

async fn cmd_ping(client: &Client) -> Result<()> {
    let r = client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .context("ping")?;
    emit_json(&doc_to_json(&r))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;

    // ─── parse_target ────────────────────────────────────────────────

    #[test]
    fn parse_target_slash_separator() {
        let (db, c) = parse_target("mydb/users").unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(c, "users");
    }

    #[test]
    fn parse_target_dot_separator() {
        let (db, c) = parse_target("mydb.events").unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(c, "events");
    }

    #[test]
    fn parse_target_slash_wins_when_both_present() {
        // split_once('/') runs first → uses slash boundary.
        let (db, c) = parse_target("a.b/c.d").unwrap();
        assert_eq!(db, "a.b");
        assert_eq!(c, "c.d");
    }

    #[test]
    fn parse_target_missing_separator_errors() {
        let err = parse_target("noseparator").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("DB/COLLECTION"), "msg = {msg}");
    }

    #[test]
    fn parse_target_empty_db_or_coll_allowed_caller_validates() {
        // The parser is liberal — empty halves accepted; downstream
        // call_to_mongo errors clearly. Pinning current behavior so a
        // future tightening is a deliberate breaking change.
        let (db, c) = parse_target("/foo").unwrap();
        assert_eq!(db, "");
        assert_eq!(c, "foo");
        let (db, c) = parse_target("bar/").unwrap();
        assert_eq!(db, "bar");
        assert_eq!(c, "");
    }

    // ─── parse_doc ───────────────────────────────────────────────────

    #[test]
    fn parse_doc_simple_object() {
        let d = parse_doc(r#"{"name":"alice","age":30}"#).unwrap();
        assert_eq!(d.get_str("name").unwrap(), "alice");
        assert_eq!(d.get_i32("age").unwrap(), 30);
    }

    #[test]
    fn parse_doc_empty_object() {
        let d = parse_doc("{}").unwrap();
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn parse_doc_invalid_json_errors() {
        let err = parse_doc("{not valid json}").unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("parsing"));
    }

    #[test]
    fn parse_doc_array_not_object_errors() {
        let err = parse_doc("[1,2,3]").unwrap_err();
        assert!(format!("{err}").contains("object"));
    }

    #[test]
    fn parse_doc_string_not_object_errors() {
        let err = parse_doc(r#""just a string""#).unwrap_err();
        assert!(format!("{err}").contains("object"));
    }

    #[test]
    fn parse_doc_nested_object() {
        let d = parse_doc(r#"{"outer":{"inner":42}}"#).unwrap();
        let inner = d.get_document("outer").unwrap();
        assert_eq!(inner.get_i32("inner").unwrap(), 42);
    }

    // ─── doc_to_json / bson_to_json (relaxed extended JSON) ──────────

    #[test]
    fn doc_to_json_roundtrip_scalars() {
        let mut d = Document::new();
        d.insert("name", "bob");
        d.insert("count", 99i32);
        let j = doc_to_json(&d);
        assert_eq!(j["name"], "bob");
        assert_eq!(j["count"], 99);
    }

    #[test]
    fn doc_to_json_does_not_mutate_input() {
        let mut d = Document::new();
        d.insert("k", "v");
        let len_before = d.len();
        let _ = doc_to_json(&d);
        assert_eq!(d.len(), len_before);
    }

    #[test]
    fn bson_to_json_int64_relaxed_form() {
        // Relaxed extJSON: i64 in safe range emits as plain JSON number,
        // not the canonical {"$numberLong": "..."} form.
        let b = Bson::Int64(123_456);
        let j = bson_to_json(&b);
        assert_eq!(j, serde_json::json!(123_456));
    }

    #[test]
    fn bson_to_json_null_round_trips() {
        let b = Bson::Null;
        assert_eq!(bson_to_json(&b), serde_json::Value::Null);
    }

    // ─── emit_ndjson (generic line writer) ───────────────────────────

    #[test]
    fn emit_ndjson_appends_newline() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &serde_json::json!({"a": 1})).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "{\"a\":1}\n");
    }

    #[test]
    fn emit_ndjson_multiple_calls_one_line_each() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &serde_json::json!({"a": 1})).unwrap();
        emit_ndjson(&mut buf, &serde_json::json!({"b": 2})).unwrap();
        emit_ndjson(&mut buf, &serde_json::json!({"c": 3})).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s.lines().count(), 3);
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn emit_ndjson_handles_unicode() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &serde_json::json!({"name": "日本語"})).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("日本語") || s.contains("\\u65e5"));
    }

    #[test]
    fn bson_to_json_bool() {
        assert_eq!(bson_to_json(&Bson::Boolean(true)), serde_json::json!(true));
    }

    #[test]
    fn bson_to_json_double() {
        let j = bson_to_json(&Bson::Double(2.5));
        assert_eq!(j, serde_json::json!(2.5));
    }

    #[test]
    fn bson_to_json_array() {
        let b = Bson::Array(vec![Bson::Int32(1), Bson::String("a".into())]);
        let j = bson_to_json(&b);
        assert_eq!(j, serde_json::json!([1, "a"]));
    }

    #[test]
    fn parse_doc_oid_extended_json() {
        let d = parse_doc(r#"{"_id":{"$oid":"507f1f77bcf86cd799439011"}}"#).unwrap();
        assert!(d.contains_key("_id"));
    }

    #[test]
    fn parse_target_collection_with_underscore() {
        let (db, c) = parse_target("analytics/events_raw").unwrap();
        assert_eq!(db, "analytics");
        assert_eq!(c, "events_raw");
    }

    #[test]
    fn doc_to_json_nested_document() {
        let mut inner = Document::new();
        inner.insert("x", 1i32);
        let mut d = Document::new();
        d.insert("nested", inner);
        let j = doc_to_json(&d);
        assert_eq!(j["nested"]["x"], 1);
    }

    #[test]
    fn parse_doc_null_value() {
        let d = parse_doc(r#"{"k":null}"#).unwrap();
        assert!(matches!(d.get("k"), Some(Bson::Null)));
    }

    #[test]
    fn bson_to_json_document_nested() {
        let mut inner = Document::new();
        inner.insert("x", 1i32);
        let b = Bson::Document(inner);
        assert_eq!(bson_to_json(&b)["x"], 1);
    }

    #[test]
    fn bson_to_json_binary_relaxed() {
        let b = Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: b"\x00\x01".to_vec(),
        });
        let j = bson_to_json(&b);
        assert!(j.is_object() || j.is_string());
    }

    #[test]
    fn parse_target_collection_with_dots_in_name() {
        let (db, c) = parse_target("db.coll.name").unwrap();
        assert_eq!(db, "db");
        assert_eq!(c, "coll.name");
    }

    #[test]
    fn doc_to_json_empty_document() {
        let d = Document::new();
        let j = doc_to_json(&d);
        assert!(j.as_object().unwrap().is_empty());
    }

    #[test]
    fn parse_doc_boolean_fields() {
        let d = parse_doc(r#"{"ok":true,"fail":false}"#).unwrap();
        assert!(d.get_bool("ok").unwrap());
        assert!(!d.get_bool("fail").unwrap());
    }

    #[test]
    fn bson_to_json_string_utf8() {
        assert_eq!(
            bson_to_json(&Bson::String("hi".into())),
            serde_json::json!("hi")
        );
    }

    #[test]
    fn parse_doc_array_field_value() {
        let d = parse_doc(r#"{"tags":["a","b"]}"#).unwrap();
        let arr = d.get_array("tags").unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn bson_to_json_double_zero() {
        assert_eq!(bson_to_json(&Bson::Double(0.0)), serde_json::json!(0.0));
    }

    #[test]
    fn parse_doc_i32_field() {
        let d = parse_doc(r#"{"n":42}"#).unwrap();
        assert_eq!(d.get_i32("n").unwrap(), 42);
    }

    #[test]
    fn parse_target_long_db_and_coll_names() {
        let (db, c) = parse_target("warehouse_2024/sales_by_region").unwrap();
        assert_eq!(db, "warehouse_2024");
        assert_eq!(c, "sales_by_region");
    }

    #[test]
    fn doc_to_json_preserves_bool() {
        let mut d = Document::new();
        d.insert("ok", false);
        assert_eq!(doc_to_json(&d)["ok"], false);
    }

    #[test]
    fn parse_doc_rejects_number_top_level() {
        assert!(parse_doc("42").is_err());
    }

    #[test]
    fn bson_to_json_int32() {
        assert_eq!(bson_to_json(&Bson::Int32(7)), serde_json::json!(7));
    }

    #[test]
    fn bson_to_json_int64_large() {
        let b = Bson::Int64(9_000_000_000);
        let j = bson_to_json(&b);
        assert_eq!(j, serde_json::json!(9_000_000_000i64));
    }

    #[test]
    fn parse_target_single_slash_only() {
        let (db, c) = parse_target("onlydb/coll").unwrap();
        assert_eq!(db, "onlydb");
        assert_eq!(c, "coll");
    }

    #[test]
    fn parse_doc_empty_string_value() {
        let d = parse_doc(r#"{"k":""}"#).unwrap();
        assert_eq!(d.get_str("k").unwrap(), "");
    }

    #[test]
    fn bson_to_json_array_empty() {
        assert_eq!(bson_to_json(&Bson::Array(vec![])), serde_json::json!([]));
    }

    #[test]
    fn doc_to_json_multiple_fields() {
        let mut d = Document::new();
        d.insert("a", 1i32);
        d.insert("b", "x");
        let j = doc_to_json(&d);
        assert_eq!(j["a"], 1);
        assert_eq!(j["b"], "x");
    }

    #[test]
    fn parse_target_db_with_hyphen() {
        let (db, c) = parse_target("my-db/events").unwrap();
        assert_eq!(db, "my-db");
        assert_eq!(c, "events");
    }

    #[test]
    fn parse_doc_float_field() {
        let d = parse_doc(r#"{"x":1.5}"#).unwrap();
        assert_eq!(d.get_f64("x").unwrap(), 1.5);
    }

    #[test]
    fn emit_ndjson_null_value() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &serde_json::Value::Null).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "null\n");
    }

    #[test]
    fn parse_doc_negative_int() {
        let d = parse_doc(r#"{"n":-1}"#).unwrap();
        assert_eq!(d.get_i32("n").unwrap(), -1);
    }

    #[test]
    fn bson_to_json_boolean_false() {
        assert_eq!(
            bson_to_json(&Bson::Boolean(false)),
            serde_json::json!(false)
        );
    }

    #[test]
    fn parse_target_leading_slash_liberal() {
        let (db, c) = parse_target("/db/coll").unwrap();
        assert_eq!(db, "");
        assert_eq!(c, "db/coll");
    }

    #[test]
    fn doc_to_json_int64_field() {
        let mut d = Document::new();
        d.insert("n", 1_000_000_000i64);
        assert_eq!(doc_to_json(&d)["n"], 1_000_000_000);
    }

    #[test]
    fn parse_doc_empty_object_string() {
        assert_eq!(parse_doc("{}").unwrap().len(), 0);
    }

    #[test]
    fn emit_ndjson_number() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &serde_json::json!(7)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "7\n");
    }

    #[test]
    fn parse_doc_string_with_quotes() {
        let d = parse_doc(r#"{"msg":"say \"hi\""}"#).unwrap();
        assert_eq!(d.get_str("msg").unwrap(), "say \"hi\"");
    }

    #[test]
    fn bson_to_json_array_of_bools() {
        let b = Bson::Array(vec![Bson::Boolean(true), Bson::Boolean(false)]);
        assert_eq!(bson_to_json(&b), serde_json::json!([true, false]));
    }

    #[test]
    fn parse_doc_u64_in_range() {
        let d = parse_doc(r#"{"n":100}"#).unwrap();
        assert_eq!(d.get_i32("n").unwrap(), 100);
    }

    #[test]
    fn bson_to_json_string_empty() {
        assert_eq!(
            bson_to_json(&Bson::String(String::new())),
            serde_json::json!("")
        );
    }

    #[test]
    fn parse_target_dot_in_collection_name() {
        let (db, c) = parse_target("mydb.ev.ents").unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(c, "ev.ents");
    }

    #[test]
    fn doc_to_json_array_field() {
        let mut d = Document::new();
        d.insert("tags", vec!["a", "b"]);
        let j = doc_to_json(&d);
        assert_eq!(j["tags"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn emit_ndjson_bool_true() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &serde_json::json!(true)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "true\n");
    }

    #[test]
    fn parse_doc_nested_array() {
        let d = parse_doc(r#"{"matrix":[[1,2],[3,4]]}"#).unwrap();
        assert!(d.get_array("matrix").unwrap().len() == 2);
    }

    #[test]
    fn bson_to_json_document_empty() {
        assert_eq!(
            bson_to_json(&Bson::Document(Document::new())),
            serde_json::json!({})
        );
    }

    #[test]
    fn parse_target_no_separator_errors() {
        assert!(parse_target("nosep").is_err());
    }

    #[test]
    fn parse_doc_i64_field() {
        let d = parse_doc(r#"{"n":9223372036854775807}"#).unwrap();
        assert_eq!(d.get_i64("n").unwrap(), 9223372036854775807);
    }

    #[test]
    fn bson_to_json_double_negative() {
        assert_eq!(bson_to_json(&Bson::Double(-1.5)), serde_json::json!(-1.5));
    }

    #[test]
    fn emit_ndjson_false_bool() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &serde_json::json!(false)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "false\n");
    }

    #[test]
    fn parse_target_slash_db_coll() {
        let (db, c) = parse_target("analytics/events").unwrap();
        assert_eq!(db, "analytics");
        assert_eq!(c, "events");
    }

    #[test]
    fn doc_to_json_bool_field() {
        let mut d = Document::new();
        d.insert("ok", true);
        assert_eq!(doc_to_json(&d)["ok"], serde_json::json!(true));
    }

    #[test]
    fn parse_doc_empty_array() {
        let d = parse_doc(r#"{"items":[]}"#).unwrap();
        assert!(d.get_array("items").unwrap().is_empty());
    }

    #[test]
    fn bson_to_json_binary_empty() {
        let b = Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![],
        });
        let j = bson_to_json(&b);
        assert!(j.get("$binary").is_some() || j.is_object());
    }

    #[test]
    fn parse_target_underscore_in_db() {
        let (db, c) = parse_target("my_db.coll").unwrap();
        assert_eq!(db, "my_db");
        assert_eq!(c, "coll");
    }

    // ─── parse_target / parse_doc error-shape pins ───────────────────
    //
    // CLI users grep mongosh-style errors for both the offending input
    // and the expected grammar. Drift here silently changes script
    // behavior that depends on those substrings.

    #[test]
    fn parse_target_error_mentions_expected_form() {
        let err = parse_target("nothing").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("DB/COLLECTION"),
            "error should template expected form; got: {msg}"
        );
    }

    #[test]
    fn parse_target_error_echoes_offending_input() {
        let err = parse_target("totally bad").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("totally bad"),
            "error should echo offending input; got: {msg}"
        );
    }

    #[test]
    fn parse_doc_rejects_array_with_object_hint() {
        let err = parse_doc("[1, 2, 3]").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("expected a JSON object"),
            "error must hint at the missing object; got: {msg}"
        );
    }

    #[test]
    fn parse_doc_rejects_scalar_with_object_hint() {
        let err = parse_doc("42").unwrap_err();
        assert!(format!("{err}").contains("expected a JSON object"));
    }

    #[test]
    fn parse_doc_surfaces_parse_json_context() {
        // anyhow chain must carry the `parsing JSON document` context
        // so callers can pattern-match on it.
        let err = parse_doc("not-json").unwrap_err();
        let chain: Vec<_> = err.chain().map(|c| c.to_string()).collect();
        assert!(
            chain.iter().any(|s| s.contains("parsing JSON document")),
            "expected `parsing JSON document` context in chain; got {chain:?}"
        );
    }

    // ─── clap parsing — Cli top-level + Cmd routing ─────────────────────
    // Pin the user-facing CLI contract: required positionals, filter
    // default, upsert defaults. Drift in upsert/default-filter would
    // silently change which documents are matched or created.

    fn parse_cli(args: &[&str]) -> Result<Cli, clap::Error> {
        let mut argv = vec!["stryke-mongo-helper"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
    }

    #[test]
    fn cli_list_databases_unit_variant() {
        let cli = parse_cli(&["list-databases"]).expect("parse");
        assert!(matches!(cli.cmd, Cmd::ListDatabases));
        assert!(cli.conn.uri.is_none(), "no --uri = driver picks default");
    }

    #[test]
    fn cli_find_requires_target_positional() {
        let err = parse_cli(&["find"]).expect_err("missing target");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn cli_find_filter_default_is_empty_object() {
        // Pin: bare `find db/coll` means filter={} → match everything.
        // A drift here (e.g. requiring --filter) would break the most
        // common one-liner usage.
        let cli = parse_cli(&["find", "mydb/users"]).expect("parse");
        match cli.cmd {
            Cmd::Find { target, filter, .. } => {
                assert_eq!(target, "mydb/users");
                assert_eq!(filter, "{}");
            }
            _ => panic!("expected Find"),
        }
    }

    #[test]
    fn cli_update_one_upsert_defaults_false() {
        // Pin: upsert must be opt-in. Default-true would silently create
        // documents on every UpdateOne miss, masking app-level bugs.
        let cli = parse_cli(&[
            "update-one",
            "db/c",
            "--filter",
            "{}",
            "--update",
            "{\"$set\":{\"x\":1}}",
        ])
        .expect("parse");
        match cli.cmd {
            Cmd::UpdateOne { upsert, .. } => assert!(!upsert),
            _ => panic!("expected UpdateOne"),
        }
    }

    #[test]
    fn cli_aggregate_requires_pipeline_flag() {
        // Bare `aggregate db/c` without --pipeline is meaningless;
        // the parser must catch it before the driver does.
        let err = parse_cli(&["aggregate", "db/c"]).expect_err("missing --pipeline");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn cli_count_filter_default_is_empty_object() {
        // Same convention as Find — bare `count db/c` counts everything.
        let cli = parse_cli(&["count", "logs/events"]).expect("parse");
        match cli.cmd {
            Cmd::Count { target, filter, .. } => {
                assert_eq!(target, "logs/events");
                assert_eq!(filter, "{}");
            }
            _ => panic!("expected Count"),
        }
    }

    // ─── clap parsing — additional Cmd surfaces (round 2) ──────────────
    // Previous round pinned find/find-filter/update-upsert/aggregate/count.
    // These pin: FindOne mirrors Find's empty-filter default; InsertOne /
    // DeleteOne / DeleteMany / ReplaceOne required flags; CreateIndex
    // --keys required and --unique opt-in (no silent uniqueness drift);
    // DropIndex two-positional contract; Ping/Indexes/ListCollections
    // routing including required positionals.

    #[test]
    fn cli_find_one_filter_default_and_projection_optional() {
        // Pin: FindOne's --filter default is `{}` (mirrors Find — same
        // one-liner convention); --projection default None.
        let cli = parse_cli(&["find-one", "db/c"]).expect("parse");
        match cli.cmd {
            Cmd::FindOne {
                target,
                filter,
                projection,
            } => {
                assert_eq!(target, "db/c");
                assert_eq!(filter, "{}");
                assert!(projection.is_none());
            }
            _ => panic!("expected FindOne"),
        }
    }

    #[test]
    fn cli_insert_one_and_delete_required_flags() {
        // Pin: InsertOne requires --doc; DeleteOne/DeleteMany require
        // --filter (no implicit delete-everything safety net).
        use clap::error::ErrorKind::MissingRequiredArgument;
        assert_eq!(
            parse_cli(&["insert-one", "db/c"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        assert_eq!(
            parse_cli(&["delete-one", "db/c"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        assert_eq!(
            parse_cli(&["delete-many", "db/c"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        // Drift to defaulting --filter='{}' on Delete would silently nuke
        // collections — the absence of a default is the safety.
    }

    #[test]
    fn cli_replace_one_upsert_default_false_and_doc_required() {
        // ReplaceOne mirrors UpdateOne's upsert default-false safety.
        use clap::error::ErrorKind::MissingRequiredArgument;
        assert_eq!(
            parse_cli(&["replace-one", "db/c"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        let cli = parse_cli(&[
            "replace-one",
            "db/c",
            "--filter",
            "{}",
            "--doc",
            r#"{"x":1}"#,
        ])
        .expect("parse");
        match cli.cmd {
            Cmd::ReplaceOne {
                target,
                filter,
                doc,
                upsert,
            } => {
                assert_eq!(target, "db/c");
                assert_eq!(filter, "{}");
                assert_eq!(doc, r#"{"x":1}"#);
                assert!(!upsert);
            }
            _ => panic!("expected ReplaceOne"),
        }
    }

    #[test]
    fn cli_create_index_keys_required_unique_default_false() {
        // Pin: --keys is the index spec; without it the call is meaningless.
        // --unique defaults false; default-true would silently fail inserts
        // on duplicate keys for existing indexes.
        use clap::error::ErrorKind::MissingRequiredArgument;
        assert_eq!(
            parse_cli(&["create-index", "db/c"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        let cli = parse_cli(&["create-index", "db/c", "--keys", r#"{"name":1}"#]).expect("parse");
        match cli.cmd {
            Cmd::CreateIndex {
                target,
                keys,
                unique,
                name,
            } => {
                assert_eq!(target, "db/c");
                assert_eq!(keys, r#"{"name":1}"#);
                assert!(!unique);
                assert!(name.is_none());
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn cli_drop_index_two_positionals_and_ping_indexes_listcollections_routing() {
        // Pin: DropIndex requires both target AND name. Ping is a unit
        // variant; Indexes/ListCollections each require one positional.
        use clap::error::ErrorKind::MissingRequiredArgument;
        assert_eq!(
            parse_cli(&["drop-index"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        assert_eq!(
            parse_cli(&["drop-index", "db/c"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        let cli = parse_cli(&["drop-index", "db/c", "idx_name_1"]).expect("parse");
        match cli.cmd {
            Cmd::DropIndex { target, name } => {
                assert_eq!(target, "db/c");
                assert_eq!(name, "idx_name_1");
            }
            _ => panic!("expected DropIndex"),
        }

        assert!(matches!(parse_cli(&["ping"]).unwrap().cmd, Cmd::Ping));
        assert_eq!(
            parse_cli(&["indexes"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        assert_eq!(
            parse_cli(&["list-collections"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
    }
}
