```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ m o n g o ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-mongo/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-mongo/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[MONGODB CLIENT FOR STRYKE // CRUD + AGGREGATION + INDEX ADMIN]`

> *"Documents, one stryke pipe at a time."*

MongoDB client for stryke. CRUD, aggregation, index admin against any
MongoDB 5.0+ standalone, replica set, or sharded cluster. Opt-in package
tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-mysql`](https://github.com/MenkeTechnologies/stryke-mysql) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-redis`](https://github.com/MenkeTechnologies/stryke-redis) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] CLI: `mongo`](#0x02-cli-mongo)
- [\[0x03\] API reference](#0x03-api-reference)
- [\[0x04\] BSON type encoding](#0x04-bson-type-encoding)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] Dev workflow](#0x07-dev-workflow)
- [\[0x08\] Layout](#0x08-layout)
- [\[0x09\] Roadmap](#0x09-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-mongo
```

From a local checkout:

```sh
cd ~/projects/stryke-mongo
cargo build --release
s pkg install -g .
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use Mongo`. A shared tokio
runtime + `mongodb::Client` cache keyed by connection URI is held in
`OnceCell` — no fork-per-call, no fresh TCP+TLS+auth handshake.

## [0x01] Quick start

```stryke
use Mongo

$ENV{MONGODB_URI} = "mongodb://localhost:27017"

# Insert / find / update / delete — target is `DB/COLLECTION`.
my $r = Mongo::insert_one "app/users",
                          { name => "alice", age => 30, role => "admin" }
p to_json $r->{inserted_id}             # { "$oid": "65f9..." }

Mongo::insert_many "app/users", [
    { name => "bob",     age => 25 },
    { name => "charlie", age => 35 },
]

p Mongo::count "app/users"

# Filters and operators use full Mongo query syntax.
my @over30 = Mongo::find "app/users",
                         filter => { age => { '$gt' => 30 } },
                         sort   => { age => -1 },
                         limit  => 100
@over30 |> ep

# Callback-per-doc variant (cdylib materializes the result, then iterates).
Mongo::find_stream "app/events",
    filter   => { ts => { '$gte' => $cutoff } },
    callback => sub ($d) { process $d }

# Updates.
Mongo::update_one "app/users",
                  { name => "alice" },
                  { '$set' => { role => "superadmin" } }
Mongo::update_many "app/users", { active => 1 }, { '$inc' => { score => 1 } }

# Replace whole document.
Mongo::replace_one "app/users", { _id => $oid }, { ...new doc... }

# Aggregation pipeline (any standard stage).
my @top = Mongo::aggregate "app/orders", [
    { '$match' => { status => "paid" } },
    { '$group' => { _id => '$customer', total => { '$sum' => '$amount' } } },
    { '$sort'  => { total => -1 } },
    { '$limit' => 10 },
]

# Index admin.
my $idx = Mongo::create_index "app/users", { email => 1 }
Mongo::indexes "app/users" |> ep
Mongo::drop_index "app/users", $idx

# Discovery.
my @dbs   = Mongo::list_databases
my @colls = Mongo::list_collections "app"
```

URI overrides on every public fn:

```stryke
my %prod = (uri => "mongodb+srv://user:pass\@cluster.example.com")
Mongo::find "logs/errors", filter => {...}, %prod
```

## [0x02] CLI: `mongo`

```sh
mongo find       app/users --filter='{"age":{"$gt":30}}' --sort='{"age":-1}' --limit=20
mongo find-one   app/users --filter='{"name":"alice"}'
mongo insert-one app/users --doc='{"name":"alice","age":30}'
cat docs.ndjson | mongo insert-many app/users
mongo update-one app/users --filter='{"name":"alice"}' --update='{"$set":{"role":"admin"}}'
mongo update-many app/users --filter='{"active":true}' --update='{"$inc":{"score":1}}'
mongo replace-one app/users --filter='{"_id":{"$oid":"..."}}' --doc='{"name":"x"}' --upsert
mongo delete-one app/users --filter='{"name":"alice"}'
mongo delete-many app/users --filter='{}'

mongo count      app/users [--filter='{...}']
mongo aggregate  app/users --pipeline='[{"$group":{"_id":"$role","n":{"$sum":1}}}]'

mongo list-databases
mongo list-collections app
mongo create-index app/users --keys='{"email":1}' --unique --name=uniq_email
mongo drop-index   app/users uniq_email
mongo indexes      app/users

mongo ping
mongo build                              # cargo build --release
mongo version
```

Global flags (also env vars):

```
-u, --uri URI         $MONGODB_URI       # mongodb:// or mongodb+srv://
```

## [0x03] API reference

### Read paths

```stryke
Mongo::find         $target, %opts → @docs         # opts: filter, projection, sort, limit, skip
Mongo::find_one     $target, %opts → \%doc | undef
Mongo::find_stream  $target, %opts → $count        # callback per doc
Mongo::count        $target, %opts → $n
Mongo::aggregate    $target, \@pipeline, %opts → @docs
```

### Convenience composites

Pure-stryke helpers over `count` / `find_one` / `list_collections` — no
extra round trips beyond the primitive they wrap.

```stryke
Mongo::exists            $target, $filter, %opts → 1 | 0      # count($filter) > 0
Mongo::find_value        $target, $filter, $field, %opts → $value | undef   # one field, projected
Mongo::collection_exists $db, $coll, %opts → 1 | 0           # $coll in list_collections($db)
```

### Write paths

```stryke
Mongo::insert_one   $target, \%doc, %opts → { inserted_id }
Mongo::insert_many  $target, \@docs, %opts → $inserted_count
Mongo::update_one   $target, \%filter, \%update, %opts → { matched_count, modified_count, upserted_id }
Mongo::update_many  $target, \%filter, \%update, %opts → { matched_count, modified_count, upserted_id }
Mongo::replace_one  $target, \%filter, \%doc, %opts → { matched_count, modified_count, upserted_id }
Mongo::delete_one   $target, \%filter, %opts → $deleted_count
Mongo::delete_many  $target, \%filter, %opts → $deleted_count
```

Write `%opts`: `upsert` (insert when no match), `array_filters` (for
positional `$[<id>]` updates).

### Atomic findAndModify

```stryke
Mongo::find_one_and_update   $target, \%filter, \%update, %opts → \%doc | undef
Mongo::find_one_and_replace  $target, \%filter, \%doc, %opts → \%doc | undef
Mongo::find_one_and_delete   $target, \%filter, %opts → \%doc | undef
```

`%opts`: `return` (`"before"` | `"after"`), `upsert`, `sort`, `projection`,
`array_filters` (update only).

### Aggregation helpers

```stryke
Mongo::distinct          $target, $field, %opts → @values     # opts: filter
Mongo::estimated_count   $target, %opts → $n                  # fast metadata count
```

### Metadata + admin

```stryke
Mongo::list_databases    %opts → @names
Mongo::list_collections  $db, %opts → @names
Mongo::create_collection $db, $coll, %opts → { ok, created }
Mongo::drop_collection   $target, %opts → { ok, dropped }
Mongo::create_index      $target, \%keys, %opts → $index_name
Mongo::create_indexes    $target, \@indexes, %opts → \%result   # [{keys, name?, unique?}]
Mongo::drop_index        $target, $name, %opts → 1 | ""
Mongo::indexes           $target, %opts → @specs
Mongo::run_command       $db, \%command, %opts → \%result     # arbitrary db command
Mongo::rename_collection $target, $to, %opts → { ok, renamed } # opt: drop_target
Mongo::coll_stats        $target, %opts → \%stats             # collStats
Mongo::db_stats          $db, %opts → \%stats                 # dbStats
Mongo::explain           $target, %opts → \%plan              # opts: filter | pipeline, verbosity
Mongo::server_status     %opts → \%status                     # opts: db (default admin)
Mongo::ping              %opts → 1 | ""
```

### Pure helpers (no connection)

```stryke
Mongo::parse_connection_string($uri) → { scheme, srv, user, password, hosts:[{host,port}], database, params }
Mongo::build_connection_string(%opts) → $uri   # inverse: hosts/srv/user/password/database/params → mongodb[+srv]:// URI (percent-encoded)
Mongo::redact_connection_string($uri, %opts) → $uri   # mask the password (default ***) for safe logging; rest preserved byte-for-byte; opts: mask
Mongo::parse_namespace($ns)          → { db, collection }   # split on first dot
Mongo::build_namespace($db, $coll)   → $ns                 # join db.collection; inverse of parse_namespace
Mongo::valid_collection_name($name, $db?) → { name, valid, reason }   # MongoDB rules: no $, no null, not empty, no system. prefix; 255-byte ns with $db
Mongo::valid_database_name($name) → { name, valid, reason }   # MongoDB db rules: not empty, <64 chars, no null/space/ /\."$*<>:|?
Mongo::is_valid_objectid($id)        → 1 | ""               # 24-hex, validated via bson
Mongo::new_objectid()                → $hex                 # fresh 24-hex ObjectId
Mongo::objectid_timestamp($id)       → { epoch_seconds, epoch_millis, iso }   # creation time from leading 4 bytes
Mongo::parse_objectid($id)           → { hex, epoch_seconds, iso, random, counter }   # full decomposition: timestamp + 5-byte random + 3-byte counter
Mongo::objectid_from_time(%opts)     → $oid   # {epoch_seconds|epoch_millis|iso} → boundary ObjectId (createFromTime); inverse of objectid_timestamp
```

Unlike a SQL DSN, `parse_connection_string` returns a host **list** (replica
sets) and recognizes `mongodb+srv://` — it parses structure only, never
resolving SRV DNS.

### Plumbing

```stryke
Mongo::version()        → $version_string       # cdylib's CARGO_PKG_VERSION
```

## [0x04] BSON type encoding

The cdylib converts BSON ↔ JSON via MongoDB's **relaxed extended JSON**
format, so non-JSON types round-trip cleanly:

| BSON | JSON |
|---|---|
| `String` | string |
| `Int32`, `Int64` | number |
| `Double` | number |
| `Decimal128` | `{"$numberDecimal": "12.34"}` |
| `Boolean` | bool |
| `Null` | null |
| `Array` | array |
| `Document` | object |
| `ObjectId` | `{"$oid": "65f9b1d2c3f6e9...."}` |
| `DateTime` | `{"$date": "2026-05-17T02:30:00Z"}` |
| `Binary` | `{"$binary": {"base64":"…","subType":"00"}}` |
| `UUID` | wrapped via `$binary` subtype 04 |
| `Regex` | `{"$regularExpression": {"pattern":"^foo","options":"i"}}` |
| `Timestamp` | `{"$timestamp": {"t":..., "i":...}}` |

You can pass extended-JSON wrappers back through filters / updates: a
filter like `{"_id": {"$oid": "65f9…"}}` will be re-parsed to a real
`ObjectId` before hitting the wire.

## [0x05] FFI layer

Each `Mongo::*` wrapper builds a JSON args dict and calls a sibling
`mongo__*` symbol resolved out of `libstryke_mongo.{dylib,so}`. The
cdylib is dlopened in-process on first `use Mongo` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook). Its exports cover
version/ping, discovery, find/count/aggregate, write paths, index admin,
and connection-free helpers (`mongo__parse_connection_string`,
`mongo__build_connection_string`, `mongo__redact_connection_string`,
`mongo__parse_namespace`, `mongo__build_namespace`, `mongo__is_valid_objectid`,
`mongo__new_objectid`, `mongo__objectid_timestamp`, `mongo__parse_objectid`). The authoritative
list is `[ffi].exports` in
`stryke.toml`.

Errors come back as a `{error}` JSON payload; the stryke wrapper dies
with `Mongo::<op>: <reason>`.

<details>
<summary>v1 wire shape (historical helper binary)</summary>

```sh
stryke-mongo-helper find app/users --filter='{"name":"alice"}' --limit=10
stryke-mongo-helper insert-one app/users --doc='{"name":"alice"}'
cat docs.ndjson | stryke-mongo-helper insert-many app/users
stryke-mongo-helper aggregate app/orders \
    --pipeline='[{"$group":{"_id":"$customer","total":{"$sum":"$amount"}}}]'
```

Output:

* `find`, `aggregate`, `list-databases`, `list-collections`, `indexes` → NDJSON
* `find-one` → single JSON doc (or `null`)
* writes / metadata → single JSON summary
* errors → stderr + non-zero exit

</details>

## [0x06] Tests

```sh
cargo test                            # compiles, no live calls
MONGODB_URI=mongodb://localhost s test t/    # live round-trip
```

Tests use a unique `stryke_test_$$` collection name and clean up.

Local test server:

```sh
brew install mongodb-community
mongod --dbpath /tmp/mdb --port 27017 &
```

## [0x07] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x08] Layout

```
stryke-mongo/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/lib.rs                       # single-file cdylib
  lib/
    Mongo.stk                      # `use Mongo`
  t/
    test_mongo.stk                 # live round-trip
    test_stryke_mongo_surface.stk
  examples/
    crud.stk
    aggregate.stk
    index_admin.stk
    counts.stk
    discover.stk
  .github/workflows/
    ci.yml                         # mongo:7 service + live round-trip
    release.yml                    # cross-compile + GH release on tag push
```

## [0x09] Roadmap

Shipped: CRUD + aggregate + index admin, atomic findAndModify (update/replace/
delete), distinct, estimated count, collection create/drop, arbitrary
`run_command`, and `upsert` / `array_filters` write options.

| Open | Later |
|---|---|
| Change Streams (requires replica set) | GridFS read/write |
| Multi-doc transactions (replica set required) | Canonical extended JSON for `$numberLong` precision |
| `bulk_write` (mixed ordered/unordered) | Connection pool / persistent serve daemon |

## [0xFF] License

MIT.
