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
- [\[0x05\] Helper protocol](#0x05-helper-protocol)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] Dev workflow](#0x07-dev-workflow)
- [\[0x08\] Layout](#0x08-layout)
- [\[0x09\] Roadmap](#0x09-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Install

```sh
cd ~/projects/stryke-mongo
cargo build --release
s pkg install -g .
```

Or:

```sh
make install
```

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

# Streaming variant — callback per doc, no buffering on the stryke side.
Mongo::find_stream "app/events",
    filter   => { ts => { '$gte' => $cutoff } },
    callback => sub ($d) { process $d }

# Updates.
Mongo::update_one "app/users",
                  { name => "alice" },
                  { '$set' => { role => "superadmin" } }
Mongo::update_many "app/users", { active => 1 }, { '$inc' => { score => 1 } }

# Replace whole document.
Mongo::replace_one "app/users", { _id => $oid }, { ...new doc... }, upsert => 1

# Aggregation pipeline (any standard stage).
my @top = Mongo::aggregate "app/orders", [
    { '$match' => { status => "paid" } },
    { '$group' => { _id => '$customer', total => { '$sum' => '$amount' } } },
    { '$sort'  => { total => -1 } },
    { '$limit' => 10 },
]

# Index admin.
Mongo::create_index "app/users", { email => 1 }, unique => 1, name => "uniq_email"
Mongo::indexes "app/users" |> ep
Mongo::drop_index "app/users", "uniq_email"

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

### Write paths

```stryke
Mongo::insert_one   $target, \%doc, %opts → { inserted_id }
Mongo::insert_many  $target, \@docs, %opts → { inserted, ids }
Mongo::update_one   $target, \%filter, \%update, %opts → { matched, modified, upserted_id }
Mongo::update_many  $target, \%filter, \%update, %opts → { matched, modified }
Mongo::replace_one  $target, \%filter, \%doc, %opts → { matched, modified, upserted_id }
Mongo::delete_one   $target, \%filter, %opts → { deleted }
Mongo::delete_many  $target, \%filter, %opts → { deleted }
```

### Metadata + indexes

```stryke
Mongo::list_databases    %opts → @{ {name, size_on_disk, empty} }
Mongo::list_collections  $db, %opts → @names
Mongo::create_index      $target, \%keys, %opts → { name }    # opts: unique, name
Mongo::drop_index        $target, $name, %opts → { name, dropped }
Mongo::indexes           $target, %opts → @{ {name, keys, unique} }
Mongo::ping              %opts → 1 | ""
```

### Helper plumbing

```stryke
Mongo::helper_path()    → $abs_path
Mongo::ensure_built()   → $abs_path
Mongo::version()        → "stryke-mongo-helper 0.1.0"
```

## [0x04] BSON type encoding

The helper converts BSON ↔ JSON via MongoDB's **relaxed extended JSON**
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

## [0x05] Helper protocol

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

## [0x06] Tests

```sh
cargo test                            # compiles, no live calls
MONGODB_URI=mongodb://localhost s test t/    # 9-test live round-trip
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
  src/main.rs                      # single-file helper, ~500 LOC
  lib/
    Mongo.stk                      # `use Mongo`
  bin/
    mongo.stk                      # `mongo` CLI
    mongo-build.stk
  t/
    test_mongo.stk                 # 9-test live round-trip
  examples/
    crud.stk
    aggregate.stk
    index_admin.stk
  .github/workflows/
    ci.yml                         # mongo:7 service + 9-test round-trip
    release.yml                    # cross-compile + GH release on tag push
```

## [0x09] Roadmap

| v1 (this release) | v2+ |
|---|---|
| Single-shot CRUD + aggregate + index admin | Change Streams (requires replica set) |
| Connection per call | Connection pool / persistent serve daemon |
| Relaxed extended JSON | Canonical extended JSON option for `$numberLong` precision |
| BSON filters / updates / pipelines | GridFS read/write |
| `mongodb` 3.x async | Transactions (replica set required) |

## [0xFF] License

MIT.
