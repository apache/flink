---
title: "Release Notes - Flink 2.4"
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Release notes - Flink 2.4

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 2.3 and Flink 2.4. Please read these notes carefully if you are
planning to upgrade your Flink version to 2.4.

### Dependencies and configuration

#### Upgrade SLF4J to 2.0.20

##### [FLINK-40574](https://issues.apache.org/jira/browse/FLINK-40574)

Flink 2.4 upgrades SLF4J from 1.7.36 to 2.0.20. Flink's default distribution uses the matching
SLF4J 2.x Log4j binding. Applications that provide another logging backend must use an SLF4J
2.x-compatible provider. In particular, Logback 1.2 is not compatible with SLF4J 2.x.

Before upgrading, remove bundled copies of `slf4j-api 1.x` from application JARs and update custom
logging providers and bridges to versions compatible with SLF4J 2.x. Mixing SLF4J 1.x and 2.x
artifacts can prevent logging providers from being discovered and result in missing log output.

#### Removed deprecated configuration keys

##### [FLINK-40734](https://issues.apache.org/jira/browse/FLINK-40734)

Related subtasks: [FLINK-40813](https://issues.apache.org/jira/browse/FLINK-40813),
[FLINK-40815](https://issues.apache.org/jira/browse/FLINK-40815),
[FLINK-40816](https://issues.apache.org/jira/browse/FLINK-40816),
[FLINK-40817](https://issues.apache.org/jira/browse/FLINK-40817).

Flink 2.4 no longer recognizes the deprecated configuration keys listed below. Replace them with
their current keys before upgrading. If only an old key is configured, it is ignored and the
replacement option remains unset or uses its default value.

| Removed key | Replacement key |
| --- | --- |
| `akka.client.timeout` | `client.timeout` |
| `dstl.dfs.base-path` | `state.changelog.dstl.dfs.base-path` |
| `dstl.dfs.compression.enabled` | `state.changelog.dstl.dfs.compression.enabled` |
| `dstl.dfs.preemptive-persist-threshold` | `state.changelog.dstl.dfs.preemptive-persist-threshold` |
| `dstl.dfs.batch.persist-delay` | `state.changelog.dstl.dfs.batch.persist-delay` |
| `dstl.dfs.batch.persist-size-threshold` | `state.changelog.dstl.dfs.batch.persist-size-threshold` |
| `dstl.dfs.upload.buffer-size` | `state.changelog.dstl.dfs.upload.buffer-size` |
| `dstl.dfs.upload.num-threads` | `state.changelog.dstl.dfs.upload.num-threads` |
| `dstl.dfs.discard.num-threads` | `state.changelog.dstl.dfs.discard.num-threads` |
| `dstl.dfs.upload.max-in-flight` | `state.changelog.dstl.dfs.upload.max-in-flight` |
| `dstl.dfs.upload.retry-policy` | `state.changelog.dstl.dfs.upload.retry-policy` |
| `dstl.dfs.upload.timeout` | `state.changelog.dstl.dfs.upload.timeout` |
| `dstl.dfs.upload.max-attempts` | `state.changelog.dstl.dfs.upload.max-attempts` |
| `dstl.dfs.upload.next-attempt-delay` | `state.changelog.dstl.dfs.upload.next-attempt-delay` |
| `dstl.dfs.download.local-cache.idle-timeout-ms` | `state.changelog.dstl.dfs.download.local-cache.idle-timeout-ms` |
| `kubernetes.jobmanager.cpu` | `kubernetes.jobmanager.cpu.amount` |
| `kubernetes.taskmanager.cpu` | `kubernetes.taskmanager.cpu.amount` |
| `kubernetes.container.image` | `kubernetes.container.image.ref` |
| `kubernetes.pod-template-file` | `kubernetes.pod-template-file.default` |
| `yarn.per-job-cluster.include-user-jar` | `yarn.classpath.include-user-jar` |
| `yarn.heartbeat-delay` | `yarn.heartbeat.interval` |
| `yarn.ship-directories` | `yarn.ship-files` |

### Table SQL / API

#### Upgrade Calcite to 1.41.0

##### [FLINK-39859](https://issues.apache.org/jira/browse/FLINK-39859)

Flink 2.4 upgrades Apache Calcite, the foundation of the SQL parser and optimizer, to version
1.41.0. This incorporates the fixes and improvements from the intervening Calcite releases.
Applications that depend directly on Calcite classes or extend Flink's planner should check
their dependencies and custom planner code against the upgraded version.

#### Common subexpression elimination in Calc code generation

##### [FLINK-39268](https://issues.apache.org/jira/browse/FLINK-39268)

Flink 2.4 avoids redundant evaluation of common subexpressions within a Calc operator, which
combines filtering and projection. Repeated deterministic expressions can reuse an already
computed result instead of being evaluated again for the same input row. This applies to both
batch and streaming queries and can reduce the cost of repeated built-in or user-defined
function calls.

Reuse respects conditional evaluation, such as short-circuiting in `AND`, `OR`, and `CASE`.
UDF authors should ensure that `isDeterministic()` correctly describes their function:
functions with side effects or results that vary for identical inputs must return `false`.

#### Built-in UDF metrics

##### [FLINK-38071](https://issues.apache.org/jira/browse/FLINK-38071) (FLIP-485)

SQL and Table API users can now monitor UDF execution through two built-in metrics:
`udfProcessingTime`, a histogram of sampled invocation durations, and `udfExceptionCount`,
a counter of exceptions from the function. Metrics are registered on the executing operator
under `udf.<udf_name>`. For asynchronous functions, processing time includes the time from
dispatch until completion.

The feature is opt-in:

- `table.exec.udf-metric-enabled` (default: `false`) enables metric collection.
- `table.exec.udf-metric.sample-interval` (default: `100`) controls how often invocation
  durations are measured. Exceptions are counted on every invocation.

The metrics cover directly invoked synchronous and asynchronous scalar and table functions.
Python functions, aggregate functions, and Process Table Functions are not covered. Enabling
the metrics adds overhead; use sampling to control the cost.

#### Early firing for interval joins

##### [FLINK-36953](https://issues.apache.org/jira/browse/FLINK-36953) (FLIP-497)

Outer interval joins can emit unmatched rows before their join interval closes. The new
`EARLY_FIRE` hint emits a speculative, null-padded result after a configured delay. If a
matching row arrives later within the interval, Flink corrects the speculative result with
update-before and update-after records.

```sql
SELECT /*+ EARLY_FIRE('delay'='5s') */ o.id, s.ship_time
FROM Orders o
LEFT OUTER JOIN Shipments s
ON o.id = s.order_id
AND o.order_time BETWEEN s.ship_time - INTERVAL '1' HOUR AND s.ship_time;
```

The optional `time-mode` selects `rowtime` or `proctime`; it defaults to the time mode of the
interval join. Each unmatched outer row fires at most once. The feature is disabled unless
the hint is present and requires downstream operators and sinks that accept updates.
Existing interval joins without the hint retain their behavior.

#### FROM_CHANGELOG and TO_CHANGELOG support all changelog modes

##### [FLINK-39258](https://issues.apache.org/jira/browse/FLINK-39258) (FLIP-564)

Flink 2.3 shipped a first, minimal version of `TO_CHANGELOG` and `FROM_CHANGELOG` that only
supported retract mode: `FROM_CHANGELOG` required separate `UPDATE_BEFORE` and `UPDATE_AFTER`
events, and `TO_CHANGELOG` on an upsert input always added a stateful `ChangelogNormalize`
operator to the job. Flink 2.4 supports all changelog modes:

- `TO_CHANGELOG` skips `ChangelogNormalize` for upsert inputs when the `op_mapping` omits
  `UPDATE_BEFORE` and either omits `DELETE` or sets `produces_full_deletes => false`. With
  `produces_full_deletes => false`, DELETE rows keep only the key columns. An `op_mapping`
  with only `INSERT` and `UPDATE_AFTER` turns an upsert input into an append-only stream that
  operators accepting only append-only input, such as window deduplication, can consume.
- `FROM_CHANGELOG` produces an upsert table when the input uses `PARTITION BY <key>` and the
  `op_mapping` contains `UPDATE_AFTER` but not `UPDATE_BEFORE`. The partition key becomes the
  upsert key.
- Both functions accept `PARTITION BY`, also through `fromChangelog()` and `toChangelog()` on
  `PartitionedTable`. Partitioned `FROM_CHANGELOG` calls can use `ORDER BY` on a watermarked
  time attribute to reorder events per key; late events are dropped.
- `error_handling => 'FAIL' | 'SKIP'` controls how `FROM_CHANGELOG` handles NULL or unmapped
  operation codes. The default is `FAIL`.

`FROM_CHANGELOG` trusts the operation codes it receives: a wrong `op_mapping` or out-of-order
events silently produce a wrong table. See the
[Changelog Conversion documentation](https://nightlies.apache.org/flink/flink-docs-release-2.4/docs/sql/reference/queries/changelog/)
for supported mappings and input requirements.

#### Process Table Function state access and broadcast support

##### [FLINK-39254](https://issues.apache.org/jira/browse/FLINK-39254) (FLIP-565)

Flink 2.4 adds lazy value-state access and broadcast inputs to Process Table Functions:

- `ValueView` provides lazy value-state access alongside `ListView` and `MapView`. A function
  can read or update state only when needed, avoiding unnecessary serialization work.
- `BROADCAST_SEMANTIC_TABLE` lets a PTF receive a rule or control table on every parallel
  instance. State declared with `@StateHint(StateKind.BROADCAST)` is writable from the
  broadcast input and readable while processing the regular inputs.
- `NOTIFY_STATEFUL_SETS` lets a broadcast update trigger re-evaluation of existing stateful
  partitions. This is opt-in because visiting all stateful partitions can be expensive.

#### Process Table Function test harness

##### [FLINK-39357](https://issues.apache.org/jira/browse/FLINK-39357) (FLIP-567)

The new `ProcessTableFunctionTestHarness` lets developers test PTF business logic without
starting a Flink cluster. Tests can configure scalar and table arguments, partitioning, and
initial state, feed rows into individual inputs, and inspect the resulting output.

The harness also supports advancing watermarks, inspecting timers, and accessing state per key.
This makes temporal and stateful behavior easier to test deterministically. Full planner and
runtime integration should still be covered by integration tests.

#### Strict BYTES-to-STRING casts and UTF-8 utilities

##### [FLINK-39600](https://issues.apache.org/jira/browse/FLINK-39600) (FLIP-568)

Casting binary data to a string now validates UTF-8. Previously, malformed byte sequences
were silently replaced with the Unicode replacement character `U+FFFD`; in Flink 2.4,
`CAST(bytes AS STRING)` fails on invalid UTF-8 and `TRY_CAST(bytes AS STRING)` returns NULL.
This is a behavior change for pipelines that previously accepted malformed input.

Two new functions provide explicit handling:

- `IS_VALID_UTF8(bytes)` returns whether the input is valid UTF-8, for filtering or routing
  malformed records.
- `MAKE_VALID_UTF8(bytes)` decodes the input and replaces invalid sequences with `U+FFFD`,
  preserving the previous substitution behavior.

Both functions return NULL for NULL input. As a migration option,
`table.exec.legacy-bytes-to-string-cast` (default: `false`) can be set to `true` to restore
the previous cast behavior.

Connector authors can use the new `StringData.fromUtf8Bytes(byte[])` method for strict
validation at ingestion. The existing `StringData.fromBytes(byte[])` remains unchanged.

#### Metadata filter push-down for table sources

##### [FLINK-39421](https://issues.apache.org/jira/browse/FLINK-39421) (FLIP-574)

Source connectors can now accept filter predicates on metadata columns, such as offsets,
partitions, or timestamps. This lets a supporting connector reduce the data it reads instead
of applying all such filters after ingestion.

Connectors opt in through the new `supportsMetadataFilterPushDown()` and
`applyMetadataFilters()` methods on `SupportsReadingMetadata`. Predicates use metadata keys
rather than SQL column aliases, and filters that the connector cannot fully apply remain in
the query. Metadata predicates are also preserved when compiled plans are restored.
Existing connectors retain their behavior unless they implement the new capability.

#### In-place conversion of tables to materialized tables

##### [FLINK-39847](https://issues.apache.org/jira/browse/FLINK-39847) (FLIP-578)

`CREATE OR ALTER MATERIALIZED TABLE` can convert an existing regular table into a materialized
table without dropping and recreating the catalog object. The conversion preserves the table's
identity and underlying storage and starts a refresh job for the defining query.

Conversion is disabled by default. Add the following option to `config.yaml` before starting the
session:

```yaml
table.materialized-table.conversion-from-table.enabled: true
```

This option is read when the session starts; changing it later with `SET` has no effect.

The catalog must support the new `convertTableToMaterializedTable` operation. The conversion
statement defines the resulting schema and options; existing primary keys, watermarks, and
other definitions must be declared again where needed. This is a one-way conversion: there is
no corresponding operation to turn a materialized table back into a regular table.

#### LATERAL SNAPSHOT joins

##### [FLINK-39779](https://issues.apache.org/jira/browse/FLINK-39779) (FLIP-579)

Flink SQL gains `LATERAL SNAPSHOT` joins for enriching an append-only stream with the current
contents of an updating table. Unlike an event-time temporal join, the join does not wait for
both inputs' watermarks for every result. It first buffers probe-side rows while initializing
the build side, then joins arriving rows immediately against the current build-side state.

`INNER` and `LEFT OUTER` joins are supported, and the build side does not need a primary key.
Streaming queries specify a watermarked build-side column through `on_time`.
`load_completed_time` controls when initialization completes; an optional
`load_completed_idle_timeout` provides a fallback when build-side watermarks stop advancing.
For static or infrequently updated build sides, configure this timeout or use a bounded source
so the join does not remain in its initialization phase.

Results follow processing-time semantics and can differ after recovery. Users requiring an
exact event-time version should continue to use event-time temporal joins.
See the [LATERAL SNAPSHOT join documentation](https://nightlies.apache.org/flink/flink-docs-release-2.4/docs/sql/reference/queries/joins/#lateral-snapshot-join)
for the full syntax and initialization behavior.

#### UUID type and extended VARIANT primitives

##### [FLINK-40485](https://issues.apache.org/jira/browse/FLINK-40485) (FLIP-604)

Flink 2.4 introduces a native `UUID` type backed by a 16-byte value, with explicit casts to
and from strings and binary data, comparison and ordering support, and typed `UUID_V4()` and
`UUID_V7()` generators. The existing `UUID()` function continues to return a string.

The `VARIANT` type also supports time-of-day values, nanosecond-precision timestamps with and
without local time zone, and UUID values. This extends compatibility with the corresponding
Parquet Variant primitive encodings. Nanosecond timestamps can be read as `TIMESTAMP(9)` or
`TIMESTAMP_LTZ(9)`; converting a VARIANT time-of-day value to SQL `TIME(3)` truncates any
sub-millisecond precision.

Existing VARIANT values remain readable. Older Flink versions cannot read values containing
the newly supported primitive codes, so consider this limitation before downgrading a job or
sharing persisted VARIANT data with older consumers.

### Python

#### Python DataFrame API

##### [FLINK-40185](https://issues.apache.org/jira/browse/FLINK-40185) (FLIP-591)

PyFlink gains a DataFrame API under `pyflink.dataframe`. It offers a Python-oriented entry
point for creating Flink tables from records, dictionaries, pandas DataFrames, or Arrow tables,
and for reading and writing through Flink connectors.

The API supports familiar transformations such as selection, filtering, joins, aggregation,
windowing, and missing-value handling, as well as Python UDFs and batch transformations.
`from_table()` and `to_table()` allow applications to move between DataFrames and the existing
Table API.

DataFrame operations use Flink's existing planner and runtime for batch and streaming
execution. The API complements the Table API; it does not replace it or promise full
compatibility with pandas or other DataFrame libraries.

### Connectors

#### Job information on source contexts

##### [FLINK-39776](https://issues.apache.org/jira/browse/FLINK-39776) (FLIP-583)

`SourceReaderContext` and `SplitEnumeratorContext` now expose `getJobInfo()`, giving source
implementations direct access to the job ID and job name. This aligns the Source API with
Sink V2 and supports use cases such as per-job authentication, external offset tracking,
and log correlation without parsing metric scope variables.

The new methods have default implementations, so existing context implementations remain
source- and binary-compatible. Flink's runtime contexts provide the job information;
third-party contexts and test stubs must override the methods if callers use them, as the
default implementation throws `UnsupportedOperationException`.

### Runtime

#### Filtering and spilling recovered channel state

##### [FLINK-38544](https://issues.apache.org/jira/browse/FLINK-38544) (FLIP-547 V2)

Checkpointing during recovery (introduced in Flink 2.3) lets a job take checkpoints while it is
still replaying in-flight data from unaligned checkpoints, so a failure or rescaling during a
long recovery no longer has to restart it from scratch. In Flink 2.3, recovered data fell back
to unbounded heap memory when network buffers ran out. Flink 2.4 spills it to local disk instead
and feeds it back to the task as buffers become available, so recovery memory is bounded and
checkpoints can still start early. A checkpoint taken during recovery captures both the data
queued in memory and the data still on disk, exactly once. The feature remains opt-in: enable
unaligned checkpoints and set `execution.checkpointing.unaligned.recover-output-on-downstream.enabled`
and `execution.checkpointing.during-recovery.enabled` to `true`.

#### Pluggable HistoryServer archive storage

##### [FLINK-39911](https://issues.apache.org/jira/browse/FLINK-39911) (FLIP-584)

The HistoryServer can store its local archive cache in an embedded RocksDB database instead
of expanding every archive into many small JSON files. This reduces inode consumption when
serving large numbers of jobs or jobs with extensive task-level details.

Select the backend with `historyserver.archive.storage.type`:

- `FILE` (default) keeps the existing file-based storage behavior.
- `ROCKSDB` stores archive entries in an embedded database under
  `historyserver.web.tmpdir/rocksdb`.

This changes how the local cache is stored; completed-job archives continue to be read from
the configured archive directories.

#### State Catalog

##### [FLINK-40175](https://issues.apache.org/jira/browse/FLINK-40175) (FLIP-599)

The new read-only State Catalog discovers checkpoints and savepoints in configured
directories and exposes their contents through Flink SQL. Each discovered snapshot becomes
a database, with tables for operator state and views for inspecting snapshot metadata.
This removes much of the manual schema definition previously needed to query saved state.

The catalog infers schemas from supported serializer snapshots and exposes keyed state,
flattened list and map state, non-keyed list/union/broadcast state, and window state.
Supported POJO and Avro state can be read without the original user classes on the classpath;
custom serialization formats still require compatible type information and reader support.

Register the catalog using one or more labeled directories:

```sql
CREATE CATALOG saved_state WITH (
  'type' = 'state',
  'directory.prod' = 's3://my-bucket/flink/checkpoints'
);
```

The catalog is provided by the `org.apache.flink:flink-state-processor-api` Maven artifact. The
Flink distribution includes its JAR in the `${FLINK_HOME}/opt/` directory, which is not on the classpath by
default. Copy the JAR to `${FLINK_HOME}/lib/` or otherwise add it to the Flink classpath before starting Flink.
Snapshot metadata views additionally require the state module to be loaded with `LOAD MODULE state`.
Catalog write operations are unsupported; querying the catalog does not modify the source
checkpoints or savepoints.
