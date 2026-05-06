---
title: "Release Notes - Flink 2.3"
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

# Release notes - Flink 2.3

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 2.2 and Flink 2.3. Please read these notes carefully if you are
planning to upgrade your Flink version to 2.3.

### Table SQL / API

#### FROM_CHANGELOG and TO_CHANGELOG built-in PTFs

##### [FLINK-39261](https://issues.apache.org/jira/browse/FLINK-39261), [FLINK-39419](https://issues.apache.org/jira/browse/FLINK-39419) (FLIP-564)

Flink 2.3 introduces two new built-in Process Table Functions for converting between append-only
streams that encode change operations and dynamic tables with full changelog semantics:

- `FROM_CHANGELOG` converts an append-only stream that carries an operation column (and optional
  before/after row descriptors) into a dynamic table. A configurable `op_mapping` makes it
  straightforward to plug in custom CDC formats, and `invalid_op_handling` (`FAIL`/`LOG`/`SKIP`)
  controls how rows with unmapped operation codes are treated.
- `TO_CHANGELOG` is the inverse: it materializes a dynamic table back into an append-only
  changelog stream. This is the first SQL-level operator that lets users convert upsert/retract
  streams into append-only form, which is useful for archival, audit and writing to append-only
  sinks. `produces_full_deletes` controls whether `-D` records carry the full row.

Both PTFs support `PARTITION BY` for parallel execution and `uid` for query evolution, and they
expose a `state_ttl` parameter for state retention.

#### CREATE/ALTER for MATERIALIZED TABLE aligned with TABLE

##### [FLINK-39303](https://issues.apache.org/jira/browse/FLINK-39303) (FLIP-550)

The DDL surface of `MATERIALIZED TABLE` is brought to parity with regular tables. `CREATE
MATERIALIZED TABLE` now accepts an explicit column list (including watermarks and primary keys)
in front of the defining `AS` query. `ALTER MATERIALIZED TABLE` gains `ADD`, `MODIFY` and `DROP`
operations on metadata and computed columns, plus `RENAME TO`, allowing materialized tables to
evolve through the same workflow already used for regular Flink tables.

#### Granular control over data reprocessing during materialized table evolution

##### [FLINK-39460](https://issues.apache.org/jira/browse/FLINK-39460) (FLIP-557)

When a materialized table's defining query is changed, Flink would previously always reprocess
historical data from the beginning. Flink 2.3 introduces an optional `START_MODE` clause on
`CREATE [OR ALTER]` and `ALTER MATERIALIZED TABLE`, letting users start the refresh pipeline
`FROM_BEGINNING`, `FROM_NOW[(interval)]`, `FROM_TIMESTAMP(timestamp)`, or resume from previous
offsets when available (`RESUME_OR_FROM_BEGINNING`/`RESUME_OR_FROM_NOW`/`RESUME_OR_FROM_TIMESTAMP`).
The default remains `FROM_BEGINNING` for backward compatibility.

#### ARTIFACT keyword in CREATE FUNCTION

##### [FLINK-39462](https://issues.apache.org/jira/browse/FLINK-39462) (FLIP-559)

The `USING` clause of `CREATE FUNCTION` accepts a new `ARTIFACT` keyword as an alternative to
`JAR`. `ARTIFACT` is intentionally generic so that future ecosystem assets (Python wheels, etc.)
can be referenced through the same syntax. Both keywords are interchangeable and may even be
mixed within a single statement; existing `USING JAR` syntax continues to work unchanged.

```sql
CREATE FUNCTION my_func AS 'com.example.MyUdf'
  USING ARTIFACT 's3://bucket/path/my-udf.jar';
```

#### SinkUpsertMaterializer improvements and changelog disorder handling

##### [FLINK-39461](https://issues.apache.org/jira/browse/FLINK-39461) (FLIP-558)

Flink 2.3 reworks how `SinkUpsertMaterializer` handles the case where a query's upsert key
differs from the sink's primary key. Previously this required maintaining the full history of
records and could blow up state. Two changes address this:

- A new `ON CONFLICT` clause with `DO NOTHING`, `DO ERROR` and `DO DEDUPLICATE` strategies makes
  the behavior on key conflict explicit. By default, planning now fails when the upsert and
  primary keys differ, requiring the user to choose a conflict strategy.
- Watermark-based record compaction is introduced to fix internal changelog disorder. The
  trigger and frequency of compaction are controlled by:
  - `table.exec.sink.upserts.compaction-mode` (default: `WATERMARK`) — `WATERMARK` or
    `CHECKPOINT`.
  - `table.exec.sink.upserts.compaction-interval` — optional fallback interval for emitting
    watermarks when none arrive naturally.

#### Process Table Function enhancements

##### [FLINK-39256](https://issues.apache.org/jira/browse/FLINK-39256), [FLINK-39392](https://issues.apache.org/jira/browse/FLINK-39392), [FLINK-39436](https://issues.apache.org/jira/browse/FLINK-39436), [FLINK-39437](https://issues.apache.org/jira/browse/FLINK-39437), [FLINK-37599](https://issues.apache.org/jira/browse/FLINK-37599) (FLIP-565)

Process Table Functions (PTFs), introduced in Flink 2.1, gain several capabilities aligning them
with the DataStream API:

- **Late data handling**: late records are no longer silently dropped; PTFs can react to them.
- **`ORDER BY` on table arguments**: `MyPtf(input => TABLE t PARTITION BY k ORDER BY ts)` lets a
  PTF receive partitioned rows in deterministic temporal order.
- **`ValueView`**: a new lazy single-value state primitive (`value()`, `update()`, `isEmpty()`,
  `clear()`) joins the existing `MapView` and `ListView` for working with single-element state
  efficiently.
- **Broadcast state**: PTFs support broadcast state through the new
  `@ArgumentHint(ArgumentTrait.BROADCAST_SEMANTIC_TABLE)` and `@StateHint(StateKind.BROADCAST)`
  annotations, plus `@ArgumentHint(ArgumentTrait.NOTIFY_STATEFUL_SETS)` for re-evaluating keys
  when broadcast state changes.
- **Conditional traits and `on_time` column expansion**: planner can use traits that depend on
  argument values, and `on_time` columns participate in `SELECT *` expansion.
- **Interruptible timers**: long timer-driven work yields to checkpoints and other timers.

#### Metadata filter push-down for table sources

##### [FLINK-39421](https://issues.apache.org/jira/browse/FLINK-39421)

Filters that apply only to metadata columns (for example, Kafka headers, partition or offset)
can now be pushed down into the source. This reduces the amount of data read from external
systems for queries that filter primarily on metadata.

#### New built-in functions and string utilities

##### [FLINK-39601](https://issues.apache.org/jira/browse/FLINK-39601), [FLINK-39602](https://issues.apache.org/jira/browse/FLINK-39602)

Flink 2.3 adds the built-in functions `IS_VALID_UTF8` and `MAKE_VALID_UTF8` for validating and
sanitizing string data, along with corresponding `StringData.fromUtf8Bytes` connector APIs.

#### Other SQL improvements

- [FLINK-39253](https://issues.apache.org/jira/browse/FLINK-39253): The `ROW` function now
  preserves field names from `AS` aliases.
- [FLINK-39424](https://issues.apache.org/jira/browse/FLINK-39424): `LIKE` correctly supports
  the default escape character.
- [FLINK-39293](https://issues.apache.org/jira/browse/FLINK-39293): `MATCH_RECOGNIZE` no longer
  fails with `SqlParserException` when used inside views.
- [FLINK-39504](https://issues.apache.org/jira/browse/FLINK-39504): Special characters in
  `VARIANT` `ITEM` calls are handled correctly.
- [FLINK-39420](https://issues.apache.org/jira/browse/FLINK-39420): Temporal joins are rejected
  in batch mode with a clear error message instead of producing incorrect results.
- [FLINK-39458](https://issues.apache.org/jira/browse/FLINK-39458): Table type converters and
  serializers were moved into a new `flink-table-type-utils` module to enable reuse outside the
  table planner.
- [FLINK-39606](https://issues.apache.org/jira/browse/FLINK-39606): `SHOW CREATE MATERIALIZED
  TABLE` exposes flags to control whether `FRESHNESS`/`REFRESH MODE` clauses are included.

### Connectors

#### Flink Native S3 FileSystem

##### [FLINK-39465](https://issues.apache.org/jira/browse/FLINK-39465) (FLIP-555)

Flink 2.3 introduces a new native S3 file system plugin (`flink-s3-fs-native`) implemented
directly on top of the AWS SDK v2, removing the Hadoop and Presto dependencies of the previous
S3 connectors. The unified plugin provides both `FileSystem` and `RecoverableWriter`
implementations (so streaming sinks retain exactly-once semantics), uses non-blocking I/O, and
natively supports modern AWS auth patterns such as IAM Roles for Service Accounts.

The plugin registers the standard `s3://` URI scheme and is deployed via the regular plugins
directory. Configuration uses a new `s3.*` namespace (e.g. `s3.region`, `s3.endpoint`,
`s3.path-style-access`, `s3.access-key`, `s3.secret-key`, `s3.upload.min.part.size`,
`s3.upload.max.concurrent.uploads`, `s3.bulk-copy.enabled`, `s3.async.enabled`,
`s3.read.buffer.size`, `s3.entropy.key`, plus SSE-KMS and chunked-encoding/checksum-validation
controls).

See the [Native S3 FileSystem documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/filesystems/s3/)
for setup details.

### Runtime

#### Adaptive Partition Selection for StreamPartitioner

##### [FLINK-39466](https://issues.apache.org/jira/browse/FLINK-39466) (FLIP-339)

`StreamPartitioner` gains an adaptive, load-aware partition-selection mode that routes records
to the least-loaded downstream channel. This mitigates backpressure caused by skewed downstream
processing and, in benchmarks, delivers up to ~3x throughput improvement under such conditions.
The feature is opt-in via two new options:

- `taskmanager.network.adaptive-partitioner.enabled` (default: `false`)
- `taskmanager.network.adaptive-partitioner.max-traverse-size` (default: `4`) — number of
  channels examined when selecting the idlest target.

#### AdaptiveScheduler rescale history and Web UI

##### [FLINK-39467](https://issues.apache.org/jira/browse/FLINK-39467), [FLINK-39468](https://issues.apache.org/jira/browse/FLINK-39468), [FLINK-38893](https://issues.apache.org/jira/browse/FLINK-38893), [FLINK-38896](https://issues.apache.org/jira/browse/FLINK-38896), [FLINK-38898](https://issues.apache.org/jira/browse/FLINK-38898), [FLINK-38900](https://issues.apache.org/jira/browse/FLINK-38900), [FLINK-38902](https://issues.apache.org/jira/browse/FLINK-38902) (FLIP-487, FLIP-495)

Streaming jobs running with the adaptive scheduler now record a history of rescale events,
including job-vertex parallelisms, slot allocations, scheduler-state transitions and
termination reasons. Events are kept in memory and on disk following the existing
`ExecutionGraphInfoStore` pattern.

A new "Rescales" tab in the Web UI exposes this information through subpages for overview,
history, summary and per-event details. The same data is available through new REST endpoints:

- `/jobs/:jobid/rescales/overview`
- `/jobs/:jobid/rescales/history`
- `/jobs/:jobid/rescales/details/:rescaleuuid`
- `/jobs/:jobid/rescales/summary`

The existing `/jobs/overview` endpoint is extended with `schedulerType` and `jobType` fields so
the UI can render adaptive-scheduler-specific information.

The feature is controlled by a new option:

- `web.adaptive-scheduler.rescale-history.size` (default: `0`) — maximum number of rescale
  records retained per job. Setting `0` disables the feature.

See the [Elastic Scaling documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/elastic_scaling/)
for details.

#### Checkpointing during recovery

##### [FLINK-39469](https://issues.apache.org/jira/browse/FLINK-39469), [FLINK-38541](https://issues.apache.org/jira/browse/FLINK-38541) (FLIP-547)

Recovering from large unaligned checkpoints can stall a job for a long time, blocking upstream
systems and increasing the cost of subsequent failures. Flink 2.3 enables checkpointing during
the recovery phase: filtered records are re-organized into new buffers, output buffers are
restored on the downstream task, and the task lifecycle allows the checkpoint coordinator to
trigger checkpoints earlier. Two opt-in options are introduced:

- `execution.checkpointing.unaligned.recover-output-on-downstream.enabled` (default: `false`)
- `execution.checkpointing.unaligned.during-recovery.enabled` (default: `false`; requires the
  option above to be enabled).

#### Application Management

##### [FLINK-39470](https://issues.apache.org/jira/browse/FLINK-39470), [FLINK-39264](https://issues.apache.org/jira/browse/FLINK-39264) (FLIP-549)

Flink 2.3 introduces a first-class **application** concept that sits above jobs and unifies the
behavior of user code across deployment modes. The cluster-job model is replaced by a
cluster-application-job hierarchy, with two backing implementations
(`PackagedProgramApplication` and `SingleJobApplication`). Application archives are organized
by cluster and application IDs.

New REST APIs:

- `GET /applications/overview` — list applications.
- `GET /applications/:applicationid` — application details.
- `POST /applications/:applicationid/cancel` — cancel an application.
- `POST /jars/:jarid/run-application` — submit an application asynchronously.

New configuration options:

- `execution.terminate-application-on-any-job-terminated-exceptionally` (default: `true`).
- `cluster.id` (default: all-zero UUID).
- `historyserver.archive.clean-expired-applications` (default: `false`).
- `historyserver.archive.retained-applications` (default: `-1`).

The Web UI gains an Applications tab and a redesigned home page; jobs link back to the
application that owns them. See the
[Application Lifecycle documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/application_lifecycle/)
for details.

#### Application Capability Enhancement

##### [FLINK-39473](https://issues.apache.org/jira/browse/FLINK-39473) (FLIP-560)

Building on FLIP-549, FLIP-560 hardens the application layer for high availability and improves
error visibility:

- Job-name-based matching enables correct recovery of multi-job applications after a JobManager
  failure; job recovery is deferred until the user `main` method reaches the corresponding
  submission point.
- Applications can execute zero or multiple jobs (with some limitations for streaming jobs and
  certain APIs).
- A new `ApplicationStore` and `ApplicationResultStore` persist application metadata and
  cleanup state across HA recovery.
- Application-level exceptions (errors thrown by user `main` code, not just job execution) are
  exposed through a new REST endpoint and Web UI subpage.

New REST APIs:

- `GET /applications/:applicationid/exceptions`
- `GET /applications/:applicationid/jobmanager/config`

New configuration options:

- `application-result-store.storage-path`
- `application-result-store.delete-on-commit` (default: `true`)

The application page in the Web UI now shows start/end times and duration, a cancel action,
log links, and a new exceptions subpage.

#### Robust OTel gRPC metric exporter

##### [FLINK-39471](https://issues.apache.org/jira/browse/FLINK-39471), [FLINK-39127](https://issues.apache.org/jira/browse/FLINK-39127) (FLIP-553)

Large Flink jobs can produce metric payloads that exceed the size limits of common OTel gRPC
backends, causing data loss. Flink 2.3 adds three opt-in robustness features to the OTel
exporter (all backward compatible):

- `metrics.reporter.otel.exporter.compression` — `gzip` or `none` (default).
- `metrics.reporter.otel.batch.size` — split a single export into multiple gRPC calls; default
  `0` (disabled).
- `metrics.reporter.otel.transform.attribute-value-length-limits.<attribute_name>` — per-attribute
  truncation; `*` applies a global default.

#### Watermark alignment improvements

##### [FLINK-39474](https://issues.apache.org/jira/browse/FLINK-39474) ([FLINK-37399](https://issues.apache.org/jira/browse/FLINK-37399))

Watermark alignment now makes better use of source resources, avoiding unnecessary stalls when
one source partition is far ahead of others.

#### MiniBatchGroupAggFunction correctness fix

##### [FLINK-39475](https://issues.apache.org/jira/browse/FLINK-39475) ([FLINK-35661](https://issues.apache.org/jira/browse/FLINK-35661))

A correctness bug in `MiniBatchGroupAggFunction` that could produce incorrect aggregation
results in certain mini-batch scenarios has been fixed. Users running streaming aggregations
with mini-batch enabled are encouraged to upgrade.

### Python

#### Python Table API: fromChangelog / toChangelog / descriptor

##### [FLINK-39479](https://issues.apache.org/jira/browse/FLINK-39479), [FLINK-39442](https://issues.apache.org/jira/browse/FLINK-39442)

The Python Table API gains parity with the Java Table API for the new changelog PTFs introduced
in this release. `fromChangelog()`, `toChangelog()` and `descriptor()` are now available from
PyFlink, allowing Python users to express the same changelog conversions as Java/SQL users.

### Documentation

#### Documentation restructure

##### [FLINK-39476](https://issues.apache.org/jira/browse/FLINK-39476) (FLIP-561)

The Flink documentation has been reorganized to make navigation easier. Highlights:

- Flink SQL gets a dedicated top-level section, separated from the Table API.
- Relational streaming concepts (changelogs, dynamic tables, state, etc.) are promoted to a
  top-level Concepts section.
- Python documentation is integrated into the relevant API sections instead of living in a
  standalone area.
- Contributor-facing content has been relocated outside the main user-facing docs.

Existing URLs continue to work via redirects. The top-level structure is documented on the
[docs landing page](https://nightlies.apache.org/flink/flink-docs-release-2.3/).

### Core / Security

#### Set security.ssl.algorithms default value to modern cipher suite

##### [FLINK-39022](https://issues.apache.org/jira/browse/FLINK-39022)

A JDK update (affecting JDK 11.0.30+, 17.0.18+, 21.0.10+, and 24+) disabled `TLS_RSA_*` cipher suites.
This was done to support forward-secrecy (RFC 9325) and comply with the IETF Draft on *Deprecating Obsolete Key Exchange Methods in TLS*.

To support these and future JDK versions, the default value for the Flink configuration option `security.ssl.algorithms` has been changed to a modern, widely available cipher suite:

`TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`

This default provides strong security and wide compatibility. You can customize the cipher suites using the `security.ssl.algorithms` configuration option if your environment has different requirements.
If these cipher suites are not supported on your setup, you will see that Flink processes will not be able to connect to each other.

#### Sensitive key redaction extended

##### [FLINK-39561](https://issues.apache.org/jira/browse/FLINK-39561)

The set of configuration keys whose values are redacted from logs and the Web UI has been
extended to cover additional access-key patterns. Users can also contribute their own
additional patterns through configuration.

### Dependency upgrades

- [FLINK-39580](https://issues.apache.org/jira/browse/FLINK-39580): Bump Flink-controlled Java
  dependencies (log4j, jackson, assertj, netty).
- [FLINK-39542](https://issues.apache.org/jira/browse/FLINK-39542): Upgrade Avro to 1.11.5.
- [FLINK-39528](https://issues.apache.org/jira/browse/FLINK-39528): Update `apache-arrow` to
  19.0.0.
- [FLINK-39598](https://issues.apache.org/jira/browse/FLINK-39598),
  [FLINK-31070](https://issues.apache.org/jira/browse/FLINK-31070): Bump `jline` to 3.30.12.
- [FLINK-39534](https://issues.apache.org/jira/browse/FLINK-39534): Bump `pemja` to 0.5.7.
- [FLINK-39427](https://issues.apache.org/jira/browse/FLINK-39427): Use Apache Parent POM 35.
- [FLINK-39483](https://issues.apache.org/jira/browse/FLINK-39483): Bump dependency-check Maven
  plugin to 12.2.1.
