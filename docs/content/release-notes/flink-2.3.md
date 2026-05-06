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

##### [FLINK-39258](https://issues.apache.org/jira/browse/FLINK-39258) (FLIP-564)

The DataStream API has long offered `toChangelogStream()` and `fromChangelogStream()` for working
with changelog streams; Flink 2.3 brings equivalent functionality to SQL via two new built-in
Process Table Functions:

- `FROM_CHANGELOG` converts an append-only stream that carries an operation column (and optional
  before/after row descriptors) into a dynamic table. A configurable `op_mapping` makes it
  straightforward to plug in custom CDC formats (e.g. Debezium-style `c`/`u`/`d` codes), and
  `invalid_op_handling` (`FAIL`/`LOG`/`SKIP`) controls how rows with unmapped operation codes
  are treated.
- `TO_CHANGELOG` is the inverse: it materializes a dynamic table back into an append-only
  changelog stream. This is the first SQL-level operator that lets users convert
  retract/upsert streams into append form, which is useful for archival, audit and writing to
  append-only sinks. `produces_full_deletes` controls whether `-D` records carry the full row.

The two PTFs are designed to be symmetric, so `FROM_CHANGELOG(TO_CHANGELOG(table))` round-trips
correctly. Both support `PARTITION BY` for parallel execution and `uid` for query evolution, and
they expose a `state_ttl` parameter for state retention.

#### CREATE/ALTER for MATERIALIZED TABLE aligned with TABLE

##### [FLINK-38673](https://issues.apache.org/jira/browse/FLINK-38673) (FLIP-550)

The DDL surface of `MATERIALIZED TABLE` is brought to parity with regular tables. `CREATE
MATERIALIZED TABLE` now accepts an explicit column list (including watermarks and primary keys)
in front of the defining `AS` query. `ALTER MATERIALIZED TABLE` gains `ADD`, `MODIFY` and `DROP`
operations on metadata and computed columns, plus `RENAME TO`, allowing materialized tables to
evolve through the same workflow already used for regular Flink tables.

#### Granular control over data reprocessing during materialized table evolution

##### [FLINK-39301](https://issues.apache.org/jira/browse/FLINK-39301) (FLIP-557)

When a materialized table's defining query is changed, Flink would previously always reprocess
historical data from the beginning. Flink 2.3 introduces an optional `START_MODE` clause on
`CREATE [OR ALTER]` and `ALTER MATERIALIZED TABLE`, letting users start the refresh pipeline
`FROM_BEGINNING`, `FROM_NOW[(interval)]`, `FROM_TIMESTAMP(timestamp)`, or resume from previous
offsets when available (`RESUME_OR_FROM_BEGINNING`/`RESUME_OR_FROM_NOW`/`RESUME_OR_FROM_TIMESTAMP`).
The default remains `FROM_BEGINNING` for backward compatibility.

#### ARTIFACT keyword in CREATE FUNCTION

##### [FLINK-39081](https://issues.apache.org/jira/browse/FLINK-39081) (FLIP-559)

The `USING` clause of `CREATE FUNCTION` accepts a new `ARTIFACT` keyword as an alternative to
`JAR`. `ARTIFACT` is intentionally generic so that future ecosystem assets (Python wheels, etc.)
can be referenced through the same syntax. Both keywords are interchangeable and may even be
mixed within a single statement; existing `USING JAR` syntax continues to work unchanged.

```sql
CREATE FUNCTION my_func AS 'com.example.MyUdf'
  USING ARTIFACT 's3://bucket/path/my-udf.jar';
```

#### SinkUpsertMaterializer improvements and changelog disorder handling

##### [FLINK-38926](https://issues.apache.org/jira/browse/FLINK-38926) (FLIP-558)

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

##### [FLINK-39254](https://issues.apache.org/jira/browse/FLINK-39254) (FLIP-565)

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

### Connectors

#### Flink Native S3 FileSystem

##### [FLINK-38592](https://issues.apache.org/jira/browse/FLINK-38592) (FLIP-555)

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

##### [FLINK-31655](https://issues.apache.org/jira/browse/FLINK-31655) (FLIP-339)

When upstream and downstream parallelism differ, Flink uses `RebalancePartitioner`, which selects
target channels round-robin. For jobs that interact with external RPC services (Redis, HBase,
LLM serving, etc.) round-robin selection causes severe backpressure as soon as a single
downstream subtask slows down — the partitioner keeps feeding it new data even though it is
already overloaded. Flink 2.3 adds an adaptive, load-aware partition-selection mode for
`StreamPartitioner` that routes records to the least-loaded downstream channel instead. In
benchmarks, this delivers up to ~3x throughput improvement under skewed downstream processing.
The feature is opt-in via two new options:

- `taskmanager.network.adaptive-partitioner.enabled` (default: `false`)
- `taskmanager.network.adaptive-partitioner.max-traverse-size` (default: `4`) — number of
  channels examined when selecting the idlest target.

#### AdaptiveScheduler rescale history

##### [FLINK-38333](https://issues.apache.org/jira/browse/FLINK-38333) (FLIP-495)

Streaming jobs running with the adaptive scheduler now record a history of rescale events,
including job-vertex parallelisms, slot allocations, scheduler-state transitions and termination
reasons. Events are kept in memory and on disk following the existing `ExecutionGraphInfoStore`
pattern. The same data is available through new REST endpoints:

- `/jobs/:jobid/rescales/overview`
- `/jobs/:jobid/rescales/history`
- `/jobs/:jobid/rescales/details/:rescaleuuid`
- `/jobs/:jobid/rescales/summary`

The feature is controlled by:

- `web.adaptive-scheduler.rescale-history.size` (default: `0`) — maximum number of rescale
  records retained per job. Setting `0` disables the feature.

#### Web UI for AdaptiveScheduler rescale history

##### [FLINK-22258](https://issues.apache.org/jira/browse/FLINK-22258) (FLIP-487)

Building on FLIP-495, the Flink Web UI gains a new "Rescales" tab for streaming jobs running
with the adaptive scheduler. Subpages expose rescale counts, the latest events, a historical
timeline, duration statistics with percentiles, per-event details, and the adaptive scheduler
configuration in effect. The existing `/jobs/overview` endpoint is extended with `schedulerType`
and `jobType` fields so the UI can render adaptive-scheduler-specific information.

See the [Elastic Scaling documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/elastic_scaling/)
for details.

#### Checkpointing during recovery

##### [FLINK-35761](https://issues.apache.org/jira/browse/FLINK-35761) (FLIP-547)

When restoring from an unaligned checkpoint, a task previously stayed in `INITIALIZING` until
*all* recovered input buffers had been processed before transitioning to `RUNNING`. For
high-parallelism jobs with large numbers of in-flight buffers, this could leave the task unable
to checkpoint for tens of minutes (30+ minutes in real cases), so any subsequent failure had to
restart from the original checkpoint. Flink 2.3 fixes this by transitioning to `RUNNING` as
soon as all input buffers have been added to `RecoveredInputChannel` and by allowing the
recovered input channels to be snapshotted, so a new unaligned checkpoint can be taken during
recovery. Two opt-in options are introduced:

- `execution.checkpointing.unaligned.recover-output-on-downstream.enabled` (default: `false`)
- `execution.checkpointing.unaligned.during-recovery.enabled` (default: `false`; requires the
  option above to be enabled).

#### Application Management

##### [FLINK-38755](https://issues.apache.org/jira/browse/FLINK-38755) (FLIP-549)

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

##### [FLINK-38972](https://issues.apache.org/jira/browse/FLINK-38972) (FLIP-560)

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

##### [FLINK-38603](https://issues.apache.org/jira/browse/FLINK-38603) (FLIP-553)

Jobs with large numbers of tasks and operators can produce metric payloads big enough for the
OTel gRPC backend to reject them, causing exported metric data to be dropped in production. The
existing exporter had three concrete limitations: gzip compression was not exposed in Flink
configuration, all data points went out in a single gRPC call without pagination, and metric
attributes such as `task_name` could grow to contain hundreds of operator names and bloat the
payload further. Flink 2.3 adds three opt-in robustness features to address these (all backward
compatible):

- `metrics.reporter.otel.exporter.compression` — `gzip` or `none` (default).
- `metrics.reporter.otel.batch.size` — split a single export into multiple gRPC calls; default
  `0` (disabled).
- `metrics.reporter.otel.transform.attribute-value-length-limits.<attribute_name>` — per-attribute
  truncation; `*` applies a global default.

### Documentation

#### Documentation restructure

##### [FLINK-38945](https://issues.apache.org/jira/browse/FLINK-38945) (FLIP-561)

The Flink documentation has been reorganized to make navigation easier. Highlights:

- Flink SQL gets a dedicated top-level section, separated from the Table API.
- Relational streaming concepts (changelogs, dynamic tables, state, etc.) are promoted to a
  top-level Concepts section.
- Python documentation is integrated into the relevant API sections instead of living in a
  standalone area.
- Contributor-facing content has been relocated outside the main user-facing docs.

Existing URLs continue to work via redirects. The top-level structure is documented on the
[docs landing page](https://nightlies.apache.org/flink/flink-docs-release-2.3/).
