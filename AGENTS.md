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

# Flink AI Agent Instructions

This file provides guidance for AI coding agents working with the Apache Flink codebase.

## Prerequisites

- Java 11, 17 (default), or 21. Java 11 syntax must be used in all modules. Java 17 syntax (records, sealed classes, pattern matching) is only permitted in the `flink-tests-java17` module.
- Maven 3.8.6 (Maven wrapper `./mvnw` included; prefer it)
- Git
- Unix-like environment (Linux, macOS, WSL, Cygwin)

## Commands

### Build

- Full build (Java 17 default): `./mvnw clean package -DskipTests -Djdk17 -Pjava17-target`
- Java 11: `./mvnw clean package -DskipTests -Djdk11 -Pjava11-target`
- Java 21: `./mvnw clean package -DskipTests -Djdk21 -Pjava21-target`
- Full build with tests: `./mvnw clean verify`
- Single module: `./mvnw clean package -DskipTests -pl flink-core-api`
- Single module with tests: `./mvnw clean verify -pl flink-core-api`

### Testing

- Single test class: `./mvnw -pl flink-core-api -Dtest=MemorySizeTest test`
- Single test method: `./mvnw -pl flink-core-api -Dtest=MemorySizeTest#testParseBytes test`

### Code Quality

- Format code (Java + Scala): `./mvnw spotless:apply`
- Check formatting: `./mvnw spotless:check`
- Checkstyle config: `tools/maven/checkstyle.xml` (v10.18.2)

## Repository Structure

Every module from the root pom.xml, organized by function. Flink provides three main user-facing APIs (recommended in this order: SQL, Table API, DataStream API) plus a newer DataStream v2 API.

### Core Infrastructure

- `flink-annotations` — Stability annotations (`@Public`, `@PublicEvolving`, `@Internal`, `@Experimental`) and `@VisibleForTesting`
- `flink-core-api` — Core API interfaces (functions, state, types) shared by all APIs
- `flink-core` — Core implementation (type system, serialization, memory management, configuration)
- `flink-runtime` — Distributed runtime (JobManager, TaskManager, scheduling, network, state)
- `flink-clients` — CLI and client-side job submission
- `flink-rpc/` — RPC framework
  - `flink-rpc-core` — RPC interfaces
  - `flink-rpc-akka`, `flink-rpc-akka-loader` — Pekko-based RPC implementation

### SQL / Table API (recommended API for most users)

- `flink-table/`
  - `flink-sql-parser` — SQL parser (extends Calcite SQL parser)
  - `flink-table-common` — Shared types, descriptors, catalog interfaces
  - `flink-table-api-java` — Table API for Java
  - `flink-table-api-scala` — Table API for Scala
  - `flink-table-api-bridge-base`, `flink-table-api-java-bridge`, `flink-table-api-scala-bridge` — Bridges between Table and DataStream APIs
  - `flink-table-api-java-uber` — Uber JAR for Table API
  - `flink-table-planner` — SQL/Table query planning and optimization (Calcite-based)
  - `flink-table-planner-loader`, `flink-table-planner-loader-bundle` — Classloader isolation for planner
  - `flink-table-runtime` — Runtime operators for Table/SQL queries
  - `flink-table-calcite-bridge` — Bridge to Apache Calcite
  - `flink-sql-gateway-api`, `flink-sql-gateway` — SQL Gateway for remote SQL execution
  - `flink-sql-client` — Interactive SQL CLI
  - `flink-sql-jdbc-driver`, `flink-sql-jdbc-driver-bundle` — JDBC driver for SQL Gateway
  - `flink-table-code-splitter` — Code generation utilities
  - `flink-table-test-utils` — Test utilities for Table/SQL

### DataStream API (original streaming API)

- `flink-streaming-java` — DataStream API and stream processing operator implementations

### DataStream API v2 (newer event-driven API)

- `flink-datastream-api` — DataStream v2 API definitions
- `flink-datastream` — DataStream v2 API implementation

### Connectors (in-tree)

- `flink-connectors/`
  - `flink-connector-base` — Base classes for source/sink connectors
  - `flink-connector-files` — Unified file system source and sink
  - `flink-connector-datagen` — DataGen source for testing
  - `flink-connector-datagen-test` — Tests for DataGen connector
  - `flink-hadoop-compatibility` — Hadoop InputFormat/OutputFormat compatibility
  - `flink-file-sink-common` — Common file sink utilities
- Most connectors (Kafka, JDBC, Elasticsearch, etc.) live in separate repos under [github.com/apache](https://github.com/apache); see README.md for the full list

### Formats

- `flink-formats/`
  - `flink-json`, `flink-csv`, `flink-avro`, `flink-parquet`, `flink-orc`, `flink-protobuf` — Serialization formats
  - `flink-avro-confluent-registry` — Avro with Confluent Schema Registry
  - `flink-sequence-file`, `flink-compress`, `flink-hadoop-bulk`, `flink-orc-nohive` — Hadoop-related formats
  - `flink-format-common` — Shared format utilities
  - `flink-sql-json`, `flink-sql-csv`, `flink-sql-avro`, `flink-sql-parquet`, `flink-sql-orc`, `flink-sql-protobuf` — SQL-layer format integrations
  - `flink-sql-avro-confluent-registry` — SQL-layer Avro with Confluent Schema Registry

### State Backends

- `flink-state-backends/`
  - `flink-statebackend-rocksdb` — RocksDB state backend
  - `flink-statebackend-forst` — ForSt state backend (experimental, RocksDB-based with JNI bindings)
  - `flink-statebackend-heap-spillable` — Heap-based spillable state backend
  - `flink-statebackend-changelog` — Changelog state backend
  - `flink-statebackend-common` — Shared state backend utilities
- `flink-dstl/flink-dstl-dfs` — State changelog storage (DFS-based persistent changelog for incremental checkpointing)

### File Systems

- `flink-filesystems/`
  - `flink-hadoop-fs` — Hadoop FileSystem abstraction
  - `flink-s3-fs-hadoop`, `flink-s3-fs-presto`, `flink-s3-fs-base` — S3 file systems
  - `flink-oss-fs-hadoop` — Alibaba OSS
  - `flink-azure-fs-hadoop` — Azure Blob Storage
  - `flink-gs-fs-hadoop` — Google Cloud Storage
  - `flink-fs-hadoop-shaded` — Shaded Hadoop dependencies

### Queryable State

- `flink-queryable-state/`
  - `flink-queryable-state-runtime` — Server-side queryable state service
  - `flink-queryable-state-client-java` — Client for querying operator state from running jobs

### Deployment

- `flink-kubernetes` — Kubernetes integration
- `flink-yarn` — YARN integration
- `flink-dist`, `flink-dist-scala` — Distribution packaging
- `flink-container` — Container entry-point and utilities for containerized deployments

### Metrics

- `flink-metrics/`
  - `flink-metrics-core` — Metrics API and core implementation
  - Reporter implementations: `flink-metrics-jmx`, `flink-metrics-prometheus`, `flink-metrics-datadog`, `flink-metrics-statsd`, `flink-metrics-graphite`, `flink-metrics-influxdb`, `flink-metrics-slf4j`, `flink-metrics-dropwizard`, `flink-metrics-otel`

### Libraries

- `flink-libraries/`
  - `flink-cep` — Complex Event Processing
  - `flink-state-processing-api` — Offline state access (savepoint reading/writing)

### Other

- `flink-models` — AI model integration (sub-modules: `flink-model-openai`, `flink-model-triton`)
- `flink-python` — PyFlink (Python API)
- `flink-runtime-web` — Web UI for JobManager dashboard
- `flink-external-resources` — External resource management (e.g., GPU)
- `flink-docs` — Documentation sources
- `flink-examples` — Example programs
- `flink-quickstart` — Maven archetype for new projects
- `flink-walkthroughs` — Tutorial walkthrough projects

### Testing

- `flink-tests` — Integration tests
- `flink-end-to-end-tests` — End-to-end tests
- `flink-test-utils-parent` — Test utility classes
- `flink-yarn-tests` — YARN-specific tests
- `flink-fs-tests` — FileSystem tests
- `flink-architecture-tests` — ArchUnit architectural boundary tests
- `tools/ci/flink-ci-tools` — CI tooling

## Architecture Boundaries

1. **Client** submits jobs to the cluster. Submission paths include the CLI (`bin/flink run` via `flink-clients`), the SQL Client (`bin/sql-client.sh` via `flink-sql-client`), the SQL Gateway (`flink-sql-gateway`, also accessible via JDBC driver), the REST API (direct HTTP to JobManager), programmatic execution (`StreamExecutionEnvironment.execute()` or `TableEnvironment.executeSql()`), and PyFlink (`flink-python`, wraps the Java APIs).
2. **JobManager** (`flink-runtime`) orchestrates execution: receives jobs, creates the execution graph, manages scheduling, coordinates checkpoints, and handles failover. Never runs user code directly.
3. **TaskManager** (`flink-runtime`) executes the user's operators in task slots. Manages network buffers, state backends, and I/O.
4. **Table Planner** (`flink-table-planner`) translates SQL/Table API programs into DataStream programs. The planner is loaded in a separate classloader (`flink-table-planner-loader`) to isolate Calcite dependencies.
5. **Connectors** communicate with external systems. Source connectors implement the `Source` API (FLIP-27); sinks implement the `SinkV2` API. Most connectors are externalized to separate repositories.
6. **State Backends** persist keyed state and operator state. RocksDB is the primary backend for production use.
7. **Checkpointing** provides exactly-once guarantees. The JobManager coordinates barriers through the data stream; TaskManagers snapshot local state to a distributed file system.

Key separations:

- **Planner vs Runtime:** The table planner generates code and execution plans; the runtime executes them. Changes to planning logic live in `flink-table-planner`; changes to runtime operators live in `flink-table-runtime` or `flink-streaming-java`.
- **API vs Implementation:** Public API surfaces (`flink-core-api`, `flink-datastream-api`, `flink-table-api-java`) are separate from implementation modules. API stability annotations control what users can depend on.
- **ArchUnit enforcement:** `flink-architecture-tests/` contains ArchUnit tests that enforce module boundaries. New violations should be avoided; if unavoidable, follow the freeze procedure in `flink-architecture-tests/README.md`.

## Common Change Patterns

This section maps common types of Flink changes to the modules they touch and the verification they require.

### Adding a new SQL built-in function

1. Register in `flink-table-common` in `BuiltInFunctionDefinitions.java` (definition, input/output type strategies, runtime class reference)
2. Implement in `flink-table-runtime` under `functions/scalar/` (extend `BuiltInScalarFunction`, implement `eval()` method)
3. Add tests in `flink-table-planner` (planner integration) and `flink-table-runtime` (unit tests)
4. Document in `flink-docs`
5. Verify: planner tests + runtime tests + SQL ITCase

### Adding a new configuration option

1. Define `ConfigOption<T>` in the relevant config class (e.g., `ExecutionConfigOptions.java` in `flink-table-api-java`)
2. Use `ConfigOptions.key("table.exec....")` builder with type, default value, and description
3. Add `@Documentation.TableOption` annotation for auto-generated docs
4. Document in `flink-docs` if user-facing
5. Verify: unit test for default value, ITCase for behavior change

### Adding a new table operator (e.g., join type, aggregate)

1. **Planner rule** in `flink-table-planner` under `plan/rules/physical/stream/` (match conditions, transform to physical node)
2. **ExecNode** in `flink-table-planner` under `plan/nodes/exec/stream/` (bridge between planner and runtime; Jackson-serializable with `@ExecNodeMetadata`)
3. **Runtime operator** in `flink-table-runtime` under `operators/` (extends `TableStreamOperator`, implements `OneInputStreamOperator` or `TwoInputStreamOperator`)
4. Verify: planner rule tests + ExecNode JSON plan tests + runtime operator tests + SQL ITCase

### Adding a new connector (Source or Sink)

1. Implement the `Source` API (`flink-connector-base`): `SplitEnumerator`, `SourceReader`, `SourceSplit`, serializers (`SimpleVersionedSerializer`)
2. Or implement the `SinkV2` API for sinks
3. Most new connectors go in separate repos under `github.com/apache`, not in the main Flink repo
4. Verify: unit tests + ITCase with real or embedded external system

### Modifying state serializers

1. Changes to `TypeSerializer` require a corresponding `TypeSerializerSnapshot` for migration
2. Bump version in `getCurrentVersion()`, handle old versions in `readSnapshot()`
3. Snapshot must have no-arg constructor for reflection-based deserialization
4. Implement `resolveSchemaCompatibility()` for upgrade paths
5. Verify: serializer snapshot migration tests, checkpoint restore tests across versions

### Introducing or changing user-facing APIs (`@Public`, `@PublicEvolving`, `@Experimental`)

1. New user-facing API requires a voted FLIP (Flink Improvement Proposal); this applies to `@Public`, `@PublicEvolving`, and `@Experimental` since users build against all three
2. Every user-facing API class and method must carry a stability annotation
3. Changes to existing `@Public` or `@PublicEvolving` API must maintain backward compatibility
4. `@Internal` APIs can be changed freely; users should not depend on them
5. Update JavaDoc on the changed class/method
6. Add to release notes
7. Verify: ArchUnit tests pass, no new architecture violations

## Coding Standards

- **Format Java files with Spotless immediately after editing:** `./mvnw spotless:apply`. Uses google-java-format with AOSP style.
- **Scala formatting:** Spotless + scalafmt (config at `.scalafmt.conf`, maxColumn 100).
- **Checkstyle:** `tools/maven/checkstyle.xml` (version defined in root `pom.xml` as `checkstyle.version`). Some modules (flink-core, flink-optimizer, flink-runtime) are not covered by checkstyle enforcement, but conventions should still be followed.
- **No new Scala code.** All Flink Scala APIs are deprecated per FLIP-265. Write all new code in Java.
- **Apache License 2.0 header** required on all new files (enforced by Apache Rat). Use an HTML comment for markdown files.
- **API stability annotations:** Every user-facing API class and method must have a stability annotation. `@Public` (stable across minor releases), `@PublicEvolving` (may change in minor releases), `@Experimental` (may change at any time). These are all part of the public API surface that users build against. `@Internal` marks APIs with no stability guarantees that users should not depend on.
- **Logging:** Use parameterized log statements (SLF4J `{}` placeholders), never string concatenation.
- **No Java serialization** for new features (except internal RPC message transport).
- Full code style guide: https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/

## Testing Standards

- Add tests for new behavior, covering success, failure, and edge cases.
- Use **JUnit 5** + **AssertJ** assertions. Do not use JUnit 4 or Hamcrest in new test code.
- Prefer real test implementations over Mockito mocks where possible.
- **Integration tests:** Name classes with `ITCase` suffix (e.g., `MyFeatureITCase.java`).
- **Red-green verification:** For bug fixes, verify that new tests actually fail without the fix before confirming they pass with it.
- **Test location** mirrors source structure within each module.
- Follow the testing conventions at https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#7-testing

## Commits and PRs

### Commit message format

- `[FLINK-XXXX][component] Description` where FLINK-XXXX is the JIRA issue number
- `[hotfix][component] Description` for typo fixes without JIRA
- Each commit must have a meaningful message including the JIRA ID
- Separate cleanup/refactoring from functional changes into distinct commits
- When AI tools were used: add `Generated-by: <Tool Name and Version>` trailer per [ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html)

### Pull request conventions

- Title format: `[FLINK-XXXX][component] Title of the pull request`
- A corresponding JIRA issue is required (except hotfixes for typos)
- Fill out the PR template completely but concisely: describe purpose, change log, testing approach, impact assessment
- Each PR should address exactly one issue
- Ensure `./mvnw clean verify` passes before opening a PR
- Always push to your fork, not directly to `apache/flink`
- Rebase onto the latest target branch before submitting

### AI-assisted contributions

- Disclose AI usage by checking the AI disclosure checkbox and uncommenting the `Generated-by` line in the PR template
- Add `Generated-by: <Tool Name and Version>` to commit messages
- Never add `Co-Authored-By` with an AI agent as co-author; agents are assistants, not authors
- You must be able to explain every change and respond to review feedback substantively

## Boundaries

### Ask first

- Adding or changing `@Public`, `@PublicEvolving`, or `@Experimental` annotations (these are user-facing API commitments requiring a FLIP)
- Large cross-module refactors
- New dependencies
- Changes to serialization formats (affects state compatibility)
- Changes to checkpoint/savepoint behavior

### Never

- Commit secrets, credentials, or tokens
- Push directly to `apache/flink`; always work from your fork
- Mix unrelated changes into one PR
- Use Java serialization for new features
- Edit generated files by hand when a generation workflow exists
- Use the legacy `SourceFunction` or `SinkFunction` interfaces for connectors; use the `Source` API (FLIP-27) and `SinkV2` API instead
- Add `Co-Authored-By` with an AI agent as co-author in commit messages; AI agents are assistants, not authors. Use `Generated-by: <Tool Name and Version>` instead.
- Suppress or bypass checkstyle rules (no `CHECKSTYLE:ON`/`CHECKSTYLE:OFF` comments, no adding entries to `tools/maven/suppressions.xml`, no `@SuppressWarnings`). Fix the code to satisfy checkstyle instead.
- Use destructive git operations unless explicitly requested

## References

- [README.md](README.md) — Build instructions and project overview
- [DEVELOPMENT.md](DEVELOPMENT.md) — IDE setup and development environment
- [.github/CONTRIBUTING.md](.github/CONTRIBUTING.md) — Contribution process
- [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) — PR checklist
- [Code Style Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/) — Detailed coding guidelines
- [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) — AI tooling policy
