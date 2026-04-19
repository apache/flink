<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Flink - AI Agent Coding Guide

This document is the AI coding guide for Apache Flink. It distills the key rules, patterns, and quality standards from the codebase, Checkstyle rules (`tools/maven/checkstyle.xml`), Spotless configuration (`pom.xml`), architecture tests (`flink-architecture-tests/`), and the official contribution guidelines.

**Purpose:** This guide helps AI coding agents contribute to Apache Flink by providing project-specific conventions, architectural patterns, and quality standards.

**Sections:** 1. Critical Rules | 2. API Stability & Design Patterns | 3. Code Organization & Module Structure | 4. Error Handling | 5. Concurrency & Thread Safety | 6. Testing | 7. Dependencies & Shading | 8. Configuration | 9. Serialization/RPC | 10. Module Boundaries | 11. Build & CI | 12. Git & Pull Request | 13. AI Agent Behavioral Boundaries

---

## 1. Critical Rules (Must / Must Not)

**Automatically enforced by Checkstyle + Spotless + ArchUnit + RAT** — violations cause CI failure.

### 1.1 Forbidden Imports (Use flink-shaded Instead)

Flink shade-repackages external dependencies. Source code **must** use the shaded paths:

```java
// ❌ Forbidden (enforced by checkstyle IllegalImport/Regexp)
import com.google.common.*              // → ✅ org.apache.flink.shaded.guava33.com.google.common.*
import org.codehaus.jackson.*           // → ✅ org.apache.flink.shaded.jackson2.com.fasterxml.jackson.*
import io.netty.*                       // → ✅ org.apache.flink.shaded.netty4.io.netty.*
import org.objectweb.asm.*              // → ✅ org.apache.flink.shaded.asm9.org.objectweb.asm.*
import com.jayway.jsonpath.*            // → ✅ org.apache.flink.shaded.jsonpath.*
import org.codehaus.jettison.*          // → ✅ com.fasterxml.jackson.*

// ❌ Forbidden mocking frameworks (mocking is discouraged; use reusable Testing classes)
import org.mockito.*;
import org.powermock.*;

// ❌ Forbidden commons-lang (use commons-lang3 only)
import org.apache.commons.lang.*

// ❌ Forbidden testcontainers internal package
import org.testcontainers.shaded.*

// ❌ Forbidden JetBrains annotations (Checkstyle enforced)
import org.jetbrains.annotations.Nullable   // → ✅ import javax.annotation.Nullable
import org.jetbrains.annotations.NotNull    // → ✅ import javax.annotation.Nonnull
```

### 1.2 Required Flink Utility Classes

```java
// ❌ com.google.common.base.Preconditions           → ✅ org.apache.flink.util.Preconditions (static import)
// ❌ com.google.common.annotations.VisibleForTesting → ✅ org.apache.flink.annotation.VisibleForTesting
// ❌ org.apache.commons.lang(3).SerializationUtils  → ✅ org.apache.flink.util.InstantiationUtil
// ❌ org.apache.commons.lang3.Validate              → ✅ Use Flink Preconditions
// ❌ Throwables.propagate(...)                      → Deprecated; use ExceptionUtils
// ❌ Boolean.getBoolean("prop") / Integer.getInteger / Long.getLong
//    → ✅ Boolean.parseBoolean(System.getProperty("prop"))
```

Common Flink utility classes (under `org.apache.flink.util.*`):
- `Preconditions`, `IOUtils`, `InstantiationUtil`, `ExceptionUtils`, `NetUtils`, `FileUtils`, `StringUtils`, `CollectionUtil`, `OperatingSystem`

### 1.3 Java Version Compatibility

**Build JDK:** Java **11, 17, or 21** (Maven 3.8.6+ required). Flink 2.x defaults to JDK 17 via `-Pjava17-target`.

**Source / target bytecode:**
- `source.java.version = 11` (POM property)
- `target.java.version = 17` (default target)
- Java 8 compatibility has been dropped in Flink 2.x; you no longer need to avoid Java 9–11 APIs.

**Still note:**
- Changes to public APIs (`@Public`, `@PublicEvolving`) must consider downstream user JDK versions.
- `flink-tests-java17/` contains JDK17-only tests; in common modules, avoid APIs that only exist in higher JDKs (e.g., Java 17 sealed classes).
- `mockito-core` is currently 5.x and only applies to test code; mocking in production code is still **forbidden**.

### 1.4 Test Assertions (AssertJ Mandatory)

Flink has **standardized on AssertJ** for new code. JUnit 4's `Assert.*` still exists in legacy code but **must not** be introduced in new code:

```java
// ❌ Assertions.assertEquals(expected, actual)       (JUnit 5)
// ❌ Assert.assertEquals(expected, actual)           (JUnit 4)
// ✅ assertThat(actual).isEqualTo(expected);

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

assertThat(list).hasSize(3).contains("a", "b");
assertThatThrownBy(() -> doSomething())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null");
```

**Test framework:** New tests use **JUnit 5 (Jupiter)**; legacy JUnit 4 code coexists via `junit-vintage-engine`. Do not introduce JUnit 4 in new modules.

### 1.5 Code Style (Enforced by Checkstyle + Spotless)

- **No wildcard imports** (`import java.util.*`) — Checkstyle `AvoidStarImport`; set the IntelliJ threshold to 9999.
- **No trailing whitespace** — run `./mvnw spotless:apply`.
- **Java-style array declarations:** `String[] args`, not `String args[]` (Checkstyle `ArrayTypeStyle`).
- **Braces are required:** `if (x) { doIt(); }`, even for single-line bodies (Checkstyle `NeedBraces`).
- **No `TODO(username)`, `FIXME`, `XXX`, `@author`** — use `TODO:` without a username; use git blame for history.
- **Upper-case `L` for long literals:** `100L`, not `100l` (Checkstyle `UpperEll`).
- **No empty statements:** no standalone `;` or consecutive `;;`.
- **Import order (Spotless enforced):** `org.apache.flink` → `org.apache.flink.shaded` → blank line → other → blank line → `javax` → `java` → `scala` → blank line → static imports.

### 1.6 Naming Conventions (Checkstyle Enforced)

| Element | Regex / Constraint |
|------|--------|
| Package name | `^[a-z]+(\.[a-z][a-z0-9]*)*$` (all lowercase, first segment cannot contain digits) |
| Type name | `^[A-Z][a-zA-Z0-9]*$` |
| Non-private constant | `^([A-Z][A-Z0-9]*(_[A-Z0-9]+)*|FLAG_.*)$` |
| Static non-final variable | `^[a-z][a-zA-Z0-9]*_?$` |
| Regular member / parameter / local variable | `^[a-z][a-zA-Z0-9]*$` |
| Method name | `^[a-z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$` |

### 1.7 Documentation

- **File size limit: 3100 lines** (Checkstyle `FileLength`).
- **Classes, interfaces, enums, and methods** at `protected` or higher visibility **must** have Javadoc (Checkstyle `JavadocType`, `JavadocMethod`).
- Javadoc paragraphs must use `<p>` tags correctly (`JavadocParagraph`).
- **License header:** All `.java` files must contain the Apache 2.0 license header, enforced by `apache-rat-plugin` during the `validate` phase; Spotless automatically inserts it before the `package` keyword. See the template in `spotless.license.header` in the root `pom.xml`.

---

## 2. API Stability & Design Patterns

### 2.1 API Stability Annotations (`org.apache.flink.annotation.*`)

```java
@Public           // Stable public API; cannot break compatibility within a major version (top-level APIs like Connection-style)
@PublicEvolving   // User-facing but signatures may still change in minor versions (new features / operators)
@Experimental     // Experimental; may change or be removed at any time
@Internal         // Internal implementation; external code must not depend on it
@VisibleForTesting // Widened visibility only for tests
```

**Principles:**
- **New user-facing APIs** default to `@PublicEvolving`; only upgrade to `@Public` once they stabilize.
- **Runtime, scheduling, RPC, execution plans**, and other internals must be marked `@Internal`.
- **Do not** use `org.jetbrains.annotations.*` from outside (Checkstyle enforced); use `javax.annotation.*` for `@Nullable`/`@Nonnull`.

**Reference:** `flink-annotations/src/main/java/org/apache/flink/annotation/`

### 2.2 Builder Pattern (`ConfigOption` as a Canonical Example)

```java
public static final ConfigOption<Duration> CLIENT_TIMEOUT =
        ConfigOptions.key("client.timeout")
                .durationType()
                .defaultValue(Duration.ofSeconds(30))
                .withDescription("The client RPC timeout.");
```

**Reference:** [flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java](flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java)

### 2.3 Factories and SPI

- **SPI:** Connectors / formats / file systems are loaded via `META-INF/services/` (`DynamicTableSourceFactory`, `FileSystemFactory`, `StateBackendFactory`, etc.).
- **Factory classes:** private constructors, static factory methods, return interface types.
- **Canonical examples:** `StreamExecutionEnvironment.getExecutionEnvironment()`, `TableEnvironment.create(...)`.

### 2.4 Immutability and Thread-Safety Conventions

- Data objects should prefer `final` fields + private constructors + `equals/hashCode/toString`.
- Document thread safety with `@javax.annotation.concurrent.{ThreadSafe, NotThreadSafe, GuardedBy}`.

---

## 3. Code Organization & Module Structure

Flink is a multi-module Maven aggregator project. The root `pom.xml` declares `<groupId>org.apache.flink</groupId>` with `<version>2.4-SNAPSHOT</version>`.

### 3.1 Core Modules (Top-Level Dependency Chain, Bottom-Up)

| Module | Purpose |
|---|---|
| [flink-annotations](flink-annotations/) | Stability annotations (`@Public`, `@Internal`, etc.) |
| [flink-core-api](flink-core-api/) | Minimal core API, intended for strict downstream dependency |
| [flink-core](flink-core/) | Type system, configuration, `fs`, `util`, `io` infrastructure |
| [flink-rpc](flink-rpc/) | Flink RPC abstraction (based on Pekko) |
| [flink-runtime](flink-runtime/) | JobManager, TaskManager, scheduling, checkpoints, network stack |
| [flink-streaming-java](flink-streaming-java/) | DataStream API and operators |
| [flink-datastream](flink-datastream/) / [flink-datastream-api](flink-datastream-api/) | Next-gen DataStream v2 API |
| [flink-clients](flink-clients/) | Client submitters, CLI |
| [flink-optimizer](flink-optimizer/) | Batch / execution-plan optimizer |
| [flink-java](flink-java/) / [flink-scala](flink-scala/) | Classic Java/Scala APIs (Scala 2.12, only specific modules) |

### 3.2 Table / SQL

- [flink-table](flink-table/): Table API, SQL, Planner, Runtime, Catalog.

### 3.3 Runtime Support

- [flink-metrics](flink-metrics/): metrics system and reporters.
- [flink-state-backends](flink-state-backends/), [flink-dstl](flink-dstl/): state backends (including changelog).
- [flink-queryable-state](flink-queryable-state/): queryable state.
- [flink-filesystems](flink-filesystems/): S3, OSS, GS, Azure, HDFS, etc. plugins.
- [flink-runtime-web](flink-runtime-web/): Flink Web UI.

### 3.4 Deployment / Containers

- [flink-yarn](flink-yarn/), [flink-kubernetes](flink-kubernetes/), [flink-mesos](flink-mesos/), [flink-container](flink-container/), [flink-dist](flink-dist/) (binary distribution).

### 3.5 Connectors and Formats

- [flink-connectors](flink-connectors/): connectors **retained in the main repo** (base, files, datagen, hive, etc.).
- [flink-formats](flink-formats/): Avro, CSV, JSON, Parquet, ORC, etc.
- **Most connectors have been externalized** into separate repositories (see [README.md](README.md) "Externalized Connectors"): kafka, jdbc, elasticsearch, pulsar, mongodb, hbase, cassandra, aws, etc.

### 3.6 Support and Testing

- [flink-test-utils-parent](flink-test-utils-parent/): `MiniClusterExtension`, `FlinkAssertions`, etc.
- [flink-tests](flink-tests/) / [flink-tests-java17](flink-tests-java17/): aggregated integration tests.
- [flink-end-to-end-tests](flink-end-to-end-tests/): end-to-end tests.
- [flink-architecture-tests](flink-architecture-tests/): architecture-compliance tests based on ArchUnit.
- [flink-docs](flink-docs/), [flink-python](flink-python/), [flink-libraries](flink-libraries/), [flink-walkthroughs](flink-walkthroughs/), [flink-examples](flink-examples/), [flink-quickstart](flink-quickstart/), [flink-external-resources](flink-external-resources/), [flink-contrib](flink-contrib/), [flink-models](flink-models/).

### 3.7 Key Directories

```
flink/
├── flink-core/            # Fundamental data types, configuration, util
├── flink-runtime/         # JM/TM runtime
├── flink-streaming-java/  # DataStream API
├── flink-table/           # Table & SQL
├── flink-connectors/      # Retained connectors
├── flink-formats/         # Data formats
├── flink-dist/            # Distribution
│   └── src/main/flink-bin/bin/   # start-cluster.sh, jobmanager.sh, taskmanager.sh...
├── flink-architecture-tests/     # ArchUnit rules and violation snapshots
├── tools/                 # checkstyle / spotless / CI helpers
│   ├── maven/checkstyle.xml      # Checkstyle rules source
│   └── ci/                       # CI shell and Java checkers
├── .github/workflows/     # GitHub Actions (ci.yml, nightly.yml, etc.)
├── azure-pipelines.yml    # Azure Pipelines (historical community PRs)
└── pom.xml                # Root Maven POM
```

### 3.8 Naming Conventions

| Kind | Convention | Example |
|---|---|---|
| Interface | Short descriptive name | `TypeInformation`, `KeySelector` |
| Implementation class | `Impl` suffix or descriptive name | `StreamExecutionEnvironmentImpl`, `DefaultExecutionGraph` |
| Abstract base class | `Abstract` prefix | `AbstractStreamOperator` |
| Utility class | `Utils`/`Util` suffix; `final class` + private constructor | `NetUtils`, `IOUtils`, `CollectionUtil` |
| Unit test | `*Test.java` | `ConfigurationTest` |
| Integration test | `*ITCase.java` (default surefire `test.unit.pattern=**/*Test.*` runs only `*Test`) | `JobManagerHARecoveryITCase` |
| Test helper | `Testing*` prefix (reusable fakes) or `*TestBase` | `TestingRpcService`, `MiniClusterTestBase` |
| Exception class | `*Exception` suffix | `FlinkRuntimeException`, `JobExecutionException` |

### 3.9 Class Member Order

- **Fields:** static constants → static fields → instance fields.
- **Methods:** constructors → static factories → public → package-private → protected → private → static utils.
- **Modifier order:** `public protected private abstract static final transient volatile synchronized native strictfp`.

---

## 4. Error Handling

**Exception hierarchy:**
- [FlinkException](flink-core/src/main/java/org/apache/flink/util/FlinkException.java) (checked) — expected, user-visible errors.
- [FlinkRuntimeException](flink-core/src/main/java/org/apache/flink/util/FlinkRuntimeException.java) (unchecked) — runtime failures.
- Subsystem specializations: `JobException`, `JobExecutionException`, `CheckpointException`, `SerializationException`, etc.

**Argument validation:** Use [Preconditions](flink-core/src/main/java/org/apache/flink/util/Preconditions.java) at API boundaries:

```java
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

checkNotNull(config, "config must not be null");
checkArgument(parallelism > 0, "parallelism must be positive, but was %s", parallelism);
```

**Exception chaining and propagation:**
- Use `org.apache.flink.util.ExceptionUtils` to handle exception chains (`findThrowable`, `rethrow`, `stripCompletionException`, etc.).
- **Do not** use Guava's `Throwables.propagate(...)` (Checkstyle enforced).
- For `CompletableFuture` scenarios, use `FutureUtils` (`org.apache.flink.util.concurrent.FutureUtils`).

---

## 5. Concurrency & Thread Safety

**Annotations:** `@javax.annotation.concurrent.{ThreadSafe, NotThreadSafe, GuardedBy}`.

**Locking:**
- Prefer explicit lock objects: `private final Object lock = new Object();`.
- Use `volatile` or `java.util.concurrent.atomic.*` for fields read across threads.
- `MailboxExecutor` is the core of the TaskManager operator thread model; operators **must not** directly `new Thread(...)`. Use the mailbox for async work.

**CompletableFuture:**
- Use utilities such as `FutureUtils.combineAll/orTimeout/retry`.
- Do not block with `.get()` inside `RpcEndpoint` threads; use `.thenApply/.thenCompose`.

**Resource management:** Implement `AutoCloseable` and use try-with-resources; `close()` must be idempotent.

---

## 6. Testing Standards

### 6.1 Framework and Assertions

- **JUnit 5 Jupiter** (new code) + **AssertJ** (mandatory). See §1.4.
- Legacy modules retaining JUnit 4 run via `junit-vintage-engine`, but **must not** introduce JUnit 4 in new code.
- **Mockito/PowerMock are forbidden** (Checkstyle blocks imports) — use reusable `Testing*` fakes instead.
- **Timeouts:** Rely on the global surefire timeout; do not put `@Timeout(...)` on individual tests.

### 6.2 Test Class Naming and Execution

- `*Test.java`: unit tests, executed by surefire (`test.unit.pattern=**/*Test.*`).
- `*ITCase.java`: integration tests, executed by the failsafe plugin during `mvn verify`.
- Integration tests typically use `MiniCluster` or `TestContainers`.

### 6.3 Common Test Extensions and Base Classes

```java
@ExtendWith(MiniClusterExtension.class)
class MyStreamingITCase {
    @Test
    void myTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ...
        env.execute();
    }
}
```

Common classes:
- [MiniClusterExtension](flink-test-utils-parent/flink-test-utils/src/main/java/org/apache/flink/test/junit5/MiniClusterExtension.java): starts a MiniCluster and injects `StreamExecutionEnvironment`.
- `InternalMiniClusterExtension`: MiniCluster extension used internally by `flink-runtime`.
- `MiniClusterWithClientResource` (legacy JUnit 4 interface): new code should use the JUnit 5 extensions above.
- `FlinkAssertions`: Flink-specific AssertJ extensions.

### 6.4 Architecture Tests (ArchUnit)

The `flink-architecture-tests/` module uses ArchUnit to freeze architectural rules. When new code introduces new violations, **do not** directly modify the `violations` store files — fix the violation first. See [flink-architecture-tests/README.md](flink-architecture-tests/README.md).

---

## 7. Dependencies & Shading

### 7.1 flink-shaded

Flink's shaded dependencies come from the separate [flink-shaded](https://github.com/apache/flink-shaded) repo. Versions are controlled by root `pom.xml` properties `flink.shaded.version`, `flink.shaded.jackson.version`, etc. Current examples:

| Shaded coordinate | Source package prefix |
|---|---|
| `org.apache.flink.shaded.guava33.com.google.common.*` | Guava |
| `org.apache.flink.shaded.jackson2.com.fasterxml.jackson.*` | Jackson |
| `org.apache.flink.shaded.netty4.io.netty.*` | Netty |
| `org.apache.flink.shaded.asm9.org.objectweb.asm.*` | ASM |
| `org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.*` | ZooKeeper |
| `org.apache.flink.shaded.jsonpath.*` | json-path |

**Rule:** Source code must reference the shaded paths; `pom.xml` depends directly on `org.apache.flink:flink-shaded-*`. Do not redeclare original coordinates such as `com.google.guava:guava`.

### 7.2 Dependency Aggregation and `<optional>`

- The root `pom.xml` property `flink.markBundledAsOptional=true` marks shade-bundled dependencies as `optional`, preventing pollution of downstream users.
- Before adding a third-party dependency: confirm license compatibility (Apache 2.0 compatible), ensure necessity, and follow the shading strategy.

---

## 8. Configuration Patterns

### 8.1 ConfigOption Definition

```java
public static final ConfigOption<Duration> SLOT_REQUEST_TIMEOUT =
        ConfigOptions.key("slot.request.timeout")
                .durationType()
                .defaultValue(Duration.ofMinutes(5))
                .withDeprecatedKeys("slot.request.timeout.ms")
                .withDescription("Timeout for requesting slots from ResourceManager.");
```

**Available types:** `stringType()`, `intType()`, `longType()`, `booleanType()`, `durationType()`, `memoryType()`, `enumType(X.class)`, `mapType()`, `stringType().asList()`, `noDefaultValue()`, `withDeprecatedKeys(...)`, `withFallbackKeys(...)`.

**Reference:** [flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java](flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java)

### 8.2 Usage

```java
Configuration conf = new Configuration();
conf.set(CoreOptions.DEFAULT_PARALLELISM, 4);
int parallelism = conf.get(CoreOptions.DEFAULT_PARALLELISM);
Optional<Duration> timeout = conf.getOptional(SLOT_REQUEST_TIMEOUT);
```

### 8.3 Naming Conventions

- Use dotted hierarchy with hyphens: `{category}.{sub}.{option-name}`, e.g., `execution.checkpointing.interval`, `taskmanager.memory.process.size`.
- Document every ConfigOption with `withDescription(...)`; the doc generation module `flink-docs` scans these automatically.

---

## 9. Serialization & RPC

### 9.1 Serialization

- **Flink type system:** POJOs, Tuples, Rows, etc. are handled by Flink's `TypeInformation`/`TypeSerializer`. **Avoid** overusing plain `Serializable` in runtime data paths.
- **State backends** require `TypeSerializer` paired with a `TypeSerializerSnapshot` to support upgrade compatibility.
- **Do not** use `SerializationUtils` from commons-lang(3) (Checkstyle).

### 9.2 RPC

- The underlying implementation has migrated from Akka to **Pekko**; the Maven module names retain their historical names (`flink-rpc-akka` / `flink-rpc-akka-loader`), while the Java packages are under `org.apache.flink.runtime.rpc.pekko`.
- **Do not depend directly on `flink-rpc-akka`** (the `forbid-direct-akka-rpc-dependencies` Enforcer rule in the root `pom.xml` enforces this). Usage:
  - Production code: load the RPC system dynamically via `RpcSystem#load`.
  - Test code: use `TestingRpcService`.
- All RPC interfaces extend `RpcGateway`, implementations extend `RpcEndpoint`.
- RPC methods return `CompletableFuture<...>`; parameters must be serializable.
- New RPC interfaces must be marked `@Internal`.

---

## 10. Module Boundaries (ArchUnit Enforced)

**Core chain:** `flink-annotations` → `flink-core-api` → `flink-core` → `flink-rpc` → `flink-runtime` → `flink-streaming-java` / `flink-optimizer` → `flink-clients`.

**Rules:**
1. **Lower layers cannot depend on upper layers:** `flink-core` must not depend on `flink-runtime`; `flink-runtime` must not depend on `flink-streaming-java`.
2. **Public API boundaries:** classes annotated `@Public`/`@PublicEvolving` must not expose `@Internal` types in their signatures (ArchUnit rules verify this).
3. **Scala vs Java:** Scala modules (`flink-scala`, `flink-streaming-scala`) are optional bridging layers; new APIs should go through the Java side. Do not proactively add new Scala APIs.
4. **Connectors / formats** depend on `flink-streaming-java` and `flink-table-*`; reverse dependencies are forbidden.
5. For changes that violate architecture tests: fix the violation first, **do not** simply refreeze the violation snapshot (see §6.4).

---

## 11. Build, CI & Deployment

### 11.1 Prerequisites

- Unix-like system (Linux / macOS / WSL).
- **Maven 3.8.6+** (strongly recommend using the `./mvnw` wrapper).
- **JDK 11 / 17 / 21**; 17 is the default recommendation.

### 11.2 Build Commands

```bash
# Java 17 (default)
./mvnw clean package -DskipTests -Djdk17 -Pjava17-target

# Java 11
./mvnw clean package -DskipTests -Djdk11 -Pjava11-target

# Java 21
./mvnw clean package -DskipTests -Djdk21 -Pjava21-target

# Parallel build (faster)
./mvnw clean install -DskipTests -T 1C

# Run all tests
./mvnw clean verify

# Test a specific module
./mvnw verify -pl flink-runtime -am

# Run a single test
./mvnw test -Dtest=ConfigurationTest -pl flink-core
```

Artifacts live in `build-target/` (a symlink to `flink-dist/target/flink-<version>-bin/flink-<version>/`).

### 11.3 Code Formatting and Static Checks

```bash
./mvnw spotless:apply        # Auto-format Java (AOSP) + Scala (scalafmt)
./mvnw spotless:check        # Check only
./mvnw checkstyle:check -T1C # Run Checkstyle
./mvnw org.apache.rat:apache-rat-plugin:check -N   # License header check
```

- **google-java-format 1.24.0** (AOSP style) — the IntelliJ plugin **must use this version and not be upgraded** (see [DEVELOPMENT.md](DEVELOPMENT.md)).
- **Checkstyle 10.18.2**; IntelliJ plugin `Checkstyle-IDEA`.
- **Scala uses scalafmt** (`.scalafmt.conf`).

### 11.4 CI Pipelines

**GitHub Actions** (`.github/workflows/`):
- `ci.yml`: the main CI triggered on PRs and pushes (delegates to `template.flink-ci.yml`).
- `template.pre-compile-checks.yml`: Checkstyle, Spotless, license headers — JDK-independent.
- `nightly.yml` / `nightly-trigger.yml`: nightly full-suite tests.
- `docs.yml` / `docs-legacy.yml`: documentation builds.

**Azure Pipelines** (`azure-pipelines.yml`): individual forks can enable this for faster testing.

**CI enforced checks:** Checkstyle → Spotless → License Headers → compile → unit tests → integration tests → architecture tests. Any failure blocks merging.

### 11.5 Documentation

- Documentation source lives in [docs/](docs/) (Hugo); run `docs/build_docs.sh` for local preview.
- Configuration option docs are generated automatically from `ConfigOption` by `flink-docs`.

---

## 12. Git & Pull Request Workflow

### 12.1 Fork-First Principle

**Never push directly to `apache/flink` upstream.** Always use a fork:

```bash
git remote -v
# origin   https://github.com/apache/flink.git       (read-only upstream usage for fetch/push)
# fork     https://github.com/<your-username>/flink.git

# If missing:
gh repo fork apache/flink --remote --remote-name fork
# or
git remote add fork https://github.com/<your-username>/flink.git
```

### 12.2 JIRA and Branches

- **A JIRA ticket** ([FLINK-XXXX](https://issues.apache.org/jira/projects/FLINK/issues)) is **required**; only documentation typos are exempt.
- Branch naming convention: `FLINK-XXXX-<short-description>`.

### 12.3 Commit Message Format

```
[FLINK-XXXX][component] Short description (≤ 72 chars)

Detailed explanation (optional).
```

- **component** examples: `[runtime]`, `[table-planner]`, `[connectors/kafka]`, `[docs]`, `[build]`, `[tests]`, `[core]`, `[streaming]`, `[python]`, `[k8s]`.
- Pure typos can use `[hotfix][docs] ...` without a JIRA ticket.
- Keep each PR to a single JIRA; do not mix unrelated issues.

### 12.4 Pre-Push Self-Check

```bash
./mvnw spotless:check
./mvnw checkstyle:check -T 1C
./mvnw org.apache.rat:apache-rat-plugin:check -N
./mvnw verify -pl <affected-modules>
git diff upstream/master...HEAD   # default branch is currently master
```

**Do not commit:** secrets, credentials, local configuration, `target/` artifacts, or IDE configuration (except shared configuration already tracked under `.idea/`).

### 12.5 Creating a Pull Request

```bash
git fetch upstream master
git rebase upstream/master
git push -u fork <branch-name>
gh pr create --web --title "[FLINK-XXXX][component] Short title"
```

**PR description must follow the [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) template**, filling in:
- What is the purpose of the change
- Brief change log
- Verifying this change
- Does this pull request potentially affect ... (Dependencies / Public API / Serializers / per-record path / Deployment / S3, etc.)
- Documentation

---

## 13. AI Agent Behavioral Boundaries

### 13.1 Seek Approval Before Acting

**Large-scale changes requiring approval:**
- Cross-module refactors (≥ 5 `flink-*` modules).
- Adding / upgrading third-party dependencies or bumping flink-shaded.
- Modifying Checkstyle / Spotless / ArchUnit rules.
- Modifying CI workflows (`.github/workflows/`, `tools/ci/`).
- Modifying `@Public` API signatures.
- Modifying serializers, savepoint formats, or RPC protocols.
- Modifying violation snapshot files under `flink-architecture-tests`.

### 13.2 Never Do Without Explicit Permission

- ❌ Commit secrets, credentials, API keys, or tokens.
- ❌ Push directly to `apache/flink` upstream (always use a fork).
- ❌ Force-push to shared branches `master` / `release-*`.
- ❌ Perform destructive git operations (`reset --hard`, `clean -fdx`, `branch -D`, `push --force` to upstream).
- ❌ Modify auto-generated files (e.g., Calcite/Pekko-generated code, `flink-docs` outputs, `japicmp` baselines).
- ❌ `git commit --no-verify` to skip hooks.
- ❌ Disable Checkstyle / Spotless / ArchUnit rules or add new snapshots to `violations/` just to bypass failures.
- ❌ Introduce new dependencies without discussing compatibility / license.
- ❌ Introduce explicitly forbidden items: Mockito/PowerMock, Guava `Preconditions`, `Throwables.propagate`, wildcard imports, etc.

### 13.3 Autonomously Allowed (Within Scope)

- ✅ Read any file in the repo.
- ✅ Run `./mvnw` test/build commands.
- ✅ Run `./mvnw spotless:apply` to auto-format.
- ✅ Fix Checkstyle violations per §1.
- ✅ Create feature branches on your own fork.
- ✅ Commit and push to your own fork, and create PRs.

### 13.4 Required Verification Checklist Before Submission

| Item | Command |
|---|---|
| Code format | `./mvnw spotless:check` |
| Checkstyle | `./mvnw checkstyle:check -T 1C` |
| License header | `./mvnw org.apache.rat:apache-rat-plugin:check -N` |
| Affected-module tests | `./mvnw verify -pl <modules> -am` |
| Architecture tests (if relevant) | `./mvnw verify -pl flink-architecture-tests-production,flink-architecture-tests-test` |
| PR linked to JIRA | Confirm FLINK-XXXX exists and the title format is correct |

**When in doubt:** Before making changes that may break a public API, compatibility, or architecture, confirm with the user first.
