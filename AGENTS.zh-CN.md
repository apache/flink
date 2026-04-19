<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Flink - AI 代理编码指南

本文档是 Apache Flink 的 AI 编码指南，包含从代码库分析、Checkstyle 规则（`tools/maven/checkstyle.xml`）、Spotless 配置（`pom.xml`）、架构测试（`flink-architecture-tests/`）和官方贡献指南中提炼出的关键规则、模式和标准。

**目的：** 本指南通过提供 Flink 项目特定的约定、架构模式和质量标准，帮助 AI 编码代理为 Apache Flink 贡献代码。

**章节：** 1. 关键规则 | 2. API 稳定性与设计模式 | 3. 代码组织与模块结构 | 4. 错误处理 | 5. 并发与线程安全 | 6. 测试 | 7. 依赖与 Shading | 8. 配置 | 9. 序列化/RPC | 10. 模块边界 | 11. 构建与 CI | 12. Git 与 Pull Request | 13. AI 代理行为边界

---

## 1. 关键规则（必须/禁止）

**由 Checkstyle + Spotless + ArchUnit + RAT 自动强制执行** —— 违反规则将导致 CI 失败。

### 1.1 禁止的导入（使用 flink-shaded 替代）

Flink 对外部依赖进行了 shade 重打包，源码中**必须**使用 shaded 版本的路径：

```java
// ❌ 禁止（由 checkstyle IllegalImport/Regexp 校验）
import com.google.common.*              // → ✅ org.apache.flink.shaded.guava33.com.google.common.*
import org.codehaus.jackson.*           // → ✅ org.apache.flink.shaded.jackson2.com.fasterxml.jackson.*
import io.netty.*                       // → ✅ org.apache.flink.shaded.netty4.io.netty.*
import org.objectweb.asm.*              // → ✅ org.apache.flink.shaded.asm9.org.objectweb.asm.*
import com.jayway.jsonpath.*            // → ✅ org.apache.flink.shaded.jsonpath.*
import org.codehaus.jettison.*          // → ✅ com.fasterxml.jackson.*

// ❌ 禁止的 mock 框架（不得 mock，采用可重用的 Testing 类替代）
import org.mockito.*;
import org.powermock.*;

// ❌ 禁止的 commons-lang（只能用 commons-lang3）
import org.apache.commons.lang.*

// ❌ 禁止的 testcontainers 内部包
import org.testcontainers.shaded.*

// ❌ 禁止的 JetBrains 注解（Checkstyle 强制）
import org.jetbrains.annotations.Nullable   // → ✅ import javax.annotation.Nullable
import org.jetbrains.annotations.NotNull    // → ✅ import javax.annotation.Nonnull
```

### 1.2 必须使用的 Flink 工具类

```java
// ❌ com.google.common.base.Preconditions           → ✅ org.apache.flink.util.Preconditions（静态导入）
// ❌ com.google.common.annotations.VisibleForTesting → ✅ org.apache.flink.annotation.VisibleForTesting
// ❌ org.apache.commons.lang(3).SerializationUtils  → ✅ org.apache.flink.util.InstantiationUtil
// ❌ org.apache.commons.lang3.Validate              → ✅ 使用 Flink Preconditions
// ❌ Throwables.propagate(...)                      → 已废弃，使用 ExceptionUtils
// ❌ Boolean.getBoolean("prop") / Integer.getInteger / Long.getLong
//    → ✅ Boolean.parseBoolean(System.getProperty("prop"))
```

常用 Flink 工具类（位于 `org.apache.flink.util.*`）：
- `Preconditions`、`IOUtils`、`InstantiationUtil`、`ExceptionUtils`、`NetUtils`、`FileUtils`、`StringUtils`、`CollectionUtil`、`OperatingSystem`

### 1.3 Java 版本兼容性

**构建 JDK：** Java **11、17 或 21**（Maven 要求 3.8.6+）。Flink 2.x 构建默认 JDK 17，使用 `-Pjava17-target`。

**源/目标字节码：**
- `source.java.version = 11`（POM 属性）
- `target.java.version = 17`（默认目标）
- 历史遗留的 Java 8 兼容性已在 Flink 2.x 上放弃，不必再回避 Java 9–11 API。

**但仍然需注意：**
- 对外公共 API（`@Public`、`@PublicEvolving`）更改需兼顾下游用户 JDK 版本。
- `flink-tests-java17/` 下有 JDK17-only 测试；通用模块请避免使用只在更高 JDK 才有的 API（如 Java 17 的 sealed classes）。
- `mockito-core` 当前为 5.x，仅对测试代码生效；生产代码仍然**禁止** mock。

### 1.4 测试断言（强制 AssertJ）

Flink 已在新代码上**统一使用 AssertJ**；JUnit 4 的 `Assert.*` 仍在历史代码中存在但**不得**在新代码中引入：

```java
// ❌ Assertions.assertEquals(expected, actual)       （JUnit 5）
// ❌ Assert.assertEquals(expected, actual)           （JUnit 4）
// ✅ assertThat(actual).isEqualTo(expected);

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

assertThat(list).hasSize(3).contains("a", "b");
assertThatThrownBy(() -> doSomething())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null");
```

**测试框架：** 新测试使用 **JUnit 5 (Jupiter)**；旧 JUnit 4 代码通过 `junit-vintage-engine` 共存，不要在新模块中继续引入 JUnit 4。

### 1.5 代码风格（Checkstyle + Spotless 强制）

- **禁止通配符导入**（`import java.util.*`）—— Checkstyle `AvoidStarImport`；IntelliJ 阈值设为 9999。
- **禁止行尾空格** —— 运行 `./mvnw spotless:apply`。
- **Java 风格数组声明：** `String[] args`，禁止 `String args[]`（Checkstyle `ArrayTypeStyle`）。
- **必须使用大括号：** `if (x) { doIt(); }`，即使是单行也要大括号（Checkstyle `NeedBraces`）。
- **禁止 `TODO(username)`、`FIXME`、`XXX`、`@author`** —— 使用不带用户名的 `TODO:`；历史追溯使用 git blame。
- **long 常量使用大写 `L`：** `100L` 而非 `100l`（Checkstyle `UpperEll`）。
- **空语句禁止：** 单独的 `;` 或连续多个 `;;`。
- **import 顺序（Spotless 强制）：** `org.apache.flink` → `org.apache.flink.shaded` → 空行 → 其他 → 空行 → `javax` → `java` → `scala` → 空行 → 静态 import。

### 1.6 命名规范（Checkstyle 强制）

| 元素 | 正则 / 约束 |
|------|--------|
| 包名 | `^[a-z]+(\.[a-z][a-z0-9]*)*$`（全小写，首段不能含数字）|
| 类型名 | `^[A-Z][a-zA-Z0-9]*$` |
| 非私有常量 | `^([A-Z][A-Z0-9]*(_[A-Z0-9]+)*|FLAG_.*)$` |
| 静态非 final 变量 | `^[a-z][a-zA-Z0-9]*_?$` |
| 普通成员/参数/局部变量 | `^[a-z][a-zA-Z0-9]*$` |
| 方法名 | `^[a-z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$` |

### 1.7 文档

- **文件大小上限：3100 行**（Checkstyle `FileLength`）。
- **protected 及以上**的类、接口、枚举、方法**必须**有 Javadoc（Checkstyle `JavadocType`、`JavadocMethod`）。
- Javadoc 段落必须正确使用 `<p>` 标签（`JavadocParagraph`）。
- **License 头：** 所有 `.java` 文件必须包含 Apache 2.0 许可证头，由 `apache-rat-plugin` 在 `validate` 阶段校验；Spotless 会在 `package` 关键字前自动补齐，模板见根 `pom.xml` 中的 `spotless.license.header`。

---

## 2. API 稳定性与设计模式

### 2.1 API 稳定性注解（`org.apache.flink.annotation.*`）

```java
@Public           // 稳定公共 API，主版本内不得破坏兼容（Connection-like 的顶层 API）
@PublicEvolving   // 面向用户但签名仍可能在小版本变化（新特性 / 新算子）
@Experimental     // 实验性；可能随时变化或移除
@Internal         // 内部实现；外部不得依赖
@VisibleForTesting // 仅为测试而放宽可见性
```

**使用原则：**
- **新增面向用户的 API** 默认标 `@PublicEvolving`；真正稳定后才能升为 `@Public`。
- **运行时、调度、RPC、执行计划** 等内部类必须标 `@Internal`。
- **不要** 从外部使用 `org.jetbrains.annotations.*`（Checkstyle 强制）；`@Nullable/@Nonnull` 使用 `javax.annotation.*`。

**参考：** `flink-annotations/src/main/java/org/apache/flink/annotation/`

### 2.2 Builder 模式（`ConfigOption` 为典型）

```java
public static final ConfigOption<Duration> CLIENT_TIMEOUT =
        ConfigOptions.key("client.timeout")
                .durationType()
                .defaultValue(Duration.ofSeconds(30))
                .withDescription("The client RPC timeout.");
```

**参考：** [flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java](flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java)

### 2.3 Factory 与 SPI

- **SPI：** 连接器/格式/文件系统通过 `META-INF/services/` 加载（`DynamicTableSourceFactory`、`FileSystemFactory`、`StateBackendFactory` 等）。
- **Factory 类：** 私有构造函数、静态工厂方法、返回接口类型。
- **典型例子：** `StreamExecutionEnvironment.getExecutionEnvironment()`、`TableEnvironment.create(...)`。

### 2.4 不可变/线程安全约定

- 数据对象尽量为 `final` 字段 + private 构造 + `equals/hashCode/toString`。
- 线程安全用 `@javax.annotation.concurrent.{ThreadSafe, NotThreadSafe, GuardedBy}` 文档化。

---

## 3. 代码组织与模块结构

Flink 是一个多模块的 Maven 聚合工程，根 `pom.xml` 声明 `<groupId>org.apache.flink</groupId>` 与 `<version>2.4-SNAPSHOT</version>`。

### 3.1 核心模块（顶层依赖链从下到上）

| 模块 | 用途 |
|---|---|
| [flink-annotations](flink-annotations/) | 稳定性注解（`@Public`、`@Internal` 等） |
| [flink-core-api](flink-core-api/) | 最小核心 API，供下游严格依赖 |
| [flink-core](flink-core/) | 类型系统、配置、`fs`、`util`、`io` 等基础设施 |
| [flink-rpc](flink-rpc/) | Flink RPC 抽象（基于 Pekko） |
| [flink-runtime](flink-runtime/) | JobManager、TaskManager、调度、Checkpoint、网络栈 |
| [flink-streaming-java](flink-streaming-java/) | DataStream API 与算子 |
| [flink-datastream](flink-datastream/) / [flink-datastream-api](flink-datastream-api/) | 新一代 DataStream v2 API |
| [flink-clients](flink-clients/) | 客户端提交器、CLI |
| [flink-optimizer](flink-optimizer/) | 批/执行计划优化器 |
| [flink-java](flink-java/) / [flink-scala](flink-scala/) | 经典 Java/Scala API（Scala 2.12 仅限特定模块） |

### 3.2 Table / SQL

- [flink-table](flink-table/)：Table API、SQL、Planner、Runtime、Catalog。

### 3.3 运行时支撑

- [flink-metrics](flink-metrics/)：指标系统与 Reporter。
- [flink-state-backends](flink-state-backends/)、[flink-dstl](flink-dstl/)：状态后端（含 changelog）。
- [flink-queryable-state](flink-queryable-state/)：可查询状态。
- [flink-filesystems](flink-filesystems/)：S3、OSS、GS、Azure、HDFS 等插件。
- [flink-runtime-web](flink-runtime-web/)：Flink Web UI。

### 3.4 部署 / 容器

- [flink-yarn](flink-yarn/)、[flink-kubernetes](flink-kubernetes/)、[flink-mesos](flink-mesos/)、[flink-container](flink-container/)、[flink-dist](flink-dist/)（二进制分发）。

### 3.5 连接器与格式

- [flink-connectors](flink-connectors/)：**保留在主仓库** 的连接器（base、files、datagen、hive 等）。
- [flink-formats](flink-formats/)：Avro、CSV、JSON、Parquet、ORC 等。
- **大多数连接器已外部化** 到独立仓库（见 [README.md](README.md) "Externalized Connectors"）：kafka、jdbc、elasticsearch、pulsar、mongodb、hbase、cassandra、aws 等。

### 3.6 支持与测试

- [flink-test-utils-parent](flink-test-utils-parent/)：`MiniClusterExtension`、`FlinkAssertions` 等。
- [flink-tests](flink-tests/) / [flink-tests-java17](flink-tests-java17/)：聚合集成测试。
- [flink-end-to-end-tests](flink-end-to-end-tests/)：端到端测试。
- [flink-architecture-tests](flink-architecture-tests/)：基于 ArchUnit 的架构合规测试。
- [flink-docs](flink-docs/)、[flink-python](flink-python/)、[flink-libraries](flink-libraries/)、[flink-walkthroughs](flink-walkthroughs/)、[flink-examples](flink-examples/)、[flink-quickstart](flink-quickstart/)、[flink-external-resources](flink-external-resources/)、[flink-contrib](flink-contrib/)、[flink-models](flink-models/)。

### 3.7 关键目录

```
flink/
├── flink-core/            # 基础数据类型、配置、util
├── flink-runtime/         # JM/TM 运行时
├── flink-streaming-java/  # DataStream API
├── flink-table/           # Table & SQL
├── flink-connectors/      # 保留的连接器
├── flink-formats/         # 数据格式
├── flink-dist/            # 发行包
│   └── src/main/flink-bin/bin/   # start-cluster.sh、jobmanager.sh、taskmanager.sh...
├── flink-architecture-tests/     # ArchUnit 规则与 violation 快照
├── tools/                 # checkstyle / spotless / CI 辅助
│   ├── maven/checkstyle.xml      # Checkstyle 规则源
│   └── ci/                       # CI Shell 与 Java 检查器
├── .github/workflows/     # GitHub Actions（ci.yml、nightly.yml 等）
├── azure-pipelines.yml    # Azure Pipelines（社区 PR 历史）
└── pom.xml                # 根 Maven POM
```

### 3.8 命名约定

| 类型 | 约定 | 示例 |
|---|---|---|
| 接口 | 简洁描述性名称 | `TypeInformation`、`KeySelector` |
| 实现类 | `Impl` 后缀 / 描述性名称 | `StreamExecutionEnvironmentImpl`、`DefaultExecutionGraph` |
| 抽象基类 | `Abstract` 前缀 | `AbstractStreamOperator` |
| 工具类 | `Utils`/`Util` 后缀；`final class` + 私有构造 | `NetUtils`、`IOUtils`、`CollectionUtil` |
| 单元测试 | `*Test.java` | `ConfigurationTest` |
| 集成测试 | `*ITCase.java`（默认 surefire `test.unit.pattern=**/*Test.*` 只跑 `*Test`） | `JobManagerHARecoveryITCase` |
| 测试辅助类 | `Testing*` 前缀（可重用的假实现）或 `*TestBase` | `TestingRpcService`、`MiniClusterTestBase` |
| 异常类 | `*Exception` 后缀 | `FlinkRuntimeException`、`JobExecutionException` |

### 3.9 类成员顺序

- **字段：** 静态常量 → 静态字段 → 实例字段。
- **方法：** 构造函数 → 静态工厂 → public → 包私有 → protected → private → 静态 util。
- **修饰符顺序：** `public protected private abstract static final transient volatile synchronized native strictfp`。

---

## 4. 错误处理

**异常层次：**
- [FlinkException](flink-core/src/main/java/org/apache/flink/util/FlinkException.java)（受检）—— 可预期的用户可见错误。
- [FlinkRuntimeException](flink-core/src/main/java/org/apache/flink/util/FlinkRuntimeException.java)（非受检）—— 运行时故障。
- 各子系统特化：`JobException`、`JobExecutionException`、`CheckpointException`、`SerializationException` 等。

**参数校验：** 在 API 边界使用 [Preconditions](flink-core/src/main/java/org/apache/flink/util/Preconditions.java)：

```java
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

checkNotNull(config, "config must not be null");
checkArgument(parallelism > 0, "parallelism must be positive, but was %s", parallelism);
```

**异常链与传播：**
- 使用 `org.apache.flink.util.ExceptionUtils` 处理异常链（`findThrowable`、`rethrow`、`stripCompletionException` 等）。
- **禁止** 使用 Guava 的 `Throwables.propagate(...)`（Checkstyle 强制）。
- `CompletableFuture` 场景使用 `FutureUtils`（`org.apache.flink.util.concurrent.FutureUtils`）。

---

## 5. 并发与线程安全

**注解：** `@javax.annotation.concurrent.{ThreadSafe, NotThreadSafe, GuardedBy}`。

**加锁：**
- 推荐使用显式锁对象 `private final Object lock = new Object();`。
- 跨线程读取的字段用 `volatile` 或 `java.util.concurrent.atomic.*`。
- `MailboxExecutor` 是 TaskManager 算子线程模型的核心；**算子内不得** 直接 `new Thread(...)`，请通过 mailbox 异步化。

**CompletableFuture：**
- 使用 `FutureUtils.combineAll/orTimeout/retry` 等工具。
- 不要在 `RpcEndpoint` 线程内 `.get()` 阻塞，使用 `.thenApply/.thenCompose`。

**资源管理：** 实现 `AutoCloseable`，使用 try-with-resources；`close()` 必须幂等。

---

## 6. 测试标准

### 6.1 框架与断言

- **JUnit 5 Jupiter**（新代码）+ **AssertJ**（强制）。参见 1.4 节。
- 保留 JUnit 4 的遗留模块通过 `junit-vintage-engine` 运行，但**不得**在新代码引入 JUnit 4。
- **禁用 Mockito/PowerMock**（Checkstyle 禁止导入）—— 使用可重用 `Testing*` 假实现。
- **超时：** 优先依赖全局 surefire 超时，不要在每个测试上 `@Timeout(...)`。

### 6.2 测试类命名与执行

- `*Test.java`：单元测试，由 surefire `test.unit.pattern=**/*Test.*` 执行。
- `*ITCase.java`：集成测试，走 failsafe 插件、`mvn verify` 阶段执行。
- 集成测试通常使用 `MiniCluster` 或 `TestContainers`。

### 6.3 常用测试扩展与基类

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

常用类：
- [MiniClusterExtension](flink-test-utils-parent/flink-test-utils/src/main/java/org/apache/flink/test/junit5/MiniClusterExtension.java)：启动 MiniCluster 并注入 `StreamExecutionEnvironment`。
- `InternalMiniClusterExtension`：`flink-runtime` 内部使用的 MiniCluster 扩展。
- `MiniClusterWithClientResource`（JUnit 4 旧接口）：新代码请用上面的 JUnit 5 扩展。
- `FlinkAssertions`：AssertJ 的 Flink 扩展断言。

### 6.4 架构测试（ArchUnit）

`flink-architecture-tests/` 模块使用 ArchUnit 冻结架构规则；当新代码引入新的违规时，**不要** 直接改 `violations` 存储文件，应优先修复违规。具体流程见 [flink-architecture-tests/README.md](flink-architecture-tests/README.md)。

---

## 7. 依赖与 Shading

### 7.1 flink-shaded

Flink 的 shaded 依赖由独立仓库 [flink-shaded](https://github.com/apache/flink-shaded) 提供，版本由根 `pom.xml` 属性 `flink.shaded.version`、`flink.shaded.jackson.version` 等控制。当前版本示例：

| shaded 坐标 | 源包前缀 |
|---|---|
| `org.apache.flink.shaded.guava33.com.google.common.*` | Guava |
| `org.apache.flink.shaded.jackson2.com.fasterxml.jackson.*` | Jackson |
| `org.apache.flink.shaded.netty4.io.netty.*` | Netty |
| `org.apache.flink.shaded.asm9.org.objectweb.asm.*` | ASM |
| `org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.*` | ZooKeeper |
| `org.apache.flink.shaded.jsonpath.*` | json-path |

**规则：** 源码必须引用 shaded 路径；`pom.xml` 直接依赖 `org.apache.flink:flink-shaded-*`，不要重复声明 `com.google.guava:guava` 等原始坐标。

### 7.2 依赖聚合与 `<optional>`

- 根 `pom.xml` 属性 `flink.markBundledAsOptional=true` 使被 shade 捆绑的依赖标为 `optional`，避免污染下游。
- 新增第三方依赖前：确认 License 合规（Apache 2.0 兼容），确保有必要性，并遵循 shade 策略。

---

## 8. 配置模式

### 8.1 ConfigOption 定义

```java
public static final ConfigOption<Duration> SLOT_REQUEST_TIMEOUT =
        ConfigOptions.key("slot.request.timeout")
                .durationType()
                .defaultValue(Duration.ofMinutes(5))
                .withDeprecatedKeys("slot.request.timeout.ms")
                .withDescription("Timeout for requesting slots from ResourceManager.");
```

**可用类型：** `stringType()`、`intType()`、`longType()`、`booleanType()`、`durationType()`、`memoryType()`、`enumType(X.class)`、`mapType()`、`stringType().asList()`、`noDefaultValue()`、`withDeprecatedKeys(...)`、`withFallbackKeys(...)`。

**参考：** [flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java](flink-core/src/main/java/org/apache/flink/configuration/ConfigOptions.java)

### 8.2 使用方式

```java
Configuration conf = new Configuration();
conf.set(CoreOptions.DEFAULT_PARALLELISM, 4);
int parallelism = conf.get(CoreOptions.DEFAULT_PARALLELISM);
Optional<Duration> timeout = conf.getOptional(SLOT_REQUEST_TIMEOUT);
```

### 8.3 命名约定

- 使用点分层级 + 连字符：`{category}.{sub}.{option-name}`，例如 `execution.checkpointing.interval`、`taskmanager.memory.process.size`。
- 文档化：为每个 ConfigOption 写清楚 `withDescription(...)`，文档生成模块 `flink-docs` 会自动扫描。

---

## 9. 序列化与 RPC

### 9.1 序列化

- **Flink 类型系统：** 对 POJO、Tuple、Row 等由 Flink `TypeInformation`/`TypeSerializer` 处理。**避免** 将 `Serializable` 滥用到运行时数据路径。
- **状态后端** 要求 `TypeSerializer` 配套 `TypeSerializerSnapshot` 以支持升级兼容。
- **禁止** 使用 commons-lang(3) 的 `SerializationUtils`（Checkstyle）。

### 9.2 RPC

- 底层实现已从 Akka 迁移到 **Pekko**；Maven 模块名仍保留历史命名 `flink-rpc-akka` / `flink-rpc-akka-loader`，Java 包路径为 `org.apache.flink.runtime.rpc.pekko`。
- **禁止直接依赖 `flink-rpc-akka`**（根 `pom.xml` 中 `forbid-direct-akka-rpc-dependencies` Enforcer 规则强制）。使用方式：
  - 生产代码：通过 `RpcSystem#load` 动态加载 RPC 系统。
  - 测试代码：使用 `TestingRpcService`。
- 所有 RPC 接口扩展 `RpcGateway`，实现扩展 `RpcEndpoint`。
- RPC 方法返回值使用 `CompletableFuture<...>`；入参需可序列化。
- 新增 RPC 接口必须标 `@Internal`。

---

## 10. 模块边界（ArchUnit 强制）

**核心链：** `flink-annotations` → `flink-core-api` → `flink-core` → `flink-rpc` → `flink-runtime` → `flink-streaming-java` / `flink-optimizer` → `flink-clients`。

**规则：**
1. **低层不依赖高层：** `flink-core` 严禁依赖 `flink-runtime`；`flink-runtime` 严禁依赖 `flink-streaming-java`。
2. **公共 API 边界：** 标注 `@Public`/`@PublicEvolving` 的类不得暴露 `@Internal` 类型到签名中（ArchUnit 有规则校验）。
3. **Scala 与 Java：** Scala 模块（`flink-scala`、`flink-streaming-scala`）属于可选桥接层；新 API 优先走 Java 侧，不要主动新增 Scala API。
4. **连接器/格式** 依赖 `flink-streaming-java` 与 `flink-table-*`，反向依赖被禁止。
5. 违反架构测试的改动：先尝试修复违规，**不要** 直接 refreeze 违规快照（见 6.4）。

---

## 11. 构建、CI 与部署

### 11.1 前置条件

- Unix 类系统（Linux / macOS / WSL）。
- **Maven 3.8.6+**（强烈建议使用 `./mvnw` 包装脚本）。
- **JDK 11 / 17 / 21**；默认推荐 17。

### 11.2 构建命令

```bash
# Java 17（默认）
./mvnw clean package -DskipTests -Djdk17 -Pjava17-target

# Java 11
./mvnw clean package -DskipTests -Djdk11 -Pjava11-target

# Java 21
./mvnw clean package -DskipTests -Djdk21 -Pjava21-target

# 并行构建（加速）
./mvnw clean install -DskipTests -T 1C

# 运行所有测试
./mvnw clean verify

# 指定模块测试
./mvnw verify -pl flink-runtime -am

# 运行单个测试
./mvnw test -Dtest=ConfigurationTest -pl flink-core
```

产物位于 `build-target/`（软链接到 `flink-dist/target/flink-<version>-bin/flink-<version>/`）。

### 11.3 代码格式化与静态检查

```bash
./mvnw spotless:apply        # 自动格式化 Java（AOSP）+ Scala（scalafmt）
./mvnw spotless:check        # 仅检查
./mvnw checkstyle:check -T1C # 检查 Checkstyle
./mvnw org.apache.rat:apache-rat-plugin:check -N   # License 头校验
```

- **google-java-format 1.24.0**（AOSP 风格）—— IntelliJ 插件**必须使用此版本且不要升级**（详见 [DEVELOPMENT.md](DEVELOPMENT.md)）。
- **Checkstyle 10.18.2**；IntelliJ 插件 `Checkstyle-IDEA`。
- **Scala 使用 scalafmt**（`.scalafmt.conf`）。

### 11.4 CI 流水线

**GitHub Actions**（`.github/workflows/`）：
- `ci.yml`：PR 与推送触发的主 CI（调用 `template.flink-ci.yml`）。
- `template.pre-compile-checks.yml`：Checkstyle、Spotless、License Headers，任意 JDK 都会跑。
- `nightly.yml` / `nightly-trigger.yml`：夜间全量测试。
- `docs.yml` / `docs-legacy.yml`：文档构建。

**Azure Pipelines**（`azure-pipelines.yml`）：个人 fork 可启用以加速测试。

**CI 强制检查：** Checkstyle → Spotless → License Headers → 编译 → 单元测试 → 集成测试 → 架构测试。任何一项失败均阻断合并。

### 11.5 文档

- 文档源码在 [docs/](docs/)（Hugo），运行 `docs/build_docs.sh` 本地预览。
- 配置项文档由 `flink-docs` 自动从 `ConfigOption` 生成。

---

## 12. Git 与 Pull Request 工作流

### 12.1 Fork 优先原则

**绝不直接推送到 `apache/flink` 上游。** 务必使用 fork：

```bash
git remote -v
# origin   https://github.com/apache/flink.git       (fetch/push 上游只读使用)
# fork     https://github.com/<your-username>/flink.git

# 如果缺失：
gh repo fork apache/flink --remote --remote-name fork
# 或
git remote add fork https://github.com/<your-username>/flink.git
```

### 12.2 JIRA 与分支

- **必须** 关联 JIRA [FLINK-XXXX](https://issues.apache.org/jira/projects/FLINK/issues)；仅文档 typo 可豁免。
- 分支命名约定：`FLINK-XXXX-<short-description>`。

### 12.3 提交信息格式

```
[FLINK-XXXX][component] 简要描述（≤ 72 字符）

详细说明（可选）。
```

- **component** 示例：`[runtime]`、`[table-planner]`、`[connectors/kafka]`、`[docs]`、`[build]`、`[tests]`、`[core]`、`[streaming]`、`[python]`、`[k8s]`。
- 纯 typo 可用 `[hotfix][docs] ...`，无需 JIRA。
- 每次 PR 尽量只对应一个 JIRA，不要混合多个无关问题。

### 12.4 推送前自查

```bash
./mvnw spotless:check
./mvnw checkstyle:check -T 1C
./mvnw org.apache.rat:apache-rat-plugin:check -N
./mvnw verify -pl <affected-modules>
git diff upstream/master...HEAD   # 主分支当前为 master
```

**禁止提交：** 密钥、凭证、本地配置、`target/` 产物、IDE 配置（除已在 `.idea/` 受控的共享配置）。

### 12.5 创建 Pull Request

```bash
git fetch upstream master
git rebase upstream/master
git push -u fork <branch-name>
gh pr create --web --title "[FLINK-XXXX][component] 简要标题"
```

**PR 描述请遵循 [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) 模板**，填齐：
- What is the purpose of the change
- Brief change log
- Verifying this change
- Does this pull request potentially affect ...（Dependencies / Public API / Serializers / per-record path / Deployment / S3 等）
- Documentation

---

## 13. AI 代理行为边界

### 13.1 行动前需请示

**需要审批的大规模改动：**
- 跨模块重构（涉及 ≥ 5 个 `flink-*` 模块）。
- 新增 / 升级第三方依赖、升级 flink-shaded。
- 修改 Checkstyle / Spotless / ArchUnit 规则。
- 修改 CI 工作流（`.github/workflows/`、`tools/ci/`）。
- 修改 `@Public` 公共 API 签名。
- 修改序列化器、Savepoint 格式、RPC 协议。
- 修改 `flink-architecture-tests` 下的违规快照文件。

### 13.2 未经明确许可，绝不执行

- ❌ 提交密钥、凭证、API key、token。
- ❌ 直接推送到 `apache/flink` 上游（始终使用 fork）。
- ❌ 对共享分支 `master` / `release-*` 强推。
- ❌ 执行破坏性 git 操作（`reset --hard`、`clean -fdx`、`branch -D`、`push --force` 到上游）。
- ❌ 修改自动生成的文件（如 Calcite/Pekko 生成代码、`flink-docs` 输出、`japicmp` 基线）。
- ❌ `git commit --no-verify` 跳过 hook。
- ❌ 为绕过 Checkstyle/Spotless/ArchUnit 失败而**禁用规则** 或往 `violations/` 里添加新快照。
- ❌ 未讨论兼容性/License 就引入新依赖。
- ❌ 引入 Mockito/PowerMock、Guava `Preconditions`、`Throwables.propagate`、通配符 import 等明确禁止项。

### 13.3 允许自主执行（在授权范围内）

- ✅ 读取仓库任何文件。
- ✅ 运行 `./mvnw` 测试/构建命令。
- ✅ 运行 `./mvnw spotless:apply` 自动格式化。
- ✅ 按第 1 节修复 Checkstyle 违规。
- ✅ 在自己的 fork 上创建特性分支。
- ✅ 提交并推送到自己的 fork，创建 PR。

### 13.4 提交前必需的验证清单

| 项目 | 命令 |
|---|---|
| 代码格式 | `./mvnw spotless:check` |
| Checkstyle | `./mvnw checkstyle:check -T 1C` |
| License 头 | `./mvnw org.apache.rat:apache-rat-plugin:check -N` |
| 受影响模块测试 | `./mvnw verify -pl <modules> -am` |
| 架构测试（涉及模块） | `./mvnw verify -pl flink-architecture-tests-production,flink-architecture-tests-test` |
| PR 关联 JIRA | 确认 FLINK-XXXX 存在且标题格式正确 |

**存疑时：** 在执行可能破坏公共 API、兼容性或架构的改动前，先与用户确认。
