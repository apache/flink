---
title: 日志
weight: 4
type: docs
aliases:
  - /zh/deployment/advanced/logging.html
  - /zh/monitoring/logging.html
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

<a name="how-to-use-logging"></a>

# 如何使用日志记录

所有 Flink 进程都会创建一个文本格式的日志文件，其中包含该进程中发生的各种事件的信息。
这些日志提供了深入了解 Flink 内部工作的途径，同时可以用来输出检测出的问题（以 WARN/ERROR 消息的形式），还可以辅助调试问题。

日志文件可以通过 Job-/TaskManager 对应的 WebUI 页面访问。所使用的 [Resource Provider]({{< ref "docs/deployment/overview" >}})（如 YARN）可能会提供额外的访问方式来访问日志。

Flink 中的日志记录是使用 [SLF4J](http://www.slf4j.org/) 日志接口实现的。这允许你不需要修改 Flink 的源代码就可以使用任何支持 SLF4J 的日志框架。

默认情况下，使用 [Log4j 2](https://logging.apache.org/log4j/2.x/index.html) 作为底层日志框架。

<a name="configuring-log4j-2"></a>

## 配置 Log4j 2

Log4j 2 是通过 property 配置文件进行配置的。

Flink 发行版在 `conf` 目录中附带了以下 log4j 配置文件，如果启用了 Log4j 2，则会自动使用如下文件：

- `log4j-cli.properties`：Flink 命令行使用（例如 `flink run`）；
- `log4j-session.properties`：Flink 命令行在启动基于 Kubernetes/Yarn 的 Session 集群时使用（例如 `kubernetes-session.sh`/`yarn-session.sh`）；
- `log4j-console.properties`：Job-/TaskManagers 在前台模式运行时使用（例如 Kubernetes）；
- `log4j.properties`： Job-/TaskManagers 默认使用的日志配置。

Log4j 会定期扫描这些文件的变更，并在必要时调整日志记录行为。默认情况下30秒检查一次，监测间隔可以通过 Log4j 配置文件的 `monitorInterval` 配置项进行设置。

<a name="compatibility-with-log4j-1"></a>

### 与 Log4j 1 的兼容

Flink 附带了 [Log4j API bridge](https://logging.apache.org/log4j/log4j-2.2/log4j-1.2-api/index.html) 相关的依赖，使当前基于 Log4j1 开发的应用程序可以继续正常运行。

如果有基于 Log4j 1 的自定义配置文件或代码，请查看官方 Log4j [兼容](https://logging.apache.org/log4j/2.x/manual/compatibility.html)和[迁移](https://logging.apache.org/log4j/2.x/manual/migration.html)指南。

<a name="configuring-log4j1"></a>

## 配置 Log4j1

要将 Flink 与 [Log4j 1](https://logging.apache.org/log4j/1.2/) 一起使用，必须确保：
- Classpath 中不存在 `org.apache.logging.log4j:log4j-core`、`org.apache.logging.log4j:log4j-slf4j-impl` 和 `org.apache.logging.log4j:log4j-1.2-api`；
- 且 Classpath 中存在 `log4j:log4j`、`org.slf4j:slf4j-log4j12`、`org.apache.logging.log4j:log4j-to-slf4j` 和 `org.apache.logging.log4j:log4j-api`。

如果在 IDE 中使用 Log4j 1，则必须在 pom 文件中使用上述 Classpath 中应该存在的 jars 依赖项来替换 Classpath 中不应该存在的 jars 依赖项，并尽可能的排除那些传递依赖于 Classpath 中不存在 jars 的依赖项。

对于 Flink 发行版，这意味着你必须
- 从 `lib` 目录中移除 `log4j-core`，`log4j-slf4j-impl` 和 `log4j-1.2-api` jars；
- 往 `lib` 目录中添加 `log4j`，`slf4j-log4j12` 和 `log4j-to-slf4j` jars；
- 用适配的 Log4j1 版本替换 `conf` 目录中的所有 log4j 配置文件。

<a name="configuring-logback"></a>

## 配置 logback

要将 Flink 与 [logback](https://logback.qos.ch/) 一起使用，必须确保：
- Classpath 中不存在 `org.apache.logging.log4j:log4j-slf4j-impl`；
- Classpath 中存在 `ch.qos.logback:logback-core` 和 `ch.qos.logback:logback-classic`。

如果在 IDE 中使用 logback，则必须在 pom 文件中使用上述 Classpath 中应该存在的 jars 依赖项来替换 Classpath 中不应该存在的 jars 依赖项，并尽可能的排除那些传递依赖于 Classpath 中不存在 jars 的依赖项。

对于 Flink 发行版，这意味着你必须
- 从 `lib` 目录中移除 `log4j-slf4j-impl`  jars；
- 向 `lib` 目录中添加 `logback-core` 和 `logback-classic` jars。

Flink 发行版在 `conf` 目录中附带了以下 logback 配置文件，如果启用了 logback，则会自动使用这些文件：
- `logback-session.properties`: Flink 命令行在启动基于 Kubernetes/Yarn 的 Session 集群时使用（例如 `kubernetes-session.sh`/`yarn-session.sh`）；
- `logback-console.properties`：Job-/TaskManagers 在前台模式运行时使用（例如 Kubernetes）；
- `logback.xml`: 命令行和 Job-/TaskManager 默认使用的日志配置。

{{< hint warning >}}

Logback 1.3+ 需要 SLF4J 2，目前不支持。

{{< /hint >}}


<a name="best-practices-for-developers"></a>

## 开发人员的最佳实践

通过将相应 Class 的类型对象作为参数调用 `org.slf4j.LoggerFactory#LoggerFactory.getLogger` 方法可以创建一个 SLF4J 的 logger。

强烈建议将 logger 字段设置为 `private static final` 修饰的类型。

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Foobar {
	private static final Logger LOG = LoggerFactory.getLogger(Foobar.class);

	public static void main(String[] args) {
		LOG.info("Hello world!");
	}
}
```

为了最大限度地利用 SLF4J，建议使用其占位符机制。使用占位符可以在日志级别设置得太高而不会记录消息的情况下避免不必要的字符串构造。

占位符的语法如下：

```java
LOG.info("This message contains {} placeholders. {}", 2, "Yippie");
```

占位符也可以和要记录的异常一起使用。

```java
catch(Exception exception){
	LOG.error("An {} occurred.", "error", exception);
}
```

{{< top >}}
