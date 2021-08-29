---
title: 命令行界面
weight: 5
type: docs
aliases:
  - /zh/deployment/cli.html
  - /zh/apis/cli.html
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

<a name="Command-Line Interface"></a>
# 命令行界面

Flink提供了命令行界面（CLI）`bin/flink` 来运行 JAR 格式的程序，同时控制其执行。该 CLI 作为 Flink 安装配置的一部分，在单节点或分布式安装的方式中都可以使用。命令行程序与运行中的 JobManager 建立连接来通信，JobManager 的连接信息可以通过`conf/flink-conf.yaml`指定。

<a name="job-lifecycle-management"></a>

## 作业生命周期管理

本节所列命令可以工作的前提条件是要有一个正在运行的 Flink 环境，其部署方式可以有多种，例如 [Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})，[YARN]({{< ref "docs/deployment/resource-providers/yarn" >}}) 或任何其它可用的方式。欢迎[在本地启动一个Flink集群]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}#starting-a-standalone-cluster-session-mode) ，然后在本机体验这些命令。

<a name="submitting-a-job"></a>

### 提交作业

提交作业就是将作业的 JAR 文件和相关依赖上传到 Flink 集群，然后开始执行作业。如下所示，我们选择了一个长期运行的作业 `examples/streaming/StateMachineExample.jar`。你可以自由选择 `examples/` 中的JAR文件，也可以选择自己开发的作业。

```bash
$ ./bin/flink run \
      --detached \
      ./examples/streaming/StateMachineExample.jar
```
使用 `--detached` 参数提交作业将使 `bin/flink` 命令在作业提交完成后退出并输出返回结果。输出信息如下所示，除其它信息外，输出结果包括了新提交作业的 ID。

```txt
Usage with built-in data generator: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms>]
Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]
Options for both the above setups:
        [--backend <file|rocks>]
        [--checkpoint-dir <filepath>]
        [--async-checkpoints <true|false>]
        [--incremental-checkpoints <true|false>]
        [--output <filepath> OR null for stdout]

Using standalone source with error rate 0.000000 and sleep delay 1 millis

Job has been submitted with JobID cca7bc1061d61cf15238e92312c2fc20
```
`usage` 对应job的使用信息，展示了可根据需要添加到作业提交命令末尾的作业相关参数。为了便于阅读，我们假设返回的 JobID 存储在变量 JOB_ID 中，用于以下命令设置：

```bash
$ export JOB_ID="cca7bc1061d61cf15238e92312c2fc20"
```

此外还有一个提交作业的操作叫做 `run-application` ，它可以设置 [Application 模式]({{< ref "docs/deployment/overview" >}}#application-mode) 来运行作业。由于它在 CLI 前端的工作原理与 `run` 操作类似，本文档将不再单独讨论。


`run` 和 `run-application` 操作支持通过 `-D` 选项传递额外的配置参数。例如，可以通过设置 `-Dpipeline.max-parallelism=120` 来设置作业的[最大并行度]({{< ref "docs/deployment/config#pipeline-max-parallelism" >}}#application-mode) 。该参数对于配置 per-job 或 application 的作业模式非常有用，因为可以在不改变配置文件的情况下给集群传递任意配置参数。

当向现有集群提交 session 模式的作业时，仅支持[执行相关配置参数]({{< ref "docs/deployment/config#execution" >}}) 。

<a name="job-monitoring"></a>

### 作业监控

通过 `list` 操作，可以监控正在运行的作业：

```bash
$ ./bin/flink list
```
```
Waiting for response...
------------------ Running/Restarting Jobs -------------------
30.11.2020 16:02:29 : cca7bc1061d61cf15238e92312c2fc20 : State machine job (RUNNING)
--------------------------------------------------------------
No scheduled jobs.
```
已提交但尚未启动的作业会展示在 "Scheduled Jobs" 下。


<a name="creating-a-savepoint"></a>

### 创建 Savepoint

可以创建 [Savepoints]({{< ref "docs/ops/state/savepoints" >}})  来保存作业所处的当前状态。创建 Savepoint 仅需提供 JobID:

```bash
$ ./bin/flink savepoint \
      $JOB_ID \ 
      /tmp/flink-savepoints
```
```
Triggering savepoint for job cca7bc1061d61cf15238e92312c2fc20.
Waiting for response...
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
You can resume your program from this savepoint with the run command.
```
savepoint 的保存目录是可选参数，如果 [state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir) 未设置，则必须指定。

savepoint 对应的存储路径可在稍后用于[重新启动 Flink 作业](#starting-a-job-from-a-savepoint)。

<a name="disposing-a-savepoint"></a>

#### 废弃 Savepoint

`savepoint` 操作可以用于删除 savepoint 。可以使用 `--dispose` 参数删除 savepoint，需要后面添加对应的 savepoint 路径：

```bash
$ ./bin/flink savepoint \ 
      --dispose \
      /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \ 
      $JOB_ID
```
```
Disposing savepoint '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab'.
Waiting for response...
Savepoint '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab' disposed.
```

如果使用自定义状态实例（例如自定义 reducing 状态或 RocksDB 状态），则必须指定触发 savepoint 的  JAR 程序路径。否则，会遇到 `ClassNotFoundException` 异常 ：

```bash
$ ./bin/flink savepoint \
      --dispose <savepointPath> \ 
      --jarfile <jarFile>
```

通过 `savepoint` 操作触发 savepoint 废弃，不仅会将数据从存储中删除，还会使 Flink 清理与 savepoint 相关的元数据。

<a name="terminating-a-savepoint"></a>

### 终止作业

<a name="stopping-a-job-gracefully-creating-a-final-savepoint"></a>

#### 优雅地终止作业并创建最终 Savepoint

终止作业运行的操作是 `stop`。`stop` 操作停止从 source 到 sink 的作业流，是一种更优雅的终止方式。当用户请求终止一项作业时，所有的 sources 将被要求发送最后的 checkpoint 屏障，这会触发创建 checkpoint ，在成功完成 checkpoint 的创建后，Flink 会调用 `cancel()` 方法来终止作业。

```bash
$ ./bin/flink stop \
      --savepointPath /tmp/flink-savepoints \
      $JOB_ID
```
```
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
如果 [state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir) 未设置，必须使用 `--savepointPath` 参数来指定 savepoint 的存储目录。

如果指定了 `--drain` 标志，那么在最后一个 checkpoint 屏障之前会发出 `MAX_WATERMARK`。这将触发所有基于事件时间注册的定时器，从而清除等待特定 watermark 的所有 state，例如窗口。作业会一直运行，直到所有的 source 被正确的关闭。该机制允许作业处理完所有正在接收的数据，因此在终止作业过程中，创建 savepoint 之后依然可能会产生一些待处理的记录。



{{< hint danger >}}

如果想永久终止作业，可以使用 `--drain` 标记。如果想在稍后的时间点恢复作业，那么不要使用  `--drain` ，因为这可能导致作业恢复时出现不正确的结果。
{{< /hint >}}

<a name="cancelling-a-job-ungracefully"></a>

#### 非优雅地取消作业

可以通过 `cancel` 操作取消作业：

```bash
$ ./bin/flink cancel $JOB_ID
```
```
Cancelling job cca7bc1061d61cf15238e92312c2fc20.
Cancelled job cca7bc1061d61cf15238e92312c2fc20.
```
相应作业的状态将从 Running 转换为 Cancelled，任何计算都将停止。

{{< hint danger >}}
`--withSavepoint` 标志会将创建 savepoint 作为作业取消过程的一部分。此功能已弃用。请改用  [stop](#stopping-a-job-gracefully-creating-a-final-savepoint) 操作。
{{< /hint >}}

<a name="starting-a-job-from-a-savepoint"></a>

### 从 Savepoint 启动作业

可以使用 `run`  (和  `run-application`) 操作来从 savepoint 启动作业。

```bash
$ ./bin/flink run \
      --detached \ 
      --fromSavepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./examples/streaming/StateMachineExample.jar
```
```
Usage with built-in data generator: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms>]
Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]
Options for both the above setups:
        [--backend <file|rocks>]
        [--checkpoint-dir <filepath>]
        [--async-checkpoints <true|false>]
        [--incremental-checkpoints <true|false>]
        [--output <filepath> OR null for stdout]

Using standalone source with error rate 0.000000 and sleep delay 1 millis

Job has been submitted with JobID 97b20a0a8ffd5c1d656328b0cd6436a6
```

请注意，该命令除了使用 `-fromSavepoint` 参数关联[之前停止作业](#stopping-a-job-gracefully-creating-a-final-savepoint)的状态外，其它参数都与[初始 run 命令](#submitting-a-job)相同。该操作会生成一个新的 JobID，用于维护作业的运行。


默认情况下，Flink 尝试将新提交的作业恢复到完整的 savepoint 状态。如果你想忽略不能随新作业恢复的 savepoint 状态，可以设置 `--allowNonRestoredState` 标志。当你删除了程序的某个操作，同时该操作是创建 savepoint 时对应程序的一部分，这种情况下，如果你仍想使用 savepoint，就需要设置此参数。

```bash
$ ./bin/flink run \
      --fromSavepoint <savepointPath> \
      --allowNonRestoredState ...
```
如果你的程序删除了相应 savepoint 的部分运算操作，使用该选项将很有帮助。

{{< top >}}

<a name="cli-actions"></a>

## CLI 操作

以下是 Flink CLI 工具支持操作的概览：

<table class="table table-bordered">
    <thead>
        <tr>
          <th class="text-left" style="width: 25%">操作</th>
          <th class="text-left" style="width: 50%">目的</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code class="highlighter-rouge">run</code></td>
            <td>
                该操作用于执行作业。不过需要包含作业所需 jar 包。如有必要，可以传递与 Flink 或作业相关的参数。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">run-application</code></td>
            <td>
                该操作用于在 <a href="{{< ref "docs/deployment/overview" >}}#application-mode">Application 模式</a>下执行作业。除此之外，它与 <code class="highlighter-rouge">run</code> 操作的参数相同。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">info</code></td>
            <td>
                该操作用于打印作业相关的优化执行图。同样需要指定包含作业的 jar。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">list</code></td>
            <td>
                该操作用于列出所有正在运行或调度中的作业。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">savepoint</code></td>
            <td>
                该操作用于为指定的作业创建或废弃 savepoint。如果在 <code class="highlighter-rouge">conf/flink-conf.yaml</code> 中没有指定 <a href="{{< ref "docs/deployment/config" >}}#state-savepoints-dir">state.savepoints.dir</a> 参数，那么除了指定 JobID 之外还需要指定 savepoint 目录。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">cancel</code></td>
            <td>
                该操作用于根据作业 JobID 取消正在运行的作业。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">stop</code></td>
            <td>
                该操作结合了 <code class="highlighter-rouge">cancel</code> 和 <code class="highlighter-rouge">savepoint</code> 的功能，停止运行作业的同时会创建用于恢复作业的 savepoint 。
            </td>
        </tr>
    </tbody>
</table>


可以通过 `bin/flink --help` 查看所有支持的操作以及操作相关参数的详细信息，也可以通过 `bin/flink <action> --help` 单独查看指定操作的使用信息。

{{< top >}}

<a name="advanced-cli"></a>

## 高级的 CLI

<a name="rest-api"></a>

### REST API

Flink 集群也可以使用 [REST API]({{< ref "docs/ops/rest_api" >}}) 进行管理。前面章节描述的命令是 Flink  REST 服务端支持命令的子集。

因此，可以使用 `curl`  之类的工具来进一步发挥 Flink 的作用。

<a name="selecting-deployment-targets"></a>

### 选择部署方式

Flink 兼容多种集群管理框架，例如 [Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}) 和 [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}})，在 Resource Provider 章节有更详细的描述。可以在不同的 [Deployment Modes]({{< ref "docs/deployment/overview" >}}#deployment-modes) 下提交作业。作业提交相关的参数化因底层框架和部署模式的不同而不同。

`bin/flink` 提供了`--target` 参数来设置不同的选项。除此之外，仍然必须使用  `run`（针对 [Session]({{< ref "docs/deployment/overview" >}}#session-mode) 和 [Per-Job Mode]({{< ref "docs/deployment/overview" >}}#per-job-mode)）或 `run-application` （针对 [Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode)）提交作业。

下面的参数组合的总结：

* YARN
  * `./bin/flink run --target yarn-session`: 将作业以 `Session` 模式提交到 YARN 集群上运行。
  * `./bin/flink run --target yarn-per-job`: 将作业以 `Per-Job` 模式提交，会基于 YARN 集群新启动一个对应 Flink Job。
  * `./bin/flink run-application --target yarn-application`: 将作业以 `yarn-application` 模式提交，会基于 YARN 集群新启动一个对应 Flink Job。
* Kubernetes
  * `./bin/flink run --target kubernetes-session`: 将作业以 `Session` 模式提交到 Kubernetes 集群上运行。
  * `./bin/flink run-application --target kubernetes-application`: 将作业以 `yarn-application` 模式提交，会基于 Kubernetes 集群新启动一个对应 Flink Job。
* Standalone:
  * `./bin/flink run --target local`: 将作业以 `Session` 模式提交到最小集群模式部署的本地 Flink。
  * `./bin/flink run --target remote`: 将作业提交到运行中的 Flink 集群。

使用  `--target` 参数可以覆盖配置文件 `conf/flink-conf.yaml` 中的  [execution.target]({{< ref "docs/deployment/config" >}}#execution-target) 配置。

关于命令和相关选项的更多细节，请参考文档中关于Resource Provider的指南。

<a name="submitting-pyFlink-jobs"></a>

### 提交 PyFlink 作业

目前，用户可以通过 CLI 提交 PyFlink 作业。与提交 Java 作业不同的是这种方式不需要指定 JAR 文件的路径或完整的 main 类名称。

{{< hint info >}}
当使用 `flink run` 提交 Python 作业时，Flink 会运行 "python" 命令。请运行以下命令以确认当前环境中的 python 是 Python 3.6 以上的版本。
{{< /hint >}}

```bash
$ python --version
# 这里打印的版本必须是3.6以上
```

如下命令展示了不同方式启动 PyFlink 作业的示例：

- 启动一个 PyFlink 作业：
```bash
$ ./bin/flink run --python examples/python/table/batch/word_count.py
```

- 启动一个指定了额外文件源和资源文件的 PyFlink 作业。通过 `--pyFiles` 参数指定的文件必须包含在 `PYTHONPATH`  中，只有这样文件才能在 Python 代码中访问。
```bash
$ ./bin/flink run \
      --python examples/python/table/batch/word_count.py \
      --pyFiles file:///user.txt,hdfs:///$namenode_address/username.txt
```

- 启动一个引用了 Java UDF 或 external connectors 的 PyFlink 作业。通过 `--jarfile` 参数指定的 JAR 文件会被上传到集群中。
```bash
$ ./bin/flink run \
      --python examples/python/table/batch/word_count.py \
      --jarfile <jarFile>
```

- 通过 `pyFiles` 参数启动一个 PyFlink 作业，同时通过 `--pyModule` 参数指定主入口模块：
```bash
$ ./bin/flink run \
      --pyModule batch.word_count \
      --pyFiles examples/python/table/batch
```

- 通过 `--jobmanager` 参数将 PyFlink  作业提交到特定主机的 JobManager ，主机地址通过 `<jobmanagerHost>` （根据具体情况调整命令参数值）设置：
```bash
$ ./bin/flink run \
      --jobmanager <jobmanagerHost>:8081 \
      --python examples/python/table/batch/word_count.py
```

- [在 YARN 集群上以 Per-Job 模式]({{< ref "docs/deployment/resource-providers/yarn" >}}#per-job-cluster-mode)启动 PyFlink 作业：
```bash
$ ./bin/flink run \
      --target yarn-per-job
      --python examples/python/table/batch/word_count.py
```

- 在原生的 Kubernetes 集群上运行 PyFlink 应用需要指定集群 ID `<ClusterId>`，还需要指定安装了 PyFlink 的 docker 镜像，镜像相关信息可以参考[在 Docker 中启用 PyFlink]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#enabling-python)：
```bash
$ ./bin/flink run-application \
      --target kubernetes-application \
      --parallelism 8 \
      -Dkubernetes.cluster-id=<ClusterId> \
      -Dtaskmanager.memory.process.size=4096m \
      -Dkubernetes.taskmanager.cpu=2 \
      -Dtaskmanager.numberOfTaskSlots=4 \
      -Dkubernetes.container.image=<PyFlinkImageName> \
      --pyModule word_count \
      --pyFiles /opt/flink/examples/python/table/batch/word_count.py
```

想了解更多可用选项，请参阅 Resource Provider 章节，那里有 [Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})
和 [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}}) 的更详细描述。

除了上面提到的 `--pyFiles`、`--pyModule` 和 `--python`，还有一些选项与 Python 相关的选项。以下是 Python 相关选项的概览，这些选项针对的是 Flink  CLI 工具支持 `run` 操作和 `run-application` 操作。

<table class="table table-bordered">
    <thead>
        <tr>
          <th class="text-left" style="width: 25%">选项</th>
          <th class="text-left" style="width: 50%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code class="highlighter-rouge">-py,--python</code></td>
            <td>
                用于指定带有程序入口的 Python 脚本。可以使用 <code class="highlighter-rouge">--pyFiles</code> 选项配置依赖的资源。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pym,--pyModule</code></td>
            <td>
                用于指定带有程序入口点的 Python 模块。该选项必须与 <code class="highlighter-rouge">--pyFiles</code> 一起使用。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyfs,--pyFiles</code></td>
            <td>
                用于为作业指定自定义的文件资源。.py、.egg、.zip、.whl 和目录等常见后缀的文件资源都是支持的。这些文件将被添加到本地客户端和远程 python UDF worker 的 PYTHONPATH 中。以 .zip 为后缀的文件将被提取并添加到 PYTHONPATH。逗号 (',') 可以用作分隔符来指定多个文件（例如，--pyFiles file:///tmp/myresource.zip,hdfs:///$namenode_address/myresource2.zip）。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyarch,--pyArchives</code></td>
            <td>
              用于为作业配置 python 归档文件。归档文件会被提取到 python UDF worker 的工作目录。对于每个归档文件，都可以指定一个目标目录。如果指定了目标目录，那么归档文件会被提取到相应名字的目标目录。否则，归档文件将被提取到与归档文件同名的目录中。通过此选项上传的文件可以通过相对路径访问。'#' 字符用作归档文件路径和目标目录名称之间分隔符。 逗号 (',') 用作分隔符来指定多个归档文件。此选项可以将 Python UDF 中使用的数据文件上传到虚拟环境（例如，--pyArchives file:///tmp/py37.zip,file:///tmp/data.zip#data --pyExecutable py37.zip/py37/bin/python）。可以在 Python UDF 中直接访问数据文件，例如：f = open('data/data.txt', 'r')。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyclientexec,--pyClientExecutable</code></td>
            <td>
                用于指定 Python 解释器的路径。当通过 <code class="highligher-rouge">flink run</code> 提交 Python 作业或编译包含 Python UDF 的 Java/Scala 作业时，使用该路径上的 Python 解释器启动 Python 进程。(示例，--pyArchives file:///tmp/py37.zip --pyClientExecutable py37.zip/py37/python)
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyexec,--pyExecutable</code></td>
            <td>
                用于指定执行 python UDF worker 的 python 解释器的路径（例如：--pyExecutable /usr/local/bin/python3）。python UDF worker 依赖于 Python 3.6+、Apache Beam（版本 == 2.27.0）、Pip（版本 >= 7.1.0）和 SetupTools（版本 >= 37.0.0）。请确保指定的环境符合上述要求。
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyreq,--pyRequirements</code></td>
            <td>
                用于指定定义第三方依赖项的 requirements.txt 文件。这些依赖会被安装并被添加到 python UDF worker 对应的 PYTHONPATH中。可以选择性地指定包含这些依赖项的安装包目录。如果可选参数存在，请使用'#'作为分隔符（例如，--pyRequirements file:///tmp/requirements.txt#file:///tmp/cached_dir）。
            </td>
        </tr>
    </tbody>
</table>

除了提交作业时的命令行选项，Flink 还支持通过配置文件或在代码中调 Python API 来配置依赖项。更多详细信息，请参阅[依赖项管理]({{< ref "docs/dev/python/dependency_management" >}}) 。

{{< top >}}
