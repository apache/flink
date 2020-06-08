---
title: "Savepoints"
nav-parent_id: ops_state
nav-pos: 8
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

* toc
{:toc}

## 什么是 Savepoint ？ Savepoint 与 Checkpoint 有什么不同？

Savepoint 是依据 Flink [checkpointing 机制]({{ site.baseurl }}/zh/training/fault_tolerance.html)所创建的流作业执行状态的一致镜像。 你可以使用 Savepoint 进行 Flink 作业的停止与重启、fork 或者更新。 Savepoint 由两部分组成：稳定存储（列入 HDFS，S3，...) 上包含二进制文件的目录（通常很大），和元数据文件（相对较小）。 稳定存储上的文件表示作业执行状态的数据镜像。 Savepoint 的元数据文件以（绝对路径）的形式包含（主要）指向作为 Savepoint 一部分的稳定存储上的所有文件的指针。

<div class="alert alert-warning">
<strong>注意:</strong> 为了允许程序和 Flink 版本之间的升级，请务必查看以下有关<a href="#分配算子-id">分配算子 ID </a>的部分 。
</div>
从概念上讲， Flink 的 Savepoint 与 Checkpoint 的不同之处类似于传统数据库中的备份与恢复日志之间的差异。 Checkpoint 的主要目的是为意外失败的作业提供恢复机制。 Checkpoint 的生命周期由 Flink 管理，即 Flink 创建，管理和删除 Checkpoint - 无需用户交互。 作为一种恢复和定期触发的方法，Checkpoint 实现有两个设计目标：i）轻量级创建和 ii）尽可能快地恢复。 可能会利用某些特定的属性来达到这个，例如， 工作代码在执行尝试之间不会改变。 在用户终止作业后，通常会删除 Checkpoint（除非明确配置为保留的 Checkpoint）。

 与此相反、Savepoint 由用户创建，拥有和删除。 他们的用例是计划的，手动备份和恢复。 例如，升级 Flink 版本，调整用户逻辑，改变并行度，以及进行红蓝部署等。 当然，Savepoint 必须在作业停止后继续存在。 从概念上讲，Savepoint 的生成，恢复成本可能更高一些，Savepoint 更多地关注可移植性和对前面提到的作业更改的支持。

除去这些概念上的差异，Checkpoint 和 Savepoint 的当前实现基本上使用相同的代码并生成相同的格式。然而，目前有一个例外，我们可能会在未来引入更多的差异。例外情况是使用 RocksDB 状态后端的增量 Checkpoint。他们使用了一些 RocksDB 内部格式，而不是 Flink 的本机 Savepoint 格式。这使他们成为了与 Savepoint 相比，更轻量级的 Checkpoint 机制的第一个实例。

## 分配算子 ID

**强烈建议**你按照本节所述调整你的程序，以便将来能够升级你的程序。主要通过 **`uid(String)`** 方法手动指定算子 ID 。这些 ID 将用于恢复每个算子的状态。

{% highlight java %}
DataStream<String> stream = env.
  // Stateful source (e.g. Kafka) with ID
  .addSource(new StatefulSource())
  .uid("source-id") // ID for the source operator
  .shuffle()
  // Stateful mapper with ID
  .map(new StatefulMapper())
  .uid("mapper-id") // ID for the mapper
  // Stateless printing sink
  .print(); // Auto-generated ID
{% endhighlight %}

如果不手动指定 ID ，则会自动生成 ID 。只要这些 ID 不变，就可以从 Savepoint 自动恢复。生成的 ID 取决于程序的结构，并且对程序更改很敏感。因此，强烈建议手动分配这些 ID 。

### Savepoint 状态

你可以将 Savepoint 想象为每个有状态的算子保存一个映射“算子 ID  ->状态”:

{% highlight plain %}
Operator ID | State
------------+------------------------
source-id   | State of StatefulSource
mapper-id   | State of StatefulMapper
{% endhighlight %}

在上面的示例中，print sink 是无状态的，因此不是 Savepoint 状态的一部分。默认情况下，我们尝试将 Savepoint 的每个条目映射回新程序。

## 算子

你可以使用[命令行客户端]({{site.baseurl}}/zh/ops/cli.html#Savepoint)来*触发 Savepoint*，*触发 Savepoint 并取消作业*，*从 Savepoint* 恢复，以及*删除 Savepoint*。

从 Flink 1.2.0 开始，还可以使用 webui *从 Savepoint 恢复*。

### 触发 Savepoint

当触发 Savepoint 时，将创建一个新的 Savepoint 目录，其中存储数据和元数据。可以通过[配置默认目标目录](#配置)或使用触发器命令指定自定义目标目录(参见[`:targetDirectory`参数](#触发-savepoint-1)来控制该目录的位置。

<div class="alert alert-warning">
<strong>注意:</strong>目标目录必须是 JobManager(s) 和 TaskManager(s) 都可以访问的位置，例如分布式文件系统上的位置。
</div>

以 `FsStateBackend`  或 `RocksDBStateBackend` 为例：

{% highlight shell %}
# Savepoint 目标目录
/savepoint/

# Savepoint 目录
/savepoint/savepoint-:shortjobid-:savepointid/

# Savepoint 文件包含 Checkpoint元数据
/savepoint/savepoint-:shortjobid-:savepointid/_metadata

# Savepoint 状态
/savepoint/savepoint-:shortjobid-:savepointid/...
{% endhighlight %}

<div class="alert alert-info">
  <strong>注意:</strong>
虽然看起来好像可以移动 Savepoint ，但由于 <code>_metadata</code> 中保存的是绝对路径，因此暂时不支持。
请按照<a href="https://issues.apache.org/jira/browse/FLINK-5778">FLINK-5778</a>了解取消此限制的进度。
</div>
请注意，如果使用 `MemoryStateBackend`，则元数据*和*  Savepoint 状态将存储在 `_metadata` 文件中。 由于它是自包含的，你可以移动文件并从任何位置恢复。

<div class="alert alert-warning">
  <strong>注意:</strong> 不建议移动或删除正在运行作业的最后一个 Savepoint ，因为这可能会干扰故障恢复。因此，Savepoint 对精确一次的接收器有副作用，为了确保精确一次的语义，如果在最后一个 Savepoint 之后没有 Checkpoint ，那么将使用 Savepoint 进行恢复。
</div>


#### 触发 Savepoint

{% highlight shell %}
$ bin/flink savepoint :jobId [:targetDirectory]
{% endhighlight %}

这将触发 ID 为 `:jobId` 的作业的 Savepoint，并返回创建的 Savepoint 路径。 你需要此路径来还原和删除 Savepoint 。

#### 使用 YARN 触发 Savepoint

{% highlight shell %}
$ bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId
{% endhighlight %}

这将触发 ID 为 `:jobId` 和 YARN 应用程序 ID `:yarnAppId` 的作业的 Savepoint，并返回创建的 Savepoint 的路径。

#### 使用 Savepoint 取消作业

{% highlight shell %}
$ bin/flink cancel -s [:targetDirectory] :jobId
{% endhighlight %}

这将自动触发 ID 为 `:jobid` 的作业的 Savepoint，并取消该作业。此外，你可以指定一个目标文件系统目录来存储 Savepoint 。该目录需要能被 JobManager(s) 和 TaskManager(s) 访问。

### 从 Savepoint 恢复

{% highlight shell %}
$ bin/flink run -s :savepointPath [:runArgs]
{% endhighlight %}

这将提交作业并指定要从中恢复的 Savepoint 。 你可以给出 Savepoint 目录或 `_metadata` 文件的路径。

#### 跳过无法映射的状态恢复

默认情况下，resume 操作将尝试将 Savepoint 的所有状态映射回你要还原的程序。 如果删除了运算符，则可以通过 `--allowNonRestoredState`（short：`-n`）选项跳过无法映射到新程序的状态：

{% highlight shell %}
$ bin/flink run -s :savepointPath -n [:runArgs]
{% endhighlight %}

### 删除 Savepoint

{% highlight shell %}
$ bin/flink savepoint -d :savepointPath
{% endhighlight %}

这将删除存储在 `:savepointPath` 中的 Savepoint。

请注意，还可以通过常规文件系统操作手动删除 Savepoint ，而不会影响其他 Savepoint 或 Checkpoint（请记住，每个 Savepoint 都是自包含的）。 在 Flink 1.2 之前，使用上面的 Savepoint 命令执行是一个更乏味的任务。

### 配置

你可以通过 `state.savepoint.dir` 配置 savepoint 的默认目录。 触发 savepoint 时，将使用此目录来存储 savepoint。 你可以通过使用触发器命令指定自定义目标目录来覆盖缺省值（请参阅[`:targetDirectory`参数](#触发-savepoint-1)）。


{% highlight yaml %}
# 默认 Savepoint 目标目录
state.savepoints.dir: hdfs:///flink/savepoints
{% endhighlight %}

如果既未配置缺省值也未指定自定义目标目录，则触发 Savepoint 将失败。

<div class="alert alert-warning">
<strong>注意:</strong>目标目录必须是 JobManager(s) 和 TaskManager(s) 可访问的位置，例如，分布式文件系统上的位置。
</div>


## F.A.Q

### 我应该为我作业中的所有算子分配 ID 吗?

根据经验，是的。 严格来说，仅通过 `uid` 方法给有状态算子分配 ID 就足够了。Savepoint 仅包含这些有状态算子的状态，无状态算子不是 Savepoint 的一部分。


在实践中，建议给所有算子分配 ID，因为 Flink 的一些内置算子（如 Window 算子）也是有状态的，而内置算子是否有状态并不很明显。 如果你完全确定算子是无状态的，则可以跳过 `uid` 方法。


### 如果我在作业中添加一个需要状态的新算子，会发生什么？

当你向作业添加新算子时，它将在没有任何状态的情况下进行初始化。 Savepoint 包含每个有状态算子的状态。 无状态算子根本不是 Savepoint 的一部分。 新算子的行为类似于无状态算子。

### 如果从作业中删除有状态的算子会发生什么?

默认情况下，从 Savepoint 恢复时将尝试将所有状态分配给新作业。如果有状态算子被删除，则无法从 Savepoint 恢复。


你可以通过使用 run 命令设置 `--allowNonRestoredState` (简称：`-n` )来允许删除有状态算子:

{% highlight shell %}
$ bin/flink run -s :savepointPath -n [:runArgs]
{% endhighlight %}

### 如果我在作业中重新排序有状态算子，会发生什么?

如果给这些算子分配了 ID，它们将像往常一样恢复。

如果没有分配 ID ，则有状态操作符自动生成的 ID 很可能在重新排序后发生更改。这将导致你无法从以前的 Savepoint 恢复。

### 如果我添加、删除或重新排序作业中没有状态的算子，会发生什么?

如果将 ID 分配给有状态操作符，则无状态操作符不会影响 Savepoint 恢复。

如果没有分配 ID ，则有状态操作符自动生成的 ID 很可能在重新排序后发生更改。这将导致你无法从以前的Savepoint 恢复。

### 当我在恢复时改变程序的并行度时会发生什么?

如果 Savepoint 是用 Flink >= 1.2.0 触发的，并且没有使用像 `Checkpointed` 这样的不推荐的状态API，那么你可以简单地从 Savepoint 恢复程序并指定新的并行度。

如果你正在从 Flink < 1.2.0 触发的 Savepoint 恢复，或者使用现在已经废弃的 api，那么你首先必须将作业和 Savepoint 迁移到 Flink >= 1.2.0，然后才能更改并行度。参见[升级作业和Flink版本指南]({{site.baseurl}}/zh/ops/upgrading.html)。

### 我可以将 savepoint 文件移动到稳定存储上吗?

这个问题的快速答案目前是“否”，因为元数据文件由于技术原因将稳定存储上的文件作为绝对路径引用。 更长的答案是：如果你因某种原因必须移动文件，那么有两个潜在的方法作为解决方法。 首先，更简单但可能更危险，你可以使用编辑器在元数据文件中查找旧路径并将其替换为新路径。 其次，你可以使用这个类 `SavepointV2Serializer`作为以新路径以编程方式读取，操作和重写元数据文件的起点。

{% top %}
