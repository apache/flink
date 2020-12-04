---
title:  "扩展资源框架"
nav-parent_id: advanced
nav-pos: 1
nav-title: 扩展资源
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

许多计算任务需要使用除了 CPU 与内存外的资源，如用深度学习场景需要使用 GPU 来进行加速。为了支持这种扩展资源，Flink 提供了一个扩展资源框架。
该框架支持从底层资源管理系统（如 Kubernetes）请求各种类型的资源，并向算子提供使用这些资源所需的信息。该框架以插件形式支持不同的资源类型。
目前 Flink 仅内置了支持 GPU 资源的插件，你可以为你想使用的资源类型实现第三方插件。

* This will be replaced by the TOC
{:toc}

<a name="what-the-external-resource-framework-does"></a>

# 扩展资源框架做了什么

扩展资源（External Resource）框架主要做了以下两件事：

  - 根据你的配置，在 Flink 从底层资源管理系统中申请资源时，设置与扩展资源相关的请求字段

  - 为算子提供使用这些资源所需要的*信息*

当 Flink 部署在资源管理系统（Kubernetes、Yarn）上时，扩展资源框架将确保分配的 Pod、Container 包含所需的扩展资源。目前，许多资源管理系统都支持扩展资源。
例如，Kubernetes 从 v1.10 开始通过 [Device Plugin](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/) 机制支持 GPU、FPGA 等资源调度，Yarn 从 2.10 和 3.1 开始支持 GPU 和 FPGA 的调度。
目前，扩展资源框架并不支持 Mesos 模式。在 Standalone 模式下，由用户负责确保扩展资源的可用性。

扩展资源框架向算子提供扩展资源相关*信息*，这些信息由你配置的扩展资源 *Driver* 生成，包含了使用扩展资源所需要的基本属性。

<a name="enable-the-external-resource-framework-for-your-workload"></a>

# 启用扩展资源框架

为了启用扩展资源框架来使用扩展资源，你需要：

  - 为该扩展资源准备扩展资源框架的*插件*

  - 为该扩展资源设置相关的配置

  - 在你的算子中，从 `RuntimeContext` 来获取扩展资源的*信息*并使用这些资源

<a name="prepare-plugins"></a>

## 准备插件

你需要为使用的扩展资源准备插件，并将其放入 Flink 发行版的 `plugins/` 文件夹中, 参看 [Flink Plugins]({% link deployment/filesystems/plugins.zh.md %})。
Flink 提供了第一方的 [GPU 资源插件](#plugin-for-gpu-resources)。你同样可以为你所使用的扩展资源实现自定义插件[实现自定义插件](#implement-a-plugin-for-your-custom-resource-type)。

## 配置项

首先，你需要使用分隔符“;”将所有使用的扩展资源类型的资源名称添加到**扩展资源列表（配置键“external-resources”）**中，例如，“external-resources: gpu;fpga”定义了两个扩展资源“gpu”和“fpga”。
只有此处定义了扩展资源名称（**\<resource_name\>**），相应的资源才会在扩展资源框架中生效。

对于每个扩展资源，有以下配置选项。下面的所有配置选项中的 **\<resource_name\>** 对应于**扩展资源列表**中列出的名称：

  - **数量** （`external.<resource_name>.amount`）：需要从外部系统请求的扩展资源的数量。

  - **Yarn 中的扩展资源配置键** （`external-resource.<resource_name>.yarn.config-key`）：*可选配置*。如果配置该项，扩展资源框架将把这个键添加到 Yarn 的容器请求的资源配置中，该键对应的值将被设置为`external-resource.<resource_name>.amount`。

  - **Kubernetes 中的扩展资源配置键** （`external-resource.<resource_name>.kubernetes.config-key`）：*可选配置*。
  如果配置该项，扩展资源框架将添加 `resources.limits.<config-key>` 和 `resources.requests.<config-key>` 到 TaskManager 的主容器配置中，对应的值将被设置为 `external-resource.<resource_name>.amount`。

  - **Driver 工厂类** （`external-resource.<resource_name>.driver-factory.class`）：*可选配置*。定义由  **\<resource_name\>** 标识的扩展资源对应的工厂类名。如果配置该项，该工厂类将被用于实例化扩展资源框架中所需要的 *drivers*。
  如果没有配置，扩展资源依然会在其他配置正确时在存在于 `TaskManager`，只是算子在这种情况下无法从 `RuntimeContext` 中拿到该资源的信息。

  - **Driver 自定义参数** （`external-resource.<resource_name>.param.<param>`）：*可选配置*。由 **\<resource_name\>** 标识的扩展资源的自定义配置选项的命名模式。只有遵循此模式的配置才会传递到该扩展资源的工厂类。

示例配置，该配置定义两个扩展资源：

{% highlight bash %}
external-resources: gpu;fpga # 定义两个扩展资源，“gpu”和“fpga”。

external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory # 定义 GPU 资源对应 Driver 的工厂类。
external-resource.gpu.amount: 2 # 定义每个 TaskManager 所需的 GPU 数量。
external-resource.gpu.param.discovery-script.args: --enable-coordination # 自定义参数 discovery-script.args，它将被传递到 GPU 对应的 Driver 中。

external-resource.fpga.driver-factory.class: org.apache.flink.externalresource.fpga.FPGADriverFactory # 定义 FPGA 资源对应 Driver 的工厂类。
external-resource.fpga.amount: 1 # 定义每个 TaskManager 所需的 FPGA 数量。
external-resource.fpga.yarn.config-key: yarn.io/fpga # 定义 FPGA 在 Yarn 中对应的配置键。
{% endhighlight %}

## 使用扩展资源

为了使用扩展资源，算子需要从 `RuntimeContext` 获取 `ExternalResourceInfo` 集合。 `ExternalResourceInfo` 包含了使用扩展资源所需的信息，可以使用 `getProperty` 检索这些信息。
其中具体包含哪些属性以及如何使用这些属性取决于特定的扩展资源插件。

算子可以通过 `getExternalResourceInfos(String resourceName)` 从 `RuntimeContext` 或 `FunctionContext` 中获取特定扩展资源的 `ExternalResourceInfo`。
此处的 `resourceName` 应与在扩展资源列表中定义的名称相同。具体用法如下：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class ExternalResourceMapFunction extends RichMapFunction<String, String> {
    private static final String RESOURCE_NAME = "foo";

    @Override
    public String map(String value) {
        Set<ExternalResourceInfo> externalResourceInfos = getRuntimeContext().getExternalResourceInfos(RESOURCE_NAME);
        List<String> addresses = new ArrayList<>();
        externalResourceInfos.iterator().forEachRemaining(externalResourceInfo ->
            addresses.add(externalResourceInfo.getProperty("address").get()));
        // map function with addresses.
        // ...
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class ExternalResourceMapFunction extends RichMapFunction[(String, String)] {
    var RESOURCE_NAME = "foo"

    override def map(value: String): String = {
        val externalResourceInfos = getRuntimeContext().getExternalResourceInfos(RESOURCE_NAME)
        val addresses = new util.ArrayList[String]
        externalResourceInfos.asScala.foreach(
        externalResourceInfo => addresses.add(externalResourceInfo.getProperty("address").get()))

        // map function with addresses.
        // ...
    }
}
{% endhighlight %}
</div>
</div>

`ExternalResourceInfo` 中包含一个或多个键-值对，其键值表示资源的不同维度。你可以通过 `ExternalResourceInfo#getKeys` 获取所有的键。

<div class="alert alert-info">
     <strong>提示：</strong>目前，RuntimeContext#getExternalResourceInfos 返回的信息对所有算子都是可用的。
</div>

<a name="implement-a-plugin-for-your-custom-resource-type"></a>

# 为你所使用的扩展资源实现自定义插件

要为你所使用的扩展资源实现自定义插件，你需要：

  - 添加你自定义的扩展资源 Driver ，该 Driver 需要实现  `org.apache.flink.api.common.externalresource.ExternalResourceDriver` 接口。

  - 添加用来实例化 *Driver* 的工厂类，该工厂类需要实现 `org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory` 接口。

  - 添加服务入口。创建 `META-INF/services/org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory` 文件，其中包含了 *Driver* 对应工厂类的类名（更多细节请参看 [Java Service Loader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html)）。

例如，要为名为“FPGA”的扩展资源实现插件，你首先需要实现 `FPGADriver` 和 `FPGADriverFactory`：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class FPGADriver implements ExternalResourceDriver {
	@Override
	public Set<FPGAInfo> retrieveResourceInfo(long amount) {
		// return the information set of "FPGA"
	}
}

public class FPGADriverFactory implements ExternalResourceDriverFactory {
	@Override
	public ExternalResourceDriver createExternalResourceDriver(Configuration config) {
		return new FPGADriver();
	}
}

// Also implement FPGAInfo which contains basic properties of "FPGA" resource.
public class FPGAInfo implements ExternalResourceInfo {
	@Override
	public Optional<String> getProperty(String key) {
		// return the property with the given key.
	}

	@Override
	public Collection<String> getKeys() {
		// return all property keys.
	}
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class FPGADriver extends ExternalResourceDriver {
  override def retrieveResourceInfo(amount: Long): Set[FPGAInfo] = {
    // return the information set of "FPGA"
  }
}

class FPGADriverFactory extends ExternalResourceDriverFactory {
  override def createExternalResourceDriver(config: Configuration): ExternalResourceDriver = {
    new FPGADriver()
  }
}

// Also implement FPGAInfo which contains basic properties of "FPGA" resource.
class FPGAInfo extends ExternalResourceInfo {
  override def getProperty(key: String): Option[String] = {
    // return the property with the given key.
  }

  override def getKeys(): util.Collection[String] = {
    // return all property keys.
  }
}
{% endhighlight %}
</div>
</div>

在 `META-INF/services/` 中创建名为 `org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory` 的文件，向其中写入工厂类名，如 `your.domain.FPGADriverFactory`。

之后，将 `FPGADriver`，`FPGADriverFactory`，`META-INF/services/` 和所有外部依赖打入 jar 包。在你的 Flink 发行版的 `plugins/` 文件夹中创建一个名为“fpga”的文件夹，将打好的 jar 包放入其中。
更多细节请查看 [Flink Plugin]({% link deployment/filesystems/plugins.zh.md %})。

<div class="alert alert-info">
     <strong>提示：</strong> 扩展资源由运行在同一台机器上的所有算子共享。社区可能会在未来的版本中支持外部资源隔离。
</div>

# 已支持的扩展资源插件

目前，Flink提供 GPU 资源插件。

<a name="plugin-for-gpu-resources"></a>

## GPU 插件

我们为 GPU 提供了第一方插件。该插件利用一个脚本来发现 GPU 设备的索引，该索引可通过“index”从 `ExternalResourceInfo` 中获取。我们提供了一个默认脚本，可以用来发现 NVIDIA GPU。您还可以提供自定义脚本。

我们提供了[一个示例程序](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/gpu/MatrixVectorMul.java)，展示了如何在 Flink 中使用 GPU 资源来做矩阵-向量乘法。

<div class="alert alert-info">
     <strong>提示：</strong>目前，对于所有算子，RuntimeContext#getExternalResourceInfos 会返回同样的资源信息。也即，在同一个 TaskManager 中运行的所有算子都可以访问同一组 GPU 设备。扩展资源目前没有算子级别的隔离。
</div>

### 前置准备

要使 GPU 资源可访问，根据您的环境，需要满足以下先决条件：

  - 对于 Standalone 模式，集群管理员应确保已安装 NVIDIA 驱动程序，并且集群中所有节点上的 GPU 资源都是可访问的。

  - 对于 Yarn 上部署，管理员需要配置 Yarn 集群使其[支持 GPU 调度](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/UsingGpus.html)。
  请注意，所需的 Hadoop 版本是 2.10+ 和 3.1+。

  - 对于 Kubernetes 上部署，管理员需要保证 NVIDIA GPU 的 [Device Plugin](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/) 已在集群上安装。
  请注意，所需的 Kubernetes 版本是 1.10+。目前，Kubernetes只支持 NVIDIA GPU 和 AMD GPU。Flink 只提供了 NVIDIA GPU 的脚本，但你可以提供支持 AMD GPU 的自定义脚本，参看 [发现脚本](#discovery-script)。

### 在计算任务中使用 GPU 资源

如[启用扩展资源框架](#enable-the-external-resource-framework-for-your-workload)中所述，要使用 GPU 资源，还需要执行两项操作：

  - 为 GPU 资源进行相关配置。

  - 在算子中获取 GPU 资源的*信息*，其中包含键为“index”的 GPU 索引。

### 配置项

对于 GPU 插件，你需要指定的扩展资源框架配置：

  - `external-resources`：你需要将 GPU 的扩展资源名称（例如“gpu”）加到该列表中。

  - `external-resource.<resource_name>.amount`：每个 TaskManager 中的 GPU 数量。

  - `external-resource.<resource_name>.yarn.config-key`：对于 Yarn，GPU 的配置键是 `yarn.io/gpu`。请注意，Yarn 目前只支持 NVIDIA GPU。

  - `external-resource.<resource_name>.kubernetes.config-key`：对于 Kubernetes，GPU 的配置键是 `<vendor>.com/gpu`。
  目前，“nvidia”和“amd”是两个支持的 GPU 品牌。请注意，如果你使用 AMD GPU，你需要提供一个自定义的[发现脚本](#discovery-script)。

  - `external-resource.<resource_name>.driver-factory.class`：需要设置为 org.apache.flink.externalresource.gpu.GPUDriverFactory。

此外，GPU 插件还有一些专有配置：

  - `external-resource.<resource_name>.param.discovery-script.path`：[发现脚本](#discovery-script)的文件路径。
  它既可以是绝对路径，也可以是相对路径，如果定义了“FLINK_HOME”，该路径将相对于“FLINK_HOME”，否则相对于当前目录。如果没有显式配置该项，GPU 插件将使用默认脚本。

  - `external-resource.<resource_name>.param.discovery-script.args`：传递给发现脚本的参数。对于默认的发现脚本，请参见[默认脚本](#default-script)以获取可用参数。

GPU 插件示例配置：

{% highlight bash %}
external-resources: gpu
external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory # 定义 GPU 资源的工厂类。
external-resource.gpu.amount: 2 # 定义每个 TaskManager 的 GPU 数量。
external-resource.gpu.param.discovery-script.path: plugins/external-resource-gpu/nvidia-gpu-discovery.sh
external-resource.gpu.param.discovery-script.args: --enable-coordination # 自定义参数，将被传递到 GPU 的 Driver 中。

external-resource.gpu.yarn.config-key: yarn.io/gpu # for Yarn

external-resource.gpu.kubernetes.config-key: nvidia.com/gpu # for Kubernetes
{% endhighlight %}

<a name="discovery-script"></a>

### 发现脚本

`GPUDriver` 利用发现脚本来发现 GPU 资源并生成 GPU 资源信息。

<a name="default-script"></a>

#### 默认脚本

我们为 NVIDIA GPU 提供了一个默认脚本，位于 Flink 发行版的 `plugins/external-resource-gpu/nvidia-gpu-discovery.sh`。
该脚本通过 `nvidia-smi` 工具获取当前可见 GPU 的索引。它尝试返回一个 GPU 索引列表，其大小由 `external-resource.<resource_name>.amount` 指定，如果 GPU 数量不足，则以非零退出。

在 Standalone 模式中，多个 TaskManager 可能位于同一台机器上，并且每个 GPU 设备对所有 TaskManager 都是可见的。
默认脚本提供 GPU 协调模式，在这种模式下，脚本利用文件来同步 GPU 的分配情况，并确保每个GPU设备只能由一个TaskManager进程使用。相关参数为：

  - `--enable-coordination-mode`：启用 GPU 协调模式。默认情况下不启用。

  - `--coordination-file filePath`：用于同步 GPU 资源分配状态的文件路径。默认路径为 `/var/tmp/flink-gpu-coordination`。

<div class="alert alert-info">
     <strong>提示：</strong>协调模式只确保一个 GPU 设备不会被同一个 Flink 集群的多个 TaskManager 共享。不同 Flink 集群间（具有不同的协调文件）或非 Flink 应用程序仍然可以使用相同的 GPU 设备。
</div>

#### 自定义脚本

你可以提供一个自定义的发现脚本来满足你的特殊需求，例如使用 AMD GPU。请确保自定义脚本的的路径正确配置（`external-resource.<resource_name>.param.discovery-script.path`）并且 Flink 可以访问。自定义的发现脚本需要：

  - `GPUDriver` 将 GPU 数量（由 `external-resource.<resource_name>.amount` 定义）作为第一个参数传递到脚本中。
  `external-resource.<resource_name>.param.discovery-script.args` 中自定义的参数会被附加在后面。

  - 脚本需返回可用 GPU 索引的列表，用逗号分隔。空白的索引将被忽略。

  - 脚本可以通过以非零退出来表示其未正确执行。在这种情况下，算子将不会得到 GPU 资源相关信息。
