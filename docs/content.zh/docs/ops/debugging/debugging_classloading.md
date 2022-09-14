---
title: "调试类加载"
weight: 3
type: docs
aliases:
  - /zh/ops/debugging/debugging_classloading.html
  - /zh/monitoring/debugging_classloading.html
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

# 调试类加载

## Flink中的类加载概述

Flink应用程序运行时，JVM会随着时间不断加载各种不同的类。
根据起源不同这些类可以分为三组类型：

  - **Java Classpath**: Java共有的classpath类加载路径，包括JDK库和Flink的`/lib`目录（Apache Flink及相关依赖的类）中的代码。它们通过*AppClassLoader*进行加载。

  - **Flink插件类组件**：存放于Flink的 `/plugins` 目录中的插件代码。Flink的插件机制确保在启动时对它们进行动态加载。

  - **动态用户代码**：动态提交job（通过REST、命令行或者web UI方式）时存在JAR文件中的类。job运行时它们通过*FlinkUserCodeClassLoader*进行动态加载或卸载。

作为通用规则，每当Flink进程先启动，之后job提交时，job相关的类都是动态加载的。
如果Flink进程与job或应用程序一起启动，或者应用代码启动Flink组件（JobManager, TaskManager等），这时所有job的类存在于Java的classpath下。

每个插件中的组件代码会由一个专用的类加载器进行动态加载。

下面是不同部署模式的一些细节信息：

**Session模式(Standalone/Yarn/Kubernetes)**

当Flink Session集群启动时，JobManager和TaskManager由Java classpath中的Flink框架类（Flink framework classes）进行启动加载。而通过session提交（REST或命令行方式）的job或应用程序由*FlinkUserCodeClassLoader*进行加载。

<!--
**Docker Containers with Flink-as-a-Library**

If you package a Flink job/application such that your application treats Flink like a library (JobManager/TaskManager daemons as spawned as needed),
then typically all classes are in the *application classpath*. This is the recommended way for container-based setups where the container is specifically
created for an job/application and will contain the job/application's jar files.

-->

**Per-Job模式（已弃用）（Yarn）**

当前只有Yarn支持Per-Job模式。默认情况下，Flink集群运行在Per-Job模式下时会将用户的jar文件包含在系统的classpath中。
这种模式可以由[yarn.classpath.include-user-jar]({{< ref "docs/deployment/config" >}}#yarn-classpath-include-user-jar) 参数控制。
当该参数设定为`DISABLED`时，Flink会将用户jar文件含在用户的classpath中，并由*FlinkUserCodeClassLoader*进行动态加载。

详细信息参见[Flink on Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}})。

**Application模式（Standalone/Yarn/Kubernetes）**

当Application模式的Flink集群基于Standalone或Kubernetes方式运行时，用户jar文件（启动命令指定的jar文件和Flink的`usrlib`目录中的jar包）会由*FlinkUserCodeClassLoader*进行动态加载。

当Flink集群以Application模式运行时，用户jar文件（启动命令指定的jar文件和Flink的`usrlib`目录中的jar包）默认情况下会包含在系统classpath（*AppClassLoader*）。与Per-Job模式相同，当[yarn.classpath.include-user-jar]({{< ref "docs/deployment/config" >}}#yarn-classpath-include-user-jar)设置为`DISABLED`时，Flink会将用户jar文件含在用户的classpath中，并由*FlinkUserCodeClassLoader*进行动态加载。


## 倒置类加载（Inverted Class Loading）和ClassLoader解析顺序

涉及到动态类加载的层次结构涉及两种ClassLoader：
（1）Java的*application classloader*，包含classpath中的所有类；
（2）动态的*plugin/user code classloader*，用来加载插件代码或用户代码的jar文件。动态的ClassLoader将应用程序classloader作为parent。

默认情况下Flink会倒置类加载顺序，首先Flink会查找动态类加载器，如果该类不属于动态加载的代码时才会去查找其parent（application classloader）。

倒置类加载的好处在于插件和job可以使用与Flink核心不同的库版本，尤其在使用不同版本的库从而出现不兼容的情况下。这种机制可以帮助避免常见的类似 `IllegalAccessError` 或`NoSuchMethodError`的依赖冲突错误。代码的不同部分会有独立的拷贝（Flink内核及它的不同依赖包可使用与用户代码或插件代码不同的拷贝），多数情况下这种方式可以正常运行，并且不需要用户进行额外配置。

然而有些情况下，倒置类加载可能会引起一些问题，参见下面的["X cannot be cast to X"](#x-cannot-be-cast-to-x-exceptions)。

对于用户代码的类加载，您可以通过调整Flink的[`classloader.resolve-order`]({{< ref "docs/deployment/config" >}}#classloader-resolve-order)配置将ClassLoader解析顺序还原至Java的默认模式（从Flink默认的`child-first`调整为`parent-first`）。

请注意由于有些类在Flink内核与插件或用户代码间共享，它们总是以*parent-first*方式进行解析的。这些类相关的包通过[`classloader.parent-first-patterns-default`]({{< ref "docs/deployment/config" >}}#classloader-parent-first-patterns-default)和[`classloader.parent-first-patterns-additional`]({{< ref "docs/deployment/config" >}}#classloader-parent-first-patterns-additional)进行配置。如果需要新添加*parent-first* 方式的包，请调整`classloader.parent-first-patterns-additional` 配置选项。


## 避免用户代码的动态类加载

Flink的组件（JobManager, TaskManager, Client, ApplicationMaster等）在启动时会在日志开头的环境信息部分记录classpath的设定。


当JobManager和TaskManager的运行模式为指定一个job时，可以通过将用户代码的JAR文件放置在`/lib`目录下，从而包含在classpath路径中，以保证它们不会被动态加载。


通常情况下将job的JAR文件放置在`/lib`目录下可以正常运行。JAR文件会同时作为classpath（*AppClassLoader*）和动态类加载器（*FlinkUserCodeClassLoader*）的一部分。
由于AppClassLoader是FlinkUserCodeClassLoader的父类（Java默认情况下以parent-first方式加载），这样类只会加载一次。

当job相关的JAR文件不能全部放在`/lib`目录下（如多个job共用的一个session）时，可以通过将相对公共的类库放在`/lib`目录下，从而避免这些类的动态加载。


## 手动进行用户代码的类加载

某些情况下，transformation、source或者sink需要进行手动类加载（通过反射动态实现），这需要通过能访问到job相关类的类加载器进行实现。

在这种情况下，可以把函数（或sources和sinks）实现为`RichFunction`（如`RichMapFunction` 或者 `RichWindowFunction`），然后通过`getRuntimeContext().getUserCodeClassLoader()`访问用户代码的类加载器。


## X cannot be cast to X 异常

当进行动态类加载时，您可能会遇到类似`com.foo.X cannot be cast to com.foo.X`类型的异常。
出现这种异常代表不同的类加载器加载了不同版本的`com.foo.X`类，并且它们互相之间尝试进行类型指定转换。

发生这种情况的通常原因是这个库与Flink的*倒置类加载*（*inverted classloading*）方式不兼容造成的。您可以通过关闭倒置类加载（inverted classloading）来进行验证（在Flink设置中调整[`classloader.resolve-order: parent-first`]({{< ref "docs/deployment/config" >}}#classloader-resolve-order)），或者将库排除在inverted classloading之外（通过设置[`classloader.parent-first-patterns-additional`]({{< ref "docs/deployment/config" >}}#classloader-parent-first-patterns-additional)）。

另一种原因可能是由缓存的对象实例引起的，比如类似*Apache Avro*或者Guava的Interners类型的对象。
解决办法是设置没有任何动态类加载，或者确保相应的库完全是动态加载代码的一部分。后者意味着库不能添加到Flink的`/lib`目录下，但必须作为应用程序的fat-jar或uber-jar的一部分。


## 卸载用户代码中动态加载的类

所有涉及动态用户代码类加载（会话）的场景都依赖于再次*卸载*的类。


类卸载指垃圾回收器发现一个类的对象不再被引用，这时会对该类（相关代码、静态变量、元数据等）进行移除。

当TaskManager启动或重启任务时会加载指定任务的代码，除非这些类可以卸载，否则就有可能引起内存泄露，因为更新新版本的类可能会随着时间不断的被加载积累。这种现象经常会引起**OutOfMemoryError: Metaspace**这种典型异常。

类泄漏的常见原因和建议的修复方式：

  - *Lingering Threads*: 确保应用代码的函数/sources/sink关闭了所有线程。延迟关闭的线程不仅自身消耗资源，同时会因为占据对象引用，从而阻止垃圾回收和类的卸载。

  - *Interners*: 避免缓存超出function/sources/sinks生命周期的特殊结构中的对象。比如Guava的Interner，或是Avro的序列化器中的类或对象。

  - *JDBC*: JDBC驱动会在用户类加载器之外泄漏引用。为了确保这些类只被加载一次，您可以将驱动JAR包放在Flink的`lib/`目录下，或者将驱动类通过[`classloader.parent-first-patterns-additional`]({{< ref "docs/deployment/config" >}}#classloader-parent-first-patterns-additional)加到父级优先加载类的列表中。

释放用户代码类加载器的钩子（hook）可以帮助卸载动态加载的类。这种钩子在类加载器卸载前执行。通常情况下最好把关闭和卸载资源作为正常函数生命周期操作的一部分（比如典型的`close()`方法）。有些情况下（比如静态字段）最好确定类加载器不再需要后就立即卸载。


释放类加载器的钩子可以通过`RuntimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent()`方法进行注册。

## 通过maven-shade-plugin解决与Flink的依赖冲突

从应用开发者的角度可以通过*shading them away*的方式公开依赖关系来解决依赖冲突。

Apache Maven提供了[maven-shade-plugin](https://maven.apache.org/plugins/maven-shade-plugin/)，通过插件可以允许在编译*后*调整类相关的包。举例来说，假如您的用户代码jar文件中包含aws的sdk中的`com.amazonaws`包，shade plugin会将它们重定位到`org.myorg.shaded.com.amazonaws`，这样代码就会正确调用您的aws sdk的版本。

这个文档页面解释了[relocating classes using the shade plugin](https://maven.apache.org/plugins/maven-shade-plugin/examples/class-relocation.html)。

对于大部分的Flink依赖如`guava`, `netty`, `jackson`等，这些已经由Flink的维护者进行处理，普通用户通常情况下无需再对其进行关注。

{{< top >}}
