---
title: "模块"
is_beta: true
nav-parent_id: tableapi
nav-pos: 70
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
模块让用户能够对 Flink 内置对象进行扩展。例如定义一些类似 Flink 内置函数的函数。模块是可插拔的，Flink 已经提供了一些预构建的模块，用户还可以实现自己的模块。

例如，用户可以自定义地理位置函数，并将其作为内置函数插入 Flink，随后就可以在 Flink SQL 和 Table API 中使用它了。另一个示例是，用户可以加载现成的 Hive 模块，随后就可以像使用 Flink 内置函数一样使用 Hive 模块的函数了。

* This will be replaced by the TOC
{:toc}

## 模块类型

### 核心模块

`核心模块` 包括 Flink 所有的系统（内置）函数，默认加载。

### Hive 模块

 `Hive模块` 为 Flink SQL 和 Table API 的用户提供了 Hive 内置函数作为 Flink 的系统函数，它们类似于 Flink 内置函数。 [Hive 文档]({{ site.baseurl }}/zh/dev/table/hive/hive_functions.html) 提供了设置该模块的详细方法。

### 用户自定义模块

用户可以通过实现 `Module` 接口来开发自定义模块。为了能在 SQL CLI 使用，用户还需要通过实现 `ModuleFactory` 接口来为对应的模块开发一个模块工厂。

模块工厂定义了一组属性，用于在 SQL CLI 启动时配置模块。属性会传递给发现服务，发现服务会为属性匹配一个 `ModuleFactory` 并创建对应的模块实例。 


## 命名空间和解析顺序

模块提供的对象被认为是 Flink 系统（内置）对象的一部分。它们没有命名空间。

当两个模块中有同名对象时，Flink 会选择最早完成加载的模块中的对象。

## 模块 API

### 模块的加载与卸载

用户可以在 Flink 会话中加载与卸载模块。

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.loadModule("myModule", new CustomModule());
tableEnv.unloadModule("myModule");
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">
使用YAML定义的模块必须设置`类型`属性来指定模块类型。原生支持以下模块类型。


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-center" style="width: 25%">模块种类</th>
      <th class="text-center">类型值</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td class="text-center">核心模块</td>
        <td class="text-center">core</td>
    </tr>
    <tr>
        <td class="text-center">Hive 模块</td>
        <td class="text-center">hive</td>
    </tr>
  </tbody>
</table>

{% highlight yaml %}
modules:

   - name: core
     type: core
   - name: myhive
     type: hive
{% endhighlight %}
</div>
</div>


### 列出可用模块

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.listModules();
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}
Flink SQL> SHOW MODULES;
{% endhighlight %}
</div>
</div>
