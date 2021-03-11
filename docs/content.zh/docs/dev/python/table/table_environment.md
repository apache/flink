---
title: "TableEnvironment"
weight: 28
type: docs
aliases:
  - /zh/dev/python/table-api-users-guide/table_environment.html
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

# TableEnvironment

本篇文档是对 PyFlink `TableEnvironment` 的介绍。 
文档包括对 `TableEnvironment` 类中每个公共接口的详细描述。



创建 TableEnvironment
-------------------------

创建 `TableEnvironment` 的推荐方式是通过 `EnvironmentSettings` 对象创建:

```python

from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
# or a batch TableEnvironment
# env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(env_settings)

```

或者，用户可以从现有的StreamExecutionEnvironment创建StreamTableEnvironment，以与DataStream API进行互操作。

```python

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, BatchTableEnvironment, TableConfig

# create a blink streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

```

TableEnvironment API
--------------------

### Table/SQL 操作

这些 APIs 用来创建或者删除 Table API/SQL 表和写查询：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>from_elements(elements, schema=None, verify_schema=True)</strong>
      </td>
      <td>
        通过元素集合来创建表。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_elements" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>from_pandas(pdf, schema=None, split_num=1)</strong>
      </td>
      <td>
        通过 pandas DataFrame 来创建表。 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_pandas" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>from_path(path)</strong>
      </td>
      <td>
        通过指定路径下已注册的表来创建一个表，例如通过 <strong>create_temporary_view</strong> 注册表。 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_path" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>sql_query(query)</strong>
      </td>
      <td>
        执行一条 SQL 查询，并将查询的结果作为一个 `Table` 对象。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.sql_query" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_temporary_view(view_path, table)</strong>
      </td>
      <td>
        将一个 `Table` 对象注册为一张临时表，类似于 SQL 的临时表。 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_view" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_view(view_path)</strong>
      </td>
      <td>
        删除指定路径下已注册的临时表。 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_view" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_table(table_path)</strong>
      </td>
      <td>
        删除指定路径下已注册的临时表。
        你可以使用这个接口来删除临时 source 表和临时 sink 表。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_table" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>execute_sql(stmt)</strong>
      </td>
      <td>
        执行指定的语句并返回执行结果。
        执行语句可以是 DDL/DML/DQL/SHOW/DESCRIBE/EXPLAIN/USE。 <br> <br>
        注意，对于 "INSERT INTO" 语句，这是一个异步操作，通常在向远程集群提交作业时才需要使用。
        但是，如果在本地集群或者 IDE 中执行作业时，你需要等待作业执行完成，这时你可以查阅 <a href="{{< ref "docs/dev/python/faq" >}}#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster">这里</a> 来获取更多细节。 <br>
        更多关于 SQL 语句的细节，可查阅 <a href="{{< ref "docs/dev/table/sql/overview" >}}">SQL</a> 文档。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.execute_sql" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

<big><strong>废弃的 APIs</strong></big>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>from_table_source(table_source)</strong>
      </td>
      <td>
        通过 table source 创建一张表。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_table_source" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>scan(*table_path)</strong>
      </td>
      <td>
        从 catalog 中扫描已注册的表并且返回结果表。
        它可以使用 <strong>from_path</strong> 来替换。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.scan" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_table(name, table)</strong>
      </td>
      <td>
        在 TableEnvironment 的 catalog 中用唯一名称注册一个 “Table” 对象。
        可以在 SQL 查询中引用已注册的表。
        它可以使用 <strong>create_temporary_view</strong> 替换。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_table" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_table_source(name, table_source)</strong>
      </td>
      <td>
        在 TableEnvironment 的 catalog 中注册一个外部 `TableSource`。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_table_source" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_table_sink(name, table_sink)</strong>
      </td>
      <td>
        在 TableEnvironment 的 catalog 中注册一个外部 `TableSink`。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_table_sink" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>insert_into(target_path, table)</strong>
      </td>
      <td>
        将 `Table` 对象的内容写到指定的 sink 表中。
        注意，这个接口不会触发作业的执行。
        你需要调用 `execute` 方法来执行你的作业。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.insert_into" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>sql_update(stmt)</strong>
      </td>
      <td>
        计算 INSERT, UPDATE 或者 DELETE 等 SQL 语句或者一个 DDL 语句。
        它可以使用 <strong>execute_sql</strong> 来替换。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.sql_update" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>connect(connector_descriptor)</strong>
      </td>
      <td>
        根据描述符创建临时表。 
        目前推荐的方式是使用 <strong>execute_sql</strong> 来注册临时表。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.connect" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

### 执行/解释作业

这些 APIs 是用来执行/解释作业。注意，`execute_sql` API 也可以用于执行作业。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>explain_sql(stmt, *extra_details)</strong>
      </td>
      <td>
       返回指定语句的抽象语法树和执行计划。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.explain_sql" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_statement_set()</strong>
      </td>
      <td>
        创建一个可接受 DML 语句或表的 StatementSet 实例。
        它可用于执行包含多个 sink 的作业。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_statement_set" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

<big><strong>废弃的 APIs</strong></big>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>explain(table=None, extended=False)</strong>
      </td>
      <td>
        返回指定 Table API 和 SQL 查询的抽象语法树，以及用来计算给定 `Table` 对象或者多个 sink 计划结果的执行计划。
        如果你使用 "insert_into" 或者 "sql_update" 方法将数据发送到多个 sinks，你可以通过这个方法来得到执行计划。
        它也可以用 <strong>TableEnvironment.explain_sql</strong>，<strong>Table.explain</strong> 或者 <strong>StatementSet.explain</strong> 来替换。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.explain" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>execute(job_name)</strong>
      </td>
      <td>
        触发程序执行。执行环境将执行程序的所有部分。
        如果你想要使用 <strong>insert_into</strong> 或者 <strong>sql_update</strong> 方法将数据发送到结果表，你可以使用这个方法触发程序的执行。
        这个方法将阻塞客户端程序，直到任务完成/取消/失败。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.execute" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

### 创建/删除用户自定义函数

这些 APIs 用来注册 UDFs 或者 删除已注册的 UDFs。
注意，`execute_sql` API 也可以用于注册/删除 UDFs。
关于不同类型 UDFs 的详细信息，可查阅 [用户自定义函数]({{< ref "docs/dev/table/functions/overview" >}})。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>create_temporary_function(path, function)</strong>
      </td>
      <td>
        将一个 Python 用户自定义函数注册为临时 catalog 函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_temporary_system_function(name, function)</strong>
      </td>
      <td>
        将一个 Python 用户自定义函数注册为临时系统函数。
        如果临时系统函数的名称与临时 catalog 函数名称相同，优先使用临时系统函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_system_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_java_function(path, function_class_name, ignore_if_exists=None)</strong>
      </td>
      <td>
        将 Java 用户自定义函数注册为指定路径下的 catalog 函数。
        如果 catalog 是持久化的，则可以跨多个 Flink 会话和集群使用已注册的 catalog 函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_java_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_java_temporary_function(path, function_class_name)</strong>
      </td>
      <td>
        将 Java 用户自定义函数注册为临时 catalog 函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_java_temporary_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_java_temporary_system_function(name, function_class_name)</strong>
      </td>
      <td>
        将 Java 用户定义的函数注册为临时系统函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_java_temporary_system_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_function(path)</strong>
      </td>
      <td>
        删除指定路径下已注册的 catalog 函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_function(path)</strong>
      </td>
      <td>
        删除指定名称下已注册的临时系统函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_system_function(name)</strong>
      </td>
      <td>
        删除指定名称下已注册的临时系统函数。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_system_function" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

<big><strong>废弃的 APIs</strong></big>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>register_function(name, function)</strong>
      </td>
      <td>
        注册一个 Python 用户自定义函数，并为其指定一个唯一的名称。 
        若已有与该名称相同的用户自定义函数，则替换之。
        它可以通过 <strong>create_temporary_system_function</strong> 来替换。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_function" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_java_function(name, function_class_name)</strong>
      </td>
      <td>
        注册一个 Java 用户自定义函数，并为其指定一个唯一的名称。 
        若已有与该名称相同的用户自定义函数，则替换之。
        它可以通过 <strong>create_java_temporary_system_function</strong> 来替换。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_java_function" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

### 依赖管理

这些 APIs 用来管理 Python UDFs 所需要的 Python 依赖。
更多细节可查阅 [依赖管理]({{< ref "docs/dev/python/table/dependency_management" >}}#python-dependency-in-python-program)。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>add_python_file(file_path)</strong>
      </td>
      <td>
        添加 Python 依赖，可以是 Python 文件，Python 包或者本地目录。 
        它们将会被添加到 Python UDF 工作程序的 PYTHONPATH 中。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.add_python_file" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>set_python_requirements(requirements_file_path, requirements_cache_dir=None)</strong>
      </td>
      <td>
        指定一个 requirements.txt 文件，该文件定义了第三方依赖关系。
        这些依赖项将安装到一个临时 catalog 中，并添加到 Python UDF 工作程序的 PYTHONPATH 中。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.set_python_requirements" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>add_python_archive(archive_path, target_dir=None)</strong>
      </td>
      <td>
        添加 Python 归档文件。该文件将被解压到 Python UDF 程序的工作目录中。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.add_python_archive" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

### 配置

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>get_config()</strong>
      </td>
      <td>
        返回 table config，可以通过 table config 来定义 Table API 的运行时行为。
        你可以在 <a href="{{< ref "docs/deployment/config" >}}">配置</a> 和
        <a href="{{< ref "docs/dev/python/python_config" >}}">Python 配置</a> 中找到所有可用的配置选项。 <br> <br>
        下面的代码示例展示了如何通过这个 API 来设置配置选项：
```python
# set the parallelism to 8
table_env.get_config().get_configuration().set_string(
    "parallelism.default", "8")
```
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_config" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

### Catalog APIs

这些 APIs 用于访问 catalog 和模块。你可以在 [模块]({{< ref "docs/dev/table/modules" >}}) 和 [catalog]({{< ref "docs/dev/table/catalogs" >}}) 文档中找到更详细的介绍。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">描述</th>
      <th class="text-center" style="width: 10%">文档</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>register_catalog(catalog_name, catalog)</strong>
      </td>
      <td>
        注册具有唯一名称的 `Catalog`。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_catalog" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>get_catalog(catalog_name)</strong>
      </td>
      <td>
        通过指定的名称来获得已注册的 `Catalog` 。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_catalog" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>use_catalog(catalog_name)</strong>
      </td>
      <td>
        将当前目录设置为所指定的 catalog。
        它也将默认数据库设置为所指定 catalog 的默认数据库。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.use_catalog" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>get_current_catalog()</strong>
      </td>
      <td>
        获取当前会话默认的 catalog 名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_current_catalog" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>get_current_database()</strong>
      </td>
      <td>
        获取正在运行会话中的当前默认数据库名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_current_database" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>use_database(database_name)</strong>
      </td>
      <td>
        设置当前默认的数据库。
        它必须存在当前 catalog 中。
        当寻找未限定的对象名称时，该路径将被用作默认路径。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.use_database" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>load_module(module_name, module)</strong>
      </td>
      <td>
        加载给定名称的 `Module`。
        模块将按照加载的顺序进行保存。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.load_module" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>unload_module(module_name)</strong>
      </td>
      <td>
        卸载给定名称的 `Module`。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.unload_module" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_catalogs()</strong>
      </td>
      <td>
        获取在这个环境中注册的所有 catalog 目录名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_catalogs" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_modules()</strong>
      </td>
      <td>
        获取在这个环境中注册的所有模块名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_modules" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_databases()</strong>
      </td>
      <td>
        获取当前 catalog 中所有数据库的名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_databases" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_tables()</strong>
      </td>
      <td>
        获取当前 catalog 的当前数据库下的所有表和临时表的名称。
        它可以返回永久和临时的表和视图。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_tables" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_views()</strong>
      </td>
      <td>
        获取当前 catalog 的当前数据库中的所有临时表名称。
        它既可以返回永久的也可以返回临时的临时表。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_views" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_user_defined_functions()</strong>
      </td>
      <td>
        获取在该环境中已注册的所有用户自定义函数的名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_user_defined_functions" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_functions()</strong>
      </td>
      <td>
        获取该环境中所有函数的名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_functions" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_temporary_tables()</strong>
      </td>
      <td>
       获取当前命名空间（当前 catalog 的当前数据库）中所有可用的表和临时表名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_temporary_tables" name="链接">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_temporary_views()</strong>
      </td>
      <td>
        获取当前命名空间（当前 catalog 的当前数据库）中所有可用的临时表名称。
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_temporary_views" name="链接">}}
      </td>
    </tr>
  </tbody>
</table>

Statebackend，Checkpoint 以及重启策略
---------------------------------------------

在 Flink 1.10 之前，你可以通过 `StreamExecutionEnvironment` 来配置 statebackend，checkpointing 以及重启策略。
现在你可以通过在 `TableConfig` 中，通过设置键值选项来配置它们，更多详情可查阅 [容错]({{< ref "docs/deployment/config" >}}#fault-tolerance)，[State Backends]({{< ref "docs/deployment/config" >}}#checkpoints-and-state-backends) 以及 [Checkpointing]({{< ref "docs/deployment/config" >}}#checkpointing)。

下面代码示例展示了如何通过 Table API 来配置 statebackend，checkpoint 以及重启策略：
```python
# 设置重启策略为 "fixed-delay"
table_env.get_config().get_configuration().set_string("restart-strategy", "fixed-delay")
table_env.get_config().get_configuration().set_string("restart-strategy.fixed-delay.attempts", "3")
table_env.get_config().get_configuration().set_string("restart-strategy.fixed-delay.delay", "30s")

# 设置 checkpoint 模式为 EXACTLY_ONCE
table_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "3min")

# 设置 statebackend 类型为 "rocksdb"，其他可选项有 "filesystem" 和 "jobmanager"
# 你也可以将这个属性设置为 StateBackendFactory 的完整类名
# e.g. org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory
table_env.get_config().get_configuration().set_string("state.backend", "rocksdb")

# 设置 RocksDB statebackend 所需要的 checkpoint 目录
table_env.get_config().get_configuration().set_string("state.checkpoints.dir", "file:///tmp/checkpoints/")
```
