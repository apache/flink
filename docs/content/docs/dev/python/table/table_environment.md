---
title: "TableEnvironment"
weight: 28
type: docs
aliases:
  - /dev/python/table-api-users-guide/table_environment.html
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

This document is an introduction of PyFlink `TableEnvironment`. 
It includes detailed descriptions of every public interface of the `TableEnvironment` class.

Create a TableEnvironment
-------------------------

The recommended way to create a `TableEnvironment` is to create from an `EnvironmentSettings` object:

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()

# or a batch TableEnvironment
# env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)
```

Alternatively, users can create a `StreamTableEnvironment` from an existing `StreamExecutionEnvironment` to interoperate with the DataStream API.

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# create a streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
```

TableEnvironment API
--------------------

### Table/SQL Operations

These APIs are used to create/remove Table API/SQL Tables and write queries:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>from_elements(elements, schema=None, verify_schema=True)</strong>
      </td>
      <td>
        Creates a table from a collection of elements. 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_elements" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>from_pandas(pdf, schema=None, split_num=1)</strong>
      </td>
      <td>
        Creates a table from a pandas DataFrame. 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_pandas" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>from_path(path)</strong>
      </td>
      <td>
        Creates a table from a registered table under the specified path, e.g. tables registered via <strong>create_temporary_view</strong>.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_path" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>sql_query(query)</strong>
      </td>
      <td>
        Evaluates a SQL query and retrieves the result as a `Table` object. 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.sql_query" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_temporary_view(view_path, table)</strong>
      </td>
      <td>
        Registers a `Table` object as a temporary view similar to SQL temporary views. 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_view" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_view(view_path)</strong>
      </td>
      <td>
        Drops a temporary view registered under the given path. 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_view" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_table(table_path)</strong>
      </td>
      <td>
        Drops a temporary table registered under the given path. 
        You can use this interface to drop the temporary source table and temporary sink table.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_table" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>execute_sql(stmt)</strong>
      </td>
      <td>
        Executes the given single statement and returns the execution result.
        The statement can be DDL/DML/DQL/SHOW/DESCRIBE/EXPLAIN/USE. <br> <br>
        Note that for "INSERT INTO" statement this is an asynchronous operation, which is usually expected when submitting a job to a remote cluster.
        However, when executing a job in a mini cluster or IDE, you need to wait until the job execution finished, then you can refer to <a href="{{< ref "docs/dev/python/faq" >}}#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster">here</a> for more details. <br>
        Please refer the <a href="{{< ref "docs/dev/table/sql/overview" >}}">SQL</a> documentation for more details about SQL statement.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.execute_sql" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

<big><strong>Deprecated APIs</strong></big>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>from_table_source(table_source)</strong>
      </td>
      <td>
        Creates a table from a table source. 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.from_table_source" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>scan(*table_path)</strong>
      </td>
      <td>
        Scans a registered table from catalog and returns the resulting Table.
        It can be replaced by <strong>from_path</strong>.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.scan" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_table(name, table)</strong>
      </td>
      <td>
        Registers a `Table` object under a unique name in the TableEnvironment's catalog. 
        Registered tables can be referenced in SQL queries.
        It can be replaced by <strong>create_temporary_view</strong>.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_table" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_table_source(name, table_source)</strong>
      </td>
      <td>
        Registers an external `TableSource` in the TableEnvironment's catalog.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_table_source" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_table_sink(name, table_sink)</strong>
      </td>
      <td>
        Registers an external `TableSink` in the TableEnvironment's catalog.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_table_sink" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>insert_into(target_path, table)</strong>
      </td>
      <td>
        Instructs to write the content of a `Table` object into a sink table.
        Note that this interface would not trigger the execution of jobs.
        You need to call the "execute" method to execute your job.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.insert_into" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>sql_update(stmt)</strong>
      </td>
      <td>
        Evaluates a SQL statement such as INSERT, UPDATE or DELETE or a DDL statement.
        It can be replaced by <strong>execute_sql</strong>.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.sql_update" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

### Execute/Explain Jobs

These APIs are used to explain/execute jobs. Note that the API `execute_sql` can also be used to execute jobs.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>explain_sql(stmt, *extra_details)</strong>
      </td>
      <td>
        Returns the AST and the execution plan of the specified statement.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.explain_sql" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_statement_set()</strong>
      </td>
      <td>
        Creates a StatementSet instance which accepts DML statements or Tables.
        It can be used to execute a multi-sink job. 
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_statement_set" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

<big><strong>Deprecated APIs</strong></big>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>explain(table=None, extended=False)</strong>
      </td>
      <td>
        Returns the AST of the specified Table API and SQL queries and the execution plan to compute
        the result of the given `Table` object or multi-sinks plan.
        If you use the <strong>insert_into</strong> or <strong>sql_update</strong> method to emit data to multiple sinks, you can use this
        method to get the plan.
        It can be replaced by <strong>TableEnvironment.explain_sql</strong>, <strong>Table.explain</strong> or <strong>StatementSet.explain</strong>.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.explain" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>execute(job_name)</strong>
      </td>
      <td>
        Triggers the program execution. The environment will execute all parts of the program.
        If you use the <strong>insert_into</strong> or <strong>sql_update</strong> method to emit data to sinks, you can use this
        method trigger the program execution.
        This method will block the client program until the job is finished/canceled/failed.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.execute" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

### Create/Drop User Defined Functions

These APIs are used to register UDFs or remove the registered UDFs. 
Note that the API `execute_sql` can also be used to register/remove UDFs.
For more details about the different kinds of UDFs, please refer to [User Defined Functions]({{< ref "docs/dev/table/functions/overview" >}}).

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>create_temporary_function(path, function)</strong>
      </td>
      <td>
        Registers a Python user defined function class as a temporary catalog function.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_temporary_system_function(name, function)</strong>
      </td>
      <td>
        Registers a Python user defined function class as a temporary system function.
        If the name of a temporary system function is the same as a temporary catalog function,
        the temporary system function takes precedence.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_system_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_java_function(path, function_class_name, ignore_if_exists=None)</strong>
      </td>
      <td>
        Registers a Java user defined function class as a catalog function under the given path.
        If the catalog is persistent, the registered catalog function can be used across multiple Flink sessions and clusters.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_java_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_java_temporary_function(path, function_class_name)</strong>
      </td>
      <td>
        Registers a Java user defined function class as a temporary catalog function.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_java_temporary_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>create_java_temporary_system_function(name, function_class_name)</strong>
      </td>
      <td>
        Registers a Java user defined function class as a temporary system function.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.create_java_temporary_system_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_function(path)</strong>
      </td>
      <td>
        Drops a catalog function registered under the given path.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_function(path)</strong>
      </td>
      <td>
        Drops a temporary system function registered under the given name.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>drop_temporary_system_function(name)</strong>
      </td>
      <td>
        Drops a temporary system function registered under the given name.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_system_function" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

<big><strong>Deprecated APIs</strong></big>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>register_function(name, function)</strong>
      </td>
      <td>
        Registers a Python user-defined function under a unique name. 
        Replaces already existing user-defined function under this name.
        It can be replaced by <strong>create_temporary_system_function</strong>.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_function" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>register_java_function(name, function_class_name)</strong>
      </td>
      <td>
        Registers a Java user defined function under a unique name. 
        Replaces already existing user-defined functions under this name.
        It can be replaced by <strong>create_java_temporary_system_function</strong>.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_java_function" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

### Dependency Management

These APIs are used to manage the Python dependencies which are required by the Python UDFs.
Please refer to the [Dependency Management]({{< ref "docs/dev/python/dependency_management" >}}#python-dependencies) documentation for more details.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>add_python_file(file_path)</strong>
      </td>
      <td>
        Adds a Python dependency which could be Python files, Python packages or local directories. 
        They will be added to the PYTHONPATH of the Python UDF worker.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.add_python_file" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>set_python_requirements(requirements_file_path, requirements_cache_dir=None)</strong>
      </td>
      <td>
        Specifies a requirements.txt file which defines the third-party dependencies.
        These dependencies will be installed to a temporary directory and added to the PYTHONPATH of the Python UDF worker.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.set_python_requirements" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>add_python_archive(archive_path, target_dir=None)</strong>
      </td>
      <td>
        Adds a Python archive file. The file will be extracted to the working directory of Python UDF worker.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.add_python_archive" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

### Configuration

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>get_config()</strong>
      </td>
      <td>
        Returns the table config to define the runtime behavior of the Table API.
        You can find all the available configuration options in <a href="{{< ref "docs/deployment/config" >}}">Configuration</a> and
        <a href="{{< ref "docs/dev/python/python_config" >}}">Python Configuration</a>. <br> <br>
        The following code is an example showing how to set the configuration options through this API:
```python
# set the parallelism to 8
table_env.get_config().get_configuration().set_string(
    "parallelism.default", "8")
```
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_config" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

### Catalog APIs

These APIs are used to access catalogs and modules. You can find more detailed introduction in [Modules]({{< ref "docs/dev/table/modules" >}}) and [Catalogs]({{< ref "docs/dev/table/catalogs" >}}) documentation.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-center">Description</th>
      <th class="text-center" style="width: 10%">Docs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>register_catalog(catalog_name, catalog)</strong>
      </td>
      <td>
        Registers a `Catalog` under a unique name.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.register_catalog" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>get_catalog(catalog_name)</strong>
      </td>
      <td>
        Gets a registered `Catalog` by name.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_catalog" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>use_catalog(catalog_name)</strong>
      </td>
      <td>
        Sets the current catalog to the given value.
        It also sets the default database to the catalog's default one.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.use_catalog" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>get_current_catalog()</strong>
      </td>
      <td>
        Gets the current default catalog name of the current session.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_current_catalog" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>get_current_database()</strong>
      </td>
      <td>
        Gets the current default database name of the running session.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.get_current_database" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>use_database(database_name)</strong>
      </td>
      <td>
        Sets the current default database.
        It has to exist in the current catalog.
        That path will be used as the default one when looking for unqualified object names.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.use_database" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>load_module(module_name, module)</strong>
      </td>
      <td>
        Loads a `Module` under a unique name.
        Modules will be kept in the loaded order.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.load_module" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>unload_module(module_name)</strong>
      </td>
      <td>
        Unloads a `Module` with given name.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.unload_module" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>use_modules(*module_names)</strong>
      </td>
      <td>
        Enables and changes the resolution order of loaded modules.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.use_modules" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_catalogs()</strong>
      </td>
      <td>
        Gets the names of all catalogs registered in this environment.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_catalogs" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_modules()</strong>
      </td>
      <td>
        Gets the names of all enabled modules registered in this environment.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_modules" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_full_modules()</strong>
      </td>
      <td>
        Gets the names of all loaded modules (including disabled modules) registered in this environment.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_full_modules" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_databases()</strong>
      </td>
      <td>
        Gets the names of all databases in the current catalog.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_databases" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_tables()</strong>
      </td>
      <td>
        Gets the names of all tables and views in the current database of the current catalog.
        It returns both temporary and permanent tables and views.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_tables" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_views()</strong>
      </td>
      <td>
        Gets the names of all views in the current database of the current catalog.
        It returns both temporary and permanent views.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_views" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_user_defined_functions()</strong>
      </td>
      <td>
        Gets the names of all user defined functions registered in this environment.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_user_defined_functions" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_functions()</strong>
      </td>
      <td>
        Gets the names of all functions in this environment.
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_functions" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_temporary_tables()</strong>
      </td>
      <td>
        Gets the names of all temporary tables and views available in the current namespace (the current database of the current catalog).
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_temporary_tables" name="link">}}
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_temporary_views()</strong>
      </td>
      <td>
        Gets the names of all temporary views available in the current namespace (the current database of the current catalog).
      </td>
      <td class="text-center">
        {{< pythondoc file="pyflink.table.html#pyflink.table.TableEnvironment.list_temporary_views" name="link">}}
      </td>
    </tr>
  </tbody>
</table>

Statebackend, Checkpoint and Restart Strategy
---------------------------------------------

Before Flink 1.10 you can configure the statebackend, checkpointing and restart strategy via the `StreamExecutionEnvironment`.
And now you can configure them by setting key-value options in `TableConfig`, see [Fault Tolerance]({{< ref "docs/deployment/config" >}}#fault-tolerance), [State Backends]({{< ref "docs/deployment/config" >}}#checkpoints-and-state-backends) and [Checkpointing]({{< ref "docs/deployment/config" >}}#checkpointing) for more details.

The following code is an example showing how to configure the statebackend, checkpoint and restart strategy through the Table API:
```python
# set the restart strategy to "fixed-delay"
table_env.get_config().get_configuration().set_string("restart-strategy", "fixed-delay")
table_env.get_config().get_configuration().set_string("restart-strategy.fixed-delay.attempts", "3")
table_env.get_config().get_configuration().set_string("restart-strategy.fixed-delay.delay", "30s")

# set the checkpoint mode to EXACTLY_ONCE
table_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "3min")

# set the statebackend type to "rocksdb", other available options are "filesystem" and "jobmanager"
# you can also set the full qualified Java class name of the StateBackendFactory to this option
# e.g. org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory
table_env.get_config().get_configuration().set_string("state.backend", "rocksdb")

# set the checkpoint directory, which is required by the RocksDB statebackend
table_env.get_config().get_configuration().set_string("state.checkpoints.dir", "file:///tmp/checkpoints/")
```
