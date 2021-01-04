---
title: "TableEnvironment"
nav-parent_id: python_tableapi
nav-pos: 27
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

This document is an introduction of PyFlink `TableEnvironment`. 
It includes detailed descriptions of every public interface of the `TableEnvironment` class.

* This will be replaced by the TOC
{:toc}

Create a TableEnvironment
-------------------------

The recommended way to create a `TableEnvironment` is to create from an `EnvironmentSettings` object:

{% highlight python %}

from pyflink.table import EnvironmentSettings, StreamTableEnvironment, BatchTableEnvironment

# create a blink streaming TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# create a blink batch TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

# create a flink streaming TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_old_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# create a flink batch TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_old_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

{% endhighlight %}

If you need to access the `ExecutionEnvironment`/`StreamExecutionEnvironment` object which the `TableEnvironment` based on,
e.g. mixing with DataStream API, configuring via the APIs of `ExecutionEnvironment`/`StreamExecutionEnvironment`, 
you can also create a `TableEnvironment` from an `ExecutionEnvironment`/`StreamExecutionEnvironment` with a optional `TableConfig` object:

{% highlight python %}

from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, BatchTableEnvironment, TableConfig

# create a blink streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# create a blink streaming TableEnvironment from a StreamExecutionEnvironment with a TableConfig
env = StreamExecutionEnvironment.get_execution_environment()
table_config = TableConfig()  # you can add configuration options in it
table_env = StreamTableEnvironment.create(env, table_config)

# create a flink batch TableEnvironment from an ExecutionEnvironment
env = ExecutionEnvironment.get_execution_environment()
table_env = BatchTableEnvironment.create(env)

# create a flink batch TableEnvironment from an ExecutionEnvironment with a TableConfig
env = ExecutionEnvironment.get_execution_environment()
table_config = TableConfig()  # you can add configuration options in it
table_env = BatchTableEnvironment.create(env, table_config)

# create a flink streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_old_planner().build()
table_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

{% endhighlight %}

**Note:** Almost all the configurations in `ExecutionEnvironment`/`StreamExecutionEnvironment` can be configured via `TableEnvironment.get_config()` now, see [Configuration]({% link deployment/config.md %}) for more details.
Only a few rarely used or deprecated configurations still require direct access to `ExecutionEnvironment` /`StreamExecutionEnvironment` for configuring, e.g. the input dependency constraint.

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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.from_elements">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.from_pandas">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.from_path">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.sql_query">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_view">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_view">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_table">link</a>
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
        However, when executing a job in a mini cluster or IDE, you need to wait until the job execution finished, then you can refer to <a href="{% link dev/python/faq.md %}#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster">here</a> for more details. <br>
        Please refer the <a href="{% link dev/table/sql/index.md %}">SQL</a> documentation for more details about SQL statement.
      </td>
      <td class="text-center">
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.execute_sql">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.from_table_source">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.scan">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.register_table">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.register_table_source">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.register_table_sink">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.insert_into">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.sql_update">link</a>
      </td>
    </tr>
    <tr>
      <td>
        <strong>connect(connector_descriptor)</strong>
      </td>
      <td>
        Creates a temporary table from a descriptor. 
        Currently the recommended way is using <strong>execute_sql</strong> to register temporary tables.
      </td>
      <td class="text-center">
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.connect">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.explain_sql">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.create_statement_set">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.explain">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.execute">link</a>
      </td>
    </tr>
  </tbody>
</table>

### Create/Drop User Defined Functions

These APIs are used to register UDFs or remove the registered UDFs. 
Note that the API `execute_sql` can also be used to register/remove UDFs.
For more details about the different kinds of UDFs, please refer to [User Defined Functions]({% link dev/table/functions/index.md %}).

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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.create_temporary_system_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.create_java_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.create_java_temporary_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.create_java_temporary_system_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.drop_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.drop_temporary_system_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.register_function">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.register_java_function">link</a>
      </td>
    </tr>
  </tbody>
</table>

### Dependency Management

These APIs are used to manage the Python dependencies which are required by the Python UDFs.
Please refer to the [Dependency Management]({% link dev/python/table-api-users-guide/dependency_management.md %}#python-dependency-in-python-program) documentation for more details.

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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.add_python_file">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.set_python_requirements">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.add_python_archive">link</a>
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
        You can find all the available configuration options in <a href="{% link deployment/config.md %}">Configuration</a> and
        <a href="{% link dev/python/python_config.md %}">Python Configuation</a>. <br> <br>
        The following code is an example showing how to set the configuration options through this API:
{% highlight python %}
# set the parallelism to 8
table_env.get_config().get_configuration().set_string(
    "parallelism.default", "8")
{% endhighlight %}
      </td>
      <td class="text-center">
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.get_config">link</a>
      </td>
    </tr>
  </tbody>
</table>

### Catalog APIs

These APIs are used to access catalogs and modules. You can find more detailed introduction in [Modules]({% link dev/table/modules.md %}) and [Catalogs]({% link dev/table/catalogs.md %}) documentation.

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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.register_catalog">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.get_catalog">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.use_catalog">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.get_current_catalog">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.get_current_database">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.use_database">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.load_module">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.unload_module">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_catalogs">link</a>
      </td>
    </tr>
    <tr>
      <td>
        <strong>list_modules()</strong>
      </td>
      <td>
        Gets the names of all modules registered in this environment.
      </td>
      <td class="text-center">
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_modules">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_databases">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_tables">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_views">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_user_defined_functions">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_functions">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_temporary_tables">link</a>
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
        <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.TableEnvironment.list_temporary_views">link</a>
      </td>
    </tr>
  </tbody>
</table>

Statebackend, Checkpoint and Restart Strategy
---------------------------------------------

Before Flink 1.10 you can configure the statebackend, checkpointing and restart strategy via the `StreamExecutionEnvironment`.
And now you can configure them by setting key-value options in `TableConfig`, see [Fault Tolerance]({% link deployment/config.md %}#fault-tolerance), [State Backends]({% link deployment/config.md %}#checkpoints-and-state-backends) and [Checkpointing]({% link deployment/config.md %}#checkpointing) for more details.

The following code is an example showing how to configure the statebackend, checkpoint and restart strategy through the Table API:
{% highlight python %}
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
{% endhighlight %}
