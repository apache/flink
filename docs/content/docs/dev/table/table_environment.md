---
title: "TableEnvironment"
weight: 3
type: docs
aliases:
  - /dev/python/table-api-users-guide/table_environment.html
  - /docs/dev/table/python/table_environment/
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

This document is an introduction to the `TableEnvironment`, the central concept of the Table API.
It includes detailed descriptions of the public interfaces of the `TableEnvironment` class.

Create a TableEnvironment
-------------------------

The recommended way to create a `TableEnvironment` is to create from an `EnvironmentSettings` object:

{{< tabs "create-table-env" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

// create a streaming TableEnvironment
EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build();

TableEnvironment tableEnv = TableEnvironment.create(settings);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

// create a streaming TableEnvironment
val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build()

val tableEnv = TableEnvironment.create(settings)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment
config = Configuration()
config.set_string('execution.buffer-timeout', '1 min')
env_settings = EnvironmentSettings \
    .new_instance() \
    .in_streaming_mode() \
    .with_configuration(config) \
    .build()

table_env = TableEnvironment.create(env_settings)
```
{{< /tab >}}
{{< /tabs >}}

Alternatively, users can create a `StreamTableEnvironment` from an existing `StreamExecutionEnvironment` to interoperate with the DataStream API.

{{< tabs "create-stream-table-env" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// create a streaming TableEnvironment from a StreamExecutionEnvironment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

// create a streaming TableEnvironment from a StreamExecutionEnvironment
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# create a streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
```
{{< /tab >}}
{{< /tabs >}}

TableEnvironment API
--------------------

### Table/SQL Operations

These APIs are used to create/remove Table API/SQL Tables and write queries:

| Java/Scala | Python | Description |
|------------|--------|-------------|
| `fromValues(values...)` | `from_elements(elements, schema)` | Creates a table from a collection of values. |
| N/A | `from_pandas(pdf, schema)` | Creates a table from a pandas DataFrame. |
| `from(path)` | `from_path(path)` | Creates a table from a registered table under the specified path. |
| `createTemporaryView(path, table)` | `create_temporary_view(path, table)` | Registers a Table as a temporary view similar to SQL temporary views. |
| `dropTemporaryView(path)` | `drop_temporary_view(path)` | Drops a temporary view registered under the given path. |
| `createTemporaryTable(path, descriptor)` | `create_temporary_table(path, descriptor)` | Creates a temporary table from a TableDescriptor. |
| `createTable(path, descriptor)` | `create_table(path, descriptor)` | Creates a catalog table from a TableDescriptor. |
| `dropTemporaryTable(path)` | `drop_temporary_table(path)` | Drops a temporary table registered under the given path. |
| `dropTable(path)` | `drop_table(path)` | Drops a table registered under the given path. |
| `executeSql(stmt)` | `execute_sql(stmt)` | Executes the given SQL statement and returns the execution result. Supports DDL/DML/DQL/SHOW/DESCRIBE/EXPLAIN/USE statements. |
| `sqlQuery(query)` | `sql_query(query)` | Evaluates a SQL query and retrieves the result as a Table. |

See the [Java API docs](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/table/api/TableEnvironment.html) and [Python API docs](https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.table/api/pyflink.table.TableEnvironment.html) for complete API reference.

<big><strong>Deprecated APIs</strong></big>

| Java/Scala | Python | Description | Replacement |
|------------|--------|-------------|-------------|
| `scan(tablePath...)` | `scan(*table_path)` | Scans a registered table from catalog. | `from` / `from_path` |
| `registerTable(name, table)` | `register_table(name, table)` | Registers a Table under a unique name. | `createTemporaryView` / `create_temporary_view` |
| `insertInto(targetPath, table)` | `insert_into(target_path, table)` | Writes Table content to a sink. | `Table.executeInsert()` / `execute_insert()` |
| N/A | `sql_update(stmt)` | Evaluates a SQL statement. | `execute_sql` |

### Execute/Explain Jobs

These APIs are used to explain/execute jobs. Note that `executeSql` / `execute_sql` can also be used to execute jobs.

| Java/Scala | Python | Description |
|------------|--------|-------------|
| `explainSql(stmt, extraDetails...)` | `explain_sql(stmt, *extra_details)` | Returns the AST and execution plan of the specified statement. |
| `createStatementSet()` | `create_statement_set()` | Creates a StatementSet for executing multiple statements as a single job. |

### Create/Drop User Defined Functions

These APIs are used to register UDFs or remove registered UDFs.
Note that `executeSql` / `execute_sql` can also be used to register/remove UDFs via SQL DDL.
For more details about UDFs, see [User Defined Functions]({{< ref "docs/dev/table/functions/overview" >}}).

| Java/Scala | Python | Description |
|------------|--------|-------------|
| `createTemporaryFunction(path, class)` | `create_temporary_function(path, function)` | Registers a function as a temporary catalog function. |
| `createTemporarySystemFunction(name, class)` | `create_temporary_system_function(name, function)` | Registers a function as a temporary system function. |
| `createFunction(path, class)` | `create_java_function(path, function_class_name)` | Registers a function class as a catalog function. |
| N/A | `create_java_temporary_function(path, function_class_name)` | Registers a Java UDF class as a temporary catalog function. |
| N/A | `create_java_temporary_system_function(name, function_class_name)` | Registers a Java UDF class as a temporary system function. |
| `dropFunction(path)` | `drop_function(path)` | Drops a catalog function registered under the given path. |
| `dropTemporaryFunction(path)` | `drop_temporary_function(path)` | Drops a temporary function registered under the given path. |
| `dropTemporarySystemFunction(name)` | `drop_temporary_system_function(name)` | Drops a temporary system function registered under the given name. |

### Dependency Management (Python only)

These APIs are used to manage Python dependencies required by Python UDFs.
See [Dependency Management]({{< ref "docs/dev/python/dependency_management" >}}#python-dependencies) for more details.

| Method | Description |
|--------|-------------|
| `add_python_file(file_path)` | Adds a Python dependency (file, package, or directory) to the PYTHONPATH of UDF workers. |
| `set_python_requirements(requirements_file_path, cache_dir)` | Specifies a requirements.txt file for third-party dependencies. |
| `add_python_archive(archive_path, target_dir)` | Adds a Python archive file to be extracted in the working directory of UDF workers. |

### Configuration

{{< tabs "configuration-api" >}}
{{< tab "Java" >}}
```java
// get the TableConfig
TableConfig config = tableEnv.getConfig();

// set configuration options
config.set("parallelism.default", "8");
config.set("pipeline.name", "my_first_job");
```

See [Configuration]({{< ref "docs/deployment/config" >}}) for all available options.
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get the TableConfig
val config = tableEnv.getConfig

// set configuration options
config.set("parallelism.default", "8")
config.set("pipeline.name", "my_first_job")
```

See [Configuration]({{< ref "docs/deployment/config" >}}) for all available options.
{{< /tab >}}
{{< tab "Python" >}}
```python
# get the TableConfig
config = table_env.get_config()

# set configuration options
config.set("parallelism.default", "8")
config.set("pipeline.name", "my_first_job")
```

See [Configuration]({{< ref "docs/deployment/config" >}}) and [Python Configuration]({{< ref "docs/dev/python/python_config" >}}) for all available options.
{{< /tab >}}
{{< /tabs >}}

### Catalog APIs

These APIs are used to access catalogs and modules. See [Modules]({{< ref "docs/dev/table/modules" >}}) and [Catalogs]({{< ref "docs/sql/catalogs" >}}) for more details.

| Java/Scala | Python | Description |
|------------|--------|-------------|
| `registerCatalog(name, catalog)` | `register_catalog(name, catalog)` | Registers a Catalog under a unique name. |
| `getCatalog(name)` | `get_catalog(name)` | Gets a registered Catalog by name. |
| `useCatalog(name)` | `use_catalog(name)` | Sets the current catalog. |
| `getCurrentCatalog()` | `get_current_catalog()` | Gets the current default catalog name. |
| `useDatabase(name)` | `use_database(name)` | Sets the current default database. |
| `getCurrentDatabase()` | `get_current_database()` | Gets the current default database name. |
| `loadModule(name, module)` | `load_module(name, module)` | Loads a Module under a unique name. |
| `unloadModule(name)` | `unload_module(name)` | Unloads a Module with the given name. |
| `useModules(names...)` | `use_modules(*names)` | Enables and changes the resolution order of loaded modules. |
| `listCatalogs()` | `list_catalogs()` | Gets the names of all registered catalogs. |
| `listModules()` | `list_modules()` | Gets the names of all enabled modules. |
| N/A | `list_full_modules()` | Gets the names of all loaded modules (including disabled). |
| `listDatabases()` | `list_databases()` | Gets the names of all databases in the current catalog. |
| `listTables()` | `list_tables()` | Gets the names of all tables and views in the current database. |
| `listViews()` | `list_views()` | Gets the names of all views in the current database. |
| `listFunctions()` | `list_functions()` | Gets the names of all functions in this environment. |
| N/A | `list_user_defined_functions()` | Gets the names of all user-defined functions. |
| `listTemporaryTables()` | `list_temporary_tables()` | Gets the names of all temporary tables and views. |
| `listTemporaryViews()` | `list_temporary_views()` | Gets the names of all temporary views. |

Statebackend, Checkpoint and Restart Strategy
---------------------------------------------

You can configure statebackend, checkpointing, and restart strategy by setting key-value options in `TableConfig`.
See [Fault Tolerance]({{< ref "docs/deployment/config" >}}#fault-tolerance), [State Backends]({{< ref "docs/deployment/config" >}}#checkpoints-and-state-backends), and [Checkpointing]({{< ref "docs/deployment/config" >}}#checkpointing) for more details.

{{< tabs "statebackend-config" >}}
{{< tab "Java" >}}
```java
TableConfig config = tableEnv.getConfig();

// set the restart strategy to "fixed-delay"
config.set("restart-strategy.type", "fixed-delay");
config.set("restart-strategy.fixed-delay.attempts", "3");
config.set("restart-strategy.fixed-delay.delay", "30s");

// set the checkpoint mode to EXACTLY_ONCE
config.set("execution.checkpointing.mode", "EXACTLY_ONCE");
config.set("execution.checkpointing.interval", "3min");

// set the statebackend type to "rocksdb"
config.set("state.backend.type", "rocksdb");

// set the checkpoint directory
config.set("execution.checkpointing.dir", "file:///tmp/checkpoints/");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val config = tableEnv.getConfig

// set the restart strategy to "fixed-delay"
config.set("restart-strategy.type", "fixed-delay")
config.set("restart-strategy.fixed-delay.attempts", "3")
config.set("restart-strategy.fixed-delay.delay", "30s")

// set the checkpoint mode to EXACTLY_ONCE
config.set("execution.checkpointing.mode", "EXACTLY_ONCE")
config.set("execution.checkpointing.interval", "3min")

// set the statebackend type to "rocksdb"
config.set("state.backend.type", "rocksdb")

// set the checkpoint directory
config.set("execution.checkpointing.dir", "file:///tmp/checkpoints/")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = table_env.get_config()

# set the restart strategy to "fixed-delay"
config.set("restart-strategy.type", "fixed-delay")
config.set("restart-strategy.fixed-delay.attempts", "3")
config.set("restart-strategy.fixed-delay.delay", "30s")

# set the checkpoint mode to EXACTLY_ONCE
config.set("execution.checkpointing.mode", "EXACTLY_ONCE")
config.set("execution.checkpointing.interval", "3min")

# set the statebackend type to "rocksdb"
config.set("state.backend.type", "rocksdb")

# set the checkpoint directory
config.set("execution.checkpointing.dir", "file:///tmp/checkpoints/")
```
{{< /tab >}}
{{< /tabs >}}
