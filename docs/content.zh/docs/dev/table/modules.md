---
title: "模块"
is_beta: true
weight: 71
type: docs
aliases:
  - /zh/dev/table/modules.html
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

# Modules

Modules allow users to extend Flink's built-in objects, such as defining functions that behave like Flink 
built-in functions. They are pluggable, and while Flink provides a few pre-built modules, users can write 
their own.

For example, users can define their own geo functions and plug them into Flink as built-in functions to be used in 
Flink SQL and Table APIs. Another example is users can load an out-of-shelf Hive module to use Hive built-in 
functions as Flink built-in functions.

## Module Types

### CoreModule

`CoreModule` contains all of Flink's system (built-in) functions and is loaded and enabled by default.

### HiveModule

The `HiveModule` provides Hive built-in functions as Flink's system functions to SQL and Table API users.
Flink's [Hive documentation]({{< ref "docs/connectors/table/hive/hive_functions" >}}) provides full details on setting up the module.

### User-Defined Module

Users can develop custom modules by implementing the `Module` interface.
To use custom modules in SQL CLI, users should develop both a module and its corresponding module factory by implementing 
the `ModuleFactory` interface.

A module factory defines a set of properties for configuring the module when the SQL CLI bootstraps.
Properties are passed to a discovery service where the service tries to match the properties to
 a `ModuleFactory` and instantiate a corresponding module instance.

##  Module Lifecycle and Resolution Order

A module can be loaded, enabled, disabled and unloaded. When TableEnvironment loads a module initially, it enables the module by default. Flink supports multiple modules and keeps track of the loading order to resolve metadata.
Besides, Flink only resolves the functions among enabled modules. *E.g.*, when there are two functions of the same name residing in two modules, there will be three conditions.
- If both of the modules are enabled, then Flink resolves the function according to the resolution order of the modules.
- If one of them is disabled, then Flink resolves the function to the enabled module.
- If both of the modules are disabled, then Flink cannot resolve the function.

Users can change the resolution order by using modules in a different declared order. *E.g.*, users can specify Flink to find functions first in Hive by `USE MODULES hive, core`.

Besides, users can also disable modules by not declaring them. *E.g.*, users can specify Flink to disable core module by `USE MODULES hive` (However, it is strongly not recommended disabling core module). Disable a module does not unload it, and users can enable it again by using it. *E.g.*, users can bring back core module and place it in the first by `USE MODULES core, hive`. A module can be enabled only when it is loaded already. Using an unloaded module will throw an Exception. Eventually, users can unload a module.

The difference between disabling and unloading a module is that TableEnvironment still keeps the disabled modules, and users can list all loaded modules to view the disabled modules.

## Namespace

Objects provided by modules are considered part of Flink's system (built-in) objects; thus, they don't have any namespaces.

## How to Load, Unload, Use and List Modules

### Using SQL

Users can use SQL to load/unload/use/list modules in both Table API and SQL CLI.

{{< tabs "SQL snippets" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

// Show initially loaded and enabled modules
tableEnv.executeSql("SHOW MODULES").print();
// +-------------+
// | module name |
// +-------------+
// |        core |
// +-------------+
tableEnv.executeSql("SHOW FULL MODULES").print();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// +-------------+------+

// Load a hive module
tableEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '...')");

// Show all enabled modules
tableEnv.executeSql("SHOW MODULES").print();
// +-------------+
// | module name |
// +-------------+
// |        core |
// |        hive |
// +-------------+

// Show all loaded modules with both name and use status
tableEnv.executeSql("SHOW FULL MODULES").print();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// |        hive | true |
// +-------------+------+

// Change resolution order
tableEnv.executeSql("USE MODULES hive, core");
tableEnv.executeSql("SHOW MODULES").print();
// +-------------+
// | module name |
// +-------------+
// |        hive |
// |        core |
// +-------------+
tableEnv.executeSql("SHOW FULL MODULES").print();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        hive | true |
// |        core | true |
// +-------------+------+

// Disable core module
tableEnv.executeSql("USE MODULES hive");
tableEnv.executeSql("SHOW MODULES").print();
// +-------------+
// | module name |
// +-------------+
// |        hive |
// +-------------+
tableEnv.executeSql("SHOW FULL MODULES").print();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive |  true |
// |        core | false |
// +-------------+-------+

// Unload hive module
tableEnv.executeSql("UNLOAD MODULE hive");
tableEnv.executeSql("SHOW MODULES").print();
// Empty set
tableEnv.executeSql("SHOW FULL MODULES").print();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive | false |
// +-------------+-------+
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
val tableEnv = TableEnvironment.create(setting)

// Show initially loaded and enabled modules
tableEnv.executeSql("SHOW MODULES").print()
// +-------------+
// | module name |
// +-------------+
// |        core |
// +-------------+
tableEnv.executeSql("SHOW FULL MODULES").print()
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// +-------------+------+

// Load a hive module
tableEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '...')")

// Show all enabled modules
tableEnv.executeSql("SHOW MODULES").print()
// +-------------+
// | module name |
// +-------------+
// |        core |
// |        hive |
// +-------------+

// Show all loaded modules with both name and use status
tableEnv.executeSql("SHOW FULL MODULES")
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// |        hive | true |
// +-------------+------+

// Change resolution order
tableEnv.executeSql("USE MODULES hive, core")
tableEnv.executeSql("SHOW MODULES").print()
// +-------------+
// | module name |
// +-------------+
// |        hive |
// |        core |
// +-------------+
tableEnv.executeSql("SHOW FULL MODULES").print()
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        hive | true |
// |        core | true |
// +-------------+------+

// Disable core module
tableEnv.executeSql("USE MODULES hive")
tableEnv.executeSql("SHOW MODULES").print()
// +-------------+
// | module name |
// +-------------+
// |        hive |
// +-------------+
tableEnv.executeSql("SHOW FULL MODULES").print()
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive |  true |
// |        core | false |
// +-------------+-------+

// Unload hive module
tableEnv.executeSql("UNLOAD MODULE hive")
tableEnv.executeSql("SHOW MODULES").print()
// Empty set
tableEnv.executeSql("SHOW FULL MODULES").print()
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive | false |
// +-------------+-------+
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *

# environment configuration
settings = EnvironmentSettings.new_instance().use_blink_planner().build()
t_env = TableEnvironment.create(settings)

# Show initially loaded and enabled modules
t_env.execute_sql("SHOW MODULES").print()
# +-------------+
# | module name |
# +-------------+
# |        core |
# +-------------+
t_env.execute_sql("SHOW FULL MODULES").print()
# +-------------+------+
# | module name | used |
# +-------------+------+
# |        core | true |
# +-------------+------+

# Load a hive module
t_env.execute_sql("LOAD MODULE hive WITH ('hive-version' = '...')")

# Show all enabled modules
t_env.execute_sql("SHOW MODULES").print()
# +-------------+
# | module name |
# +-------------+
# |        core |
# |        hive |
# +-------------+

# Show all loaded modules with both name and use status
t_env.execute_sql("SHOW FULL MODULES").print()
# +-------------+------+
# | module name | used |
# +-------------+------+
# |        core | true |
# |        hive | true |
# +-------------+------+

# Change resolution order
t_env.execute_sql("USE MODULES hive, core")
t_env.execute_sql("SHOW MODULES").print()
# +-------------+
# | module name |
# +-------------+
# |        hive |
# |        core |
# +-------------+
t_env.execute_sql("SHOW FULL MODULES").print()
# +-------------+------+
# | module name | used |
# +-------------+------+
# |        hive | true |
# |        core | true |
# +-------------+------+

# Disable core module
t_env.execute_sql("USE MODULES hive")
t_env.execute_sql("SHOW MODULES").print()
# +-------------+
# | module name |
# +-------------+
# |        hive |
# +-------------+
t_env.execute_sql("SHOW FULL MODULES").print()
# +-------------+-------+
# | module name |  used |
# +-------------+-------+
# |        hive |  true |
# |        core | false |
# +-------------+-------+

# Unload hive module
t_env.execute_sql("UNLOAD MODULE hive")
t_env.execute_sql("SHOW MODULES").print()
# Empty set
t_env.execute_sql("SHOW FULL MODULES").print()
# +-------------+-------+
# | module name |  used |
# +-------------+-------+
# |        hive | false |
# +-------------+-------+
```
{{< /tab >}}
{{< tab "SQL Client" >}}
```sql
-- Show initially loaded and enabled modules
Flink SQL> SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
+-------------+
1 row in set
Flink SQL> SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        core | true |
+-------------+------+
1 row in set

-- Load a hive module
Flink SQL> LOAD MODULE hive WITH ('hive-version' = '...');

-- Show all enabled modules
Flink SQL> SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
|        hive |
+-------------+
2 rows in set

-- Show all loaded modules with both name and use status
Flink SQL> SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        core | true |
|        hive | true |
+-------------+------+
2 rows in set

-- Change resolution order
Flink SQL> USE MODULES hive, core ;
Flink SQL> SHOW MODULES;
+-------------+
| module name |
+-------------+
|        hive |
|        core |
+-------------+
2 rows in set
Flink SQL> SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        hive | true |
|        core | true |
+-------------+------+
2 rows in set

-- Unload hive module
Flink SQL> UNLOAD MODULE hive;
Flink SQL> SHOW MODULES;
Empty set
Flink SQL> SHOW FULL MODULES;
+-------------+-------+
| module name |  used |
+-------------+-------+
|        hive | false |
+-------------+-------+
1 row in set
```
{{< /tab >}}
{{< tab "YAML" >}}

All modules defined using YAML must provide a `type` property that specifies the type. 
The following types are supported out of the box.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-center" style="width: 25%">Module</th>
      <th class="text-center">Type Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td class="text-center">CoreModule</td>
        <td class="text-center">core</td>
    </tr>
    <tr>
        <td class="text-center">HiveModule</td>
        <td class="text-center">hive</td>
    </tr>
  </tbody>
</table>

```yaml
modules:
   - name: core
     type: core
   - name: hive
     type: hive
```
{{< /tab >}}
{{< /tabs >}}
{{< hint warning >}}
When using SQL, module name is used to perform the module discovery. It is parsed as a simple identifier and case-sensitive.
{{< /hint >}}

### Using Java, Scala or Python

Users can use Java, Scala or Python to load/unload/use/list modules programmatically.

{{< tabs "API snippets" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

// Show initially loaded and enabled modules
tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        core |
// +-------------+
tableEnv.listFullModules();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// +-------------+------+

// Load a hive module
tableEnv.loadModule("hive", new HiveModule());

// Show all enabled modules
tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        core |
// |        hive |
// +-------------+

// Show all loaded modules with both name and use status
tableEnv.listFullModules();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// |        hive | true |
// +-------------+------+

// Change resolution order
tableEnv.useModules("hive", "core");
tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        hive |
// |        core |
// +-------------+
tableEnv.listFullModules();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        hive | true |
// |        core | true |
// +-------------+------+

// Disable core module
tableEnv.useModules("hive");
tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        hive |
// +-------------+
tableEnv.listFullModules();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive |  true |
// |        core | false |
// +-------------+-------+

// Unload hive module
tableEnv.unloadModule("hive");
tableEnv.listModules();
// Empty set
tableEnv.listFullModules();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive | false |
// +-------------+-------+
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
val tableEnv = TableEnvironment.create(setting)

// Show initially loaded and enabled modules
tableEnv.listModules()
// +-------------+
// | module name |
// +-------------+
// |        core |
// +-------------+
tableEnv.listFullModules()
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// +-------------+------+

// Load a hive module
tableEnv.loadModule("hive", new HiveModule())

// Show all enabled modules
tableEnv.listModules()
// +-------------+
// | module name |
// +-------------+
// |        core |
// |        hive |
// +-------------+

// Show all loaded modules with both name and use status
tableEnv.listFullModules()
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// |        hive | true |
// +-------------+------+

// Change resolution order
tableEnv.useModules("hive", "core")
tableEnv.listModules()
// +-------------+
// | module name |
// +-------------+
// |        hive |
// |        core |
// +-------------+
tableEnv.listFullModules()
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        hive | true |
// |        core | true |
// +-------------+------+

// Disable core module
tableEnv.useModules("hive")
tableEnv.listModules()
// +-------------+
// | module name |
// +-------------+
// |        hive |
// +-------------+
tableEnv.listFullModules()
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive |  true |
// |        core | false |
// +-------------+-------+

// Unload hive module
tableEnv.unloadModule("hive")
tableEnv.listModules()
// Empty set
tableEnv.listFullModules()
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive | false |
// +-------------+-------+
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *

# environment configuration
settings = EnvironmentSettings.new_instance().use_blink_planner().build()
t_env = TableEnvironment.create(settings)

# Show initially loaded and enabled modules
t_env.list_modules()
# +-------------+
# | module name |
# +-------------+
# |        core |
# +-------------+
t_env.list_full_modules()
# +-------------+------+
# | module name | used |
# +-------------+------+
# |        core | true |
# +-------------+------+

# Load a hive module
t_env.load_module("hive", HiveModule())

# Show all enabled modules
t_env.list_modules()
# +-------------+
# | module name |
# +-------------+
# |        core |
# |        hive |
# +-------------+

# Show all loaded modules with both name and use status
t_env.list_full_modules()
# +-------------+------+
# | module name | used |
# +-------------+------+
# |        core | true |
# |        hive | true |
# +-------------+------+

# Change resolution order
t_env.use_modules("hive", "core")
t_env.list_modules()
# +-------------+
# | module name |
# +-------------+
# |        hive |
# |        core |
# +-------------+
t_env.list_full_modules()
# +-------------+------+
# | module name | used |
# +-------------+------+
# |        hive | true |
# |        core | true |
# +-------------+------+

# Disable core module
t_env.use_modules("hive")
t_env.list_modules()
# +-------------+
# | module name |
# +-------------+
# |        hive |
# +-------------+
t_env.list_full_modules()
# +-------------+-------+
# | module name |  used |
# +-------------+-------+
# |        hive |  true |
# |        core | false |
# +-------------+-------+

# Unload hive module
t_env.unload_module("hive")
t_env.list_modules()
# Empty set
t_env.list_full_modules()
# +-------------+-------+
# | module name |  used |
# +-------------+-------+
# |        hive | false |
# +-------------+-------+
```
{{< /tab >}}
{{< /tabs >}}
{{< top >}}
