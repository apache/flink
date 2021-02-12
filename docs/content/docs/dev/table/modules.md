---
title: "Modules"
is_beta: true
weight: 71
type: docs
aliases:
  - /dev/table/modules.html
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

`CoreModule` contains all of Flink's system (built-in) functions and is loaded by default.

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
 

## Namespace and Resolution Order

Objects provided by modules are considered part of Flink's system (built-in) objects; thus, they don't have any namespaces.

When there are two objects of the same name residing in two modules, Flink always resolves the object reference to the one in the 1st loaded module.

## Module API

### Loading and unloading a Module

Users can load and unload modules in an existing Flink session.

{{< tabs "8af50989-0812-4ab9-b1e6-30422753d340" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.loadModule("myModule", new CustomModule());
tableEnv.unloadModule("myModule");
```
{{< /tab >}}
{{< tab "YAML" >}}

All modules defined using YAML must provide a `type` property that specifies the type. 
The following types are supported out of the box.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-center" style="width: 25%">Catalog</th>
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
   - name: myhive
     type: hive
```
{{< /tab >}}
{{< /tabs >}}

### List Available Modules

{{< tabs "7b5a086d-0db1-4914-98eb-a20e86e55160" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.listModules();
```
{{< /tab >}}
{{< tab "SQL" >}}
```sql
Flink SQL> SHOW MODULES;
```
{{< /tab >}}
{{< /tabs >}}
