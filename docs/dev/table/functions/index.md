---
title: "Functions"
nav-id: table_functions
nav-parent_id: tableapi
nav-pos: 60
nav-show_overview: true
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

Flink Table API & SQL empowers users to do data transformations with functions.

* This will be replaced by the TOC
{:toc}

Types of Functions
------------------

There are two dimensions to classify functions in Flink.

One dimension is system (or built-in) functions v.s. catalog functions. System functions have no namespace and can be
referenced with just their names. Catalog functions belong to a catalog and database therefore they have catalog and database
namespaces, they can be referenced by either fully/partially qualified name (`catalog.db.func` or `db.func`) or just the
function name.

The other dimension is temporary functions v.s. persistent functions. Temporary functions are volatile and only live up to
 lifespan of a session, they are always created by users. Persistent functions live across lifespan of sessions, they are either
 provided by the system or persisted in catalogs.
 
The two dimensions give Flink users 4 categories of functions:

1. Temporary system functions
2. System functions
3. Temporary catalog functions
4. Catalog functions

Referencing Functions
---------------------

There are two ways users can reference a function in Flink - referencing function precisely or ambiguously.

## Precise Function Reference

Precise function reference empowers users to use catalog functions specifically, and across catalog and across database, 
e.g. `select mycatalog.mydb.myfunc(x) from mytable` and `select mydb.myfunc(x) from mytable`.

This is only supported starting from Flink 1.10.

## Ambiguous Function Reference

In ambiguous function reference, users just specify the function's name in SQL query, e.g. `select myfunc(x) from mytable`.


Function Resolution Order
-------------------------

The resolution order only matters when there are functions of different types but the same name, 
e.g. when there’re three functions all named “myfunc” but are of temporary catalog, catalog, and system function respectively. 
If there’s no function name collision, functions will just be resolved to the sole one.

## Precise Function Reference

Because system functions don’t have namespaces, a precise function reference in Flink must be pointing to either a temporary catalog 
function or a catalog function.

The resolution order is:

1. Temporary catalog function
2. Catalog function

## Ambiguous Function Reference

The resolution order is:

1. Temporary system function
2. System function
3. Temporary catalog function, in the current catalog and current database of the session
4. Catalog function, in the current catalog and current database of the session
