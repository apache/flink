---
title: "Regular Expressions"
weight: 13
type: docs
bookCollapseSection: true
sqlFunctions: regex
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

# Regular Expression Functions

Regular expression functions test, count, find, extract and replace the substrings of a string that match a regular expression, such as `REGEXP`, `REGEXP_EXTRACT` and `REGEXP_REPLACE`. A regular expression uses the syntax of Java's [`Pattern`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html) class. For simpler patterns, see `LIKE` and `SIMILAR TO` in [Comparison]({{< ref "docs/sql/built-in-functions/comparison" >}}).

{{< sql_functions_zh "regex" >}}
