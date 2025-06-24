---
title: "SET Statements"
weight: 8
type: docs
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

# SET Statements

## Description

The `SET` statement sets a property which provide a ways to set variables for a session and
configuration property including system variable and Hive configuration.
But environment variable can't be set via `SET` statement. The behavior of `SET` with Hive dialect is compatible to Hive's.

## EXAMPLES

```sql
-- set Flink's configuration
SET table.sql-dialect=default;

-- set Hive's configuration
SET hiveconf:k1=v1;

-- set system property
SET system:k2=v2;

-- set vairable for current session
SET hivevar:k3=v3;

-- get value for configuration
SET table.sql-dialect;
SET hiveconf:k1;
SET system:k2;
SET hivevar:k3;

-- only print Flink's configuration
SET;

-- print all configurations
SET -v;
```

{{< hint warning >}}
**Note:**
- In Hive, the `SET` command `SET xx=yy` whose key has no prefix is equivalent to `SET hiveconf:xx=yy`, which means it'll set it to Hive Conf.
  But in Flink, with Hive dialect, such `SET` command `set xx=yy` will set `xx` with value `yy` to Flink's configuration.
  So, if you want to set configuration to Hive's Conf, please add the prefix `hiveconf:`, using the  `SET` command like `SET hiveconf:xx=yy`.
- In Hive dialect, the `key`/`value` to be set shouldn't be quoted.
  {{< /hint  >}}
