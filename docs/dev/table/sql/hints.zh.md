---
title: "SQL Hints"
nav-parent_id: sql
nav-pos: 6
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

* This will be replaced by the TOC
{:toc}

## Dynamic Table Options
Dynamic table options allows to specify or override table options dynamically, different with static table options defined with SQL DDL or connect API, 
these options can be specified flexibly in per-table scope within each query.

Thus it is very suitable to use for the ad-hoc queries in interactive terminal, for example, in the SQL-CLI,
you can specify to ignore the parse error for a CSV source just by adding a dynamic option `/*+ OPTIONS('csv.ignore-parse-errors'='true') */`.

Note: Dynamic table options default is forbidden to use because it may change the semantics of the query.
You need to set the config option `table.dynamic-table-options.enabled` to be `true` explicitly (default is false),
See the <a href="{{ site.baseurl }}/dev/table/config.html">Configuration</a> for details on how to set up the config options.</p>

### Syntax
In order to not break the SQL compatibility, we use a Oracle style SQL hint syntax:
{% highlight sql %}
table_path /*+ OPTIONS(key=val [, key=val]*) */

key:
    stringLiteral
val:
    stringLiteral

{% endhighlight %}

#### Examples

{% highlight sql %}

CREATE TABLE csv_table1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE csv_table2 (id BIGINT, name STRING, age INT) WITH (...);

-- override table options in query source
select id, name from csv_table1 /*+ OPTIONS('csv.field-delimiter'='|') */;

-- override table options in join
select * from
    csv_table1 /*+ OPTIONS('csv.field-delimiter'='|') */ t1
    join
    csv_table2 /*+ OPTIONS('csv.field-delimiter'=',') */ t2
    on t1.id = t2.id;

-- override table options for INSERT target table
insert into csv_table1 /*+ OPTIONS('csv.null-literal'='N/A') */ select * from csv_table2;

{% endhighlight %}

{% top %}