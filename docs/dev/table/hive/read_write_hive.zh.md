---
title: "读写 Hive 表"
nav-parent_id: hive_tableapi
nav-pos: 2
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

通过 `HiveCatalog` 和 Flink 的 Hive 连接器, Flink 可替代 Hive 作为执行引擎读写 Hive 表。
请确保你的应用程序已按照指示包含了正确的依赖[dependencies]({{ site.baseurl }}/zh/dev/table/hive/#depedencies)。
另外请注意目前 Hive 连接器仅被 blink 计划器所支持。

* This will be replaced by the TOC
{:toc}

## Hive 读取

假设在 Hive 的 default 数据库中，存在一个包含几行数据的 people 表。

{% highlight bash %}
hive> show databases;
OK
default
Time taken: 0.841 seconds, Fetched: 1 row(s)

hive> show tables;
OK
Time taken: 0.087 seconds

hive> CREATE TABLE mytable(name string, value double);
OK
Time taken: 0.127 seconds

hive> SELECT * FROM mytable;
OK
Tom   4.72
John  8.0
Tom   24.2
Bob   3.14
Bob   4.72
Tom   34.9
Mary  4.79
Tiff  2.72
Bill  4.33
Mary  77.7
Time taken: 0.097 seconds, Fetched: 10 row(s)
{% endhighlight %}

准备好数据后，你可以连接到 Hive ({{ site.baseurl }}/zh/dev/table/hive/#connecting-to-hive) 并执行查询操作。

{% highlight bash %}

Flink SQL> show catalogs;
myhive
default_catalog

# ------ Set the current catalog to be 'myhive' catalog if you haven't set it in the yaml file ------

Flink SQL> use catalog myhive;

# ------ See all registered database in catalog 'mytable' ------

Flink SQL> show databases;
default

# ------ See the previously registered table 'mytable' ------

Flink SQL> show tables;
mytable

# ------ The table schema that Flink sees is the same that we created in Hive, two columns - name as string and value as double ------ 
Flink SQL> describe mytable;
root
 |-- name: name
 |-- type: STRING
 |-- name: value
 |-- type: DOUBLE

# ------ Select from hive table or hive view ------ 
Flink SQL> SELECT * FROM mytable;

   name      value
__________ __________

    Tom      4.72
    John     8.0
    Tom      24.2
    Bob      3.14
    Bob      4.72
    Tom      34.9
    Mary     4.79
    Tiff     2.72
    Bill     4.33
    Mary     77.7

{% endhighlight %}

### 查询 Hive 视图 

如果你需要查询 Hive 视图，请注意：

1. 在查询 Hive 视图之前，你必须将该视图所在的 Hive catalog 作为你当前使用的 catalog。你可以使用 Table API 中的 `tableEnv.useCatalog(...)` 或者 SQL client中的 `USE CATALOG ...` 完成该操作。
2. Hive 和 Flink SQL 存在不同的语法，例如不同的保留关键字和字面意思。请确保视图的查询语句与 Flink 语法兼容。

## Hive 写入

同样，通过 `INSERT` 语句数据能够被写入 Hive。

假设有一个名叫 "mytable" 的表，其中拥有 name 和 age 两列，分别为字符串类型和整数类型。

{% highlight bash %}
# ------ INSERT INTO will append to the table or partition, keeping the existing data intact ------ 
Flink SQL> INSERT INTO mytable SELECT 'Tom', 25;

# ------ INSERT OVERWRITE will overwrite any existing data in the table or partition ------ 
Flink SQL> INSERT OVERWRITE mytable SELECT 'Tom', 25;
{% endhighlight %}

我们也支持写入分区表, 假设有一个名叫 myparttable 的分区表，该表拥有 4 列，分别是name、age、my_type、my_date，其中 my_type 和 my_date 是分区键。

{% highlight bash %}
# ------ Insert with static partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1', my_date='2019-08-08') SELECT 'Tom', 25;

# ------ Insert with dynamic partition ------ 
Flink SQL> INSERT OVERWRITE myparttable SELECT 'Tom', 25, 'type_1', '2019-08-08';

# ------ Insert with static(my_type) and dynamic(my_date) partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1') SELECT 'Tom', 25, '2019-08-08';
{% endhighlight %}


## 格式

我们已经测试了以下表存储格式: text, csv, SequenceFile, ORC, and Parquet。


## 优化

### 分区修剪

当查询 Hive 表时，Flink 使用分区修剪作为一项优化手段来限制文件和分区的读入数量。
对数据进行分区后，当查询与某些过滤条件匹配时，
Flink 只会读取分区的部分子集。

### 投影下推

Flink 利用投影下推功能，通过在表扫描时过滤不必要的字段来
最大程度减少 Flink 和 Hive 之间的数据传输。

当一个表中包含大量字段时，该优化能大大提高效率。

### Limit 下推

对于使用了 LIMIT 子句的查询，Flink 将会限制输出数据量，
最大程度的减少网络之间传输的数据量。

### 读取时进行 ORC 矢量优化

当满足以下条件时，将自动使用该优化：

- 没有复杂数据类型的列, 譬如 hive 类型中的: List, Map, Struct, Union。
- Hive 版本大于等于 2.0.0.

此功能默认打开。如果有问题，可以选择使用以下配置选项关闭 ORC 矢量优化：

{% highlight bash %}
table.exec.hive.fallback-mapred-reader=true
{% endhighlight %}


## 未来规划

我们正在计划并积极推动以支持以下功能：

- ACID tables
- bucketed tables
- more formats

请与社区联系以获取更多功能需求 https://flink.apache.org/community.html#mailing-lists。
