---
title: "时区"
weight: 4
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

## 概述

Flink为Date和Time提供了丰富的数据类型, 包括`DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ`, `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND` (更多详情请参考 [Date and Time]({{< ref "docs/dev/table/types" >}}#date-and-time)).
Flink支持在session级别设置时区(更多详情请参考 [table.local-time-zone]({{< ref "docs/dev/table/config">}}#table-local-time-zone)).
Flink对多种时间类型和时区的支持使得跨时区的数据处理变得非常容易.

## TIMESTAMP vs TIMESTAMP_LTZ

### TIMESTAMP 类型
 - `TIMESTAMP(p)`是`TIMESTAMP(p) WITHOUT TIME ZONE`的简写, 精度`p`支持的范围是0-9, 默认是6.
 - `TIMESTAMP` 用于描述年, 月, 日, 小时, 分钟, 秒 和 小数秒对应的时间戳.
 - `TIMESTAMP` 可以通过一个字符串来指定, 例如:
 ```sql
Flink SQL> SELECT TIMESTAMP '1970-01-01 00:00:04.001';
+-------------------------+
| 1970-01-01 00:00:04.001 |
+-------------------------+
```

### TIMESTAMP_LTZ 类型
 - `TIMESTAMP_LTZ(p)`是`TIMESTAMP(p) WITH LOCAL TIME ZONE`的简写, 精度`p`支持的范围是0-9, 默认是6.
 - `TIMESTAMP_LTZ` 用于描述时间线上的绝对时间点, 使用long保存从epoch至今的毫秒数, 使用int保存毫秒中的纳秒数. epoch时间是从java的标准epoch时间`1970-01-01T00:00:00Z`开始计算. 在计算和可视化时, 每个`TIMESTAMP_LTZ`类型的数据都是使用的session中配置的时区.
 - `TIMESTAMP_LTZ` 没有字符串表达形式因此无法通过字符串来指定, 可以通过一个long类型的epoch时间来转化(例如: 通过Java来产生一个long类型epoch时间 `System.currentTimeMillis()`)

 ```sql
Flink SQL> CREATE VIEW T1 AS SELECT TO_TIMESTAMP_LTZ(4001, 3);
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT * FROM T1;
+---------------------------+
| TO_TIMESTAMP_LTZ(4001, 3) |
+---------------------------+
|   1970-01-01 00:00:04.001 |
+---------------------------+

Flink SQL> SET table.local-time-zone=Asia/Shanghai;
Flink SQL> SELECT * FROM T1;
+---------------------------+
| TO_TIMESTAMP_LTZ(4001, 3) |
+---------------------------+
|   1970-01-01 08:00:04.001 |
+---------------------------+
```

- `TIMESTAMP_LTZ`可以用于跨时区的计算, 因为它是一个基于epoch的绝对时间点(例如:  超过 `4001` 毫秒) 代表的就是不同时区的同一个瞬时时间点.
用一个场景来描述就是: 在同一个时间点上, 全世界所有的机器上执行`System.currentTimeMillis()`都会返回同样的值. (比如上例中的 `4001` milliseconds), 这就是绝对时间的定义.

## 时区的用法
本地时区定义了当前session所在的时区id. 你可以在Sql client或者Applications中定义.

{{< tabs "SQL snippets" >}}
{{< tab "SQL Client" >}}
```sql
-- 设置为 UTC 时区
Flink SQL> SET table.local-time-zone=UTC;

-- 设置为上海时区
Flink SQL> SET table.local-time-zone=Asia/Shanghai;

-- 设置为Los_Angeles时区
Flink SQL> SET table.local-time-zone=America/Los_Angeles;
```
{{< /tab >}}
{{< tab "Java" >}}
```java
 EnvironmentSettings envSetting = EnvironmentSettings.newInstance().build();
 TableEnvironment tEnv = TableEnvironment.create(envSetting);

 // 设置为UTC时区
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

// 设置为上海时区
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

// 设置为Los_Angeles时区
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("America/Los_Angeles"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val envSetting = EnvironmentSettings.newInstance.build
val tEnv = TableEnvironment.create(envSetting)

// 设置为UTC时区
tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

// 设置为上海时区
tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

// 设置为Los_Angeles时区
tEnv.getConfig.setLocalTimeZone(ZoneId.of("America/Los_Angeles"))
```
{{< /tab >}}
{{< /tabs >}}

session的时区设置在Flink SQL中非常有用, 它的主要用法如下:

### 确定时间函数的返回值
session中配置的时区会对以下函数生效.
* LOCALTIME
* LOCALTIMESTAMP
* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* CURRENT_ROW_TIMESTAMP()
* NOW()
* PROCTIME()


```sql
Flink SQL> SET sql-client.execution.result-mode=tableau;
Flink SQL> CREATE VIEW MyView1 AS SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME();
Flink SQL> DESC MyView1;
```

```
+------------------------+-----------------------------+-------+-----+--------+-----------+
|                   name |                        type |  null | key | extras | watermark |
+------------------------+-----------------------------+-------+-----+--------+-----------+
|              LOCALTIME |                     TIME(0) | false |     |        |           |
|         LOCALTIMESTAMP |                TIMESTAMP(3) | false |     |        |           |
|           CURRENT_DATE |                        DATE | false |     |        |           |
|           CURRENT_TIME |                     TIME(0) | false |     |        |           |
|      CURRENT_TIMESTAMP |            TIMESTAMP_LTZ(3) | false |     |        |           |
|CURRENT_ROW_TIMESTAMP() |            TIMESTAMP_LTZ(3) | false |     |        |           |
|                  NOW() |            TIMESTAMP_LTZ(3) | false |     |        |           |
|             PROCTIME() | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |        |           |
+------------------------+-----------------------------+-------+-----+--------+-----------+
```

```sql
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT * FROM MyView1;
```

```
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| LOCALTIME |          LOCALTIMESTAMP | CURRENT_DATE | CURRENT_TIME |       CURRENT_TIMESTAMP | CURRENT_ROW_TIMESTAMP() |                   NOW() |              PROCTIME() |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
|  15:18:36 | 2021-04-15 15:18:36.384 |   2021-04-15 |     15:18:36 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai;
Flink SQL> SELECT * FROM MyView1;
```

```
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| LOCALTIME |          LOCALTIMESTAMP | CURRENT_DATE | CURRENT_TIME |       CURRENT_TIMESTAMP | CURRENT_ROW_TIMESTAMP() |                   NOW() |              PROCTIME() |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
|  23:18:36 | 2021-04-15 23:18:36.384 |   2021-04-15 |     23:18:36 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
```

### `TIMESTAMP_LTZ` 字符串表示
当一个 `TIMESTAMP_LTZ` 值转为string格式时, session中配置的时区会生效. 例如打印这个值，将类型强制转化为 `STRING` 类型, 将类型强制转换为 `TIMESTAMP`, 将 `TIMESTAMP` 的值转化为 `TIMESTAMP_LTZ` 类型:
```sql
Flink SQL> CREATE VIEW MyView2 AS SELECT TO_TIMESTAMP_LTZ(4001, 3) AS ltz, TIMESTAMP '1970-01-01 00:00:01.001'  AS ntz;
Flink SQL> DESC MyView2;
```

```
+------+------------------+-------+-----+--------+-----------+
| name |             type |  null | key | extras | watermark |
+------+------------------+-------+-----+--------+-----------+
|  ltz | TIMESTAMP_LTZ(3) |  true |     |        |           |
|  ntz |     TIMESTAMP(3) | false |     |        |           |
+------+------------------+-------+-----+--------+-----------+
```

```sql
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT * FROM MyView2;
```

```
+-------------------------+-------------------------+
|                     ltz |                     ntz |
+-------------------------+-------------------------+
| 1970-01-01 00:00:04.001 | 1970-01-01 00:00:01.001 |
+-------------------------+-------------------------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai;
Flink SQL> SELECT * FROM MyView2;
```

```
+-------------------------+-------------------------+
|                     ltz |                     ntz |
+-------------------------+-------------------------+
| 1970-01-01 08:00:04.001 | 1970-01-01 00:00:01.001 |
+-------------------------+-------------------------+
```

```sql
Flink SQL> CREATE VIEW MyView3 AS SELECT ltz, CAST(ltz AS TIMESTAMP(3)), CAST(ltz AS STRING), ntz, CAST(ntz AS TIMESTAMP_LTZ(3)) FROM MyView2;
```

```
Flink SQL> DESC MyView3;
+-------------------------------+------------------+-------+-----+--------+-----------+
|                          name |             type |  null | key | extras | watermark |
+-------------------------------+------------------+-------+-----+--------+-----------+
|                           ltz | TIMESTAMP_LTZ(3) |  true |     |        |           |
|     CAST(ltz AS TIMESTAMP(3)) |     TIMESTAMP(3) |  true |     |        |           |
|           CAST(ltz AS STRING) |           STRING |  true |     |        |           |
|                           ntz |     TIMESTAMP(3) | false |     |        |           |
| CAST(ntz AS TIMESTAMP_LTZ(3)) | TIMESTAMP_LTZ(3) | false |     |        |           |
+-------------------------------+------------------+-------+-----+--------+-----------+
```

```sql
Flink SQL> SELECT * FROM MyView3;
```

```
+-------------------------+---------------------------+-------------------------+-------------------------+-------------------------------+
|                     ltz | CAST(ltz AS TIMESTAMP(3)) |     CAST(ltz AS STRING) |                     ntz | CAST(ntz AS TIMESTAMP_LTZ(3)) |
+-------------------------+---------------------------+-------------------------+-------------------------+-------------------------------+
| 1970-01-01 08:00:04.001 |   1970-01-01 08:00:04.001 | 1970-01-01 08:00:04.001 | 1970-01-01 00:00:01.001 |       1970-01-01 00:00:01.001 |
+-------------------------+---------------------------+-------------------------+-------------------------+-------------------------------+
```

## 时间属性和时区
更多时间属性相关的详细介绍, 请参考 [Time Attribute]({{< ref "docs/dev/table/concepts/time_attributes">}}#时间属性).

### 处理时间和时区
Flink SQL 使用函数 `PROCTIME()` 来定义处理时间属性, 该函数返回的类型是 `TIMESTAMP_LTZ`.

{{< hint info >}}
在Flink1.13之前, `PROCTIME()` 函数返回的类型是 `TIMESTAMP`, 返回值是UTC时区下的 `TIMESTAMP`.
例如: 当上海的时间为 `2021-03-01 12:00:00` 时, `PROCTIME()` 显示的时间却是`2021-03-01 04:00:00`, 因此是错的.
这个问题在Flink 1.13中修复了, 因此用户不用再去处理时区的问题了. 
{{< /hint >}}

`PROCTIME()` 返回的时本地时区的时间, 使用 `TIMESTAMP_LTZ` 类型也可以支持夏令时时间.

```sql
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT PROCTIME();
```
```
+-------------------------+
|              PROCTIME() |
+-------------------------+
| 2021-04-15 14:48:31.387 |
+-------------------------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai;
Flink SQL> SELECT PROCTIME();
```
```
+-------------------------+
|              PROCTIME() |
+-------------------------+
| 2021-04-15 22:48:31.387 |
+-------------------------+
```

```sql
Flink SQL> CREATE TABLE MyTable1 (
                  item STRING,
                  price DOUBLE,
                  proctime as PROCTIME()
            ) WITH (
                'connector' = 'socket',
                'hostname' = '127.0.0.1',
                'port' = '9999',
                'format' = 'csv'
           );

Flink SQL> CREATE VIEW MyView3 AS
            SELECT
                TUMBLE_START(proctime, INTERVAL '10' MINUTES) AS window_start,
                TUMBLE_END(proctime, INTERVAL '10' MINUTES) AS window_end,
                TUMBLE_PROCTIME(proctime, INTERVAL '10' MINUTES) as window_proctime,
                item,
                MAX(price) as max_price
            FROM MyTable1
                GROUP BY TUMBLE(proctime, INTERVAL '10' MINUTES), item;

Flink SQL> DESC MyView3;
```

```
+-----------------+-----------------------------+-------+-----+--------+-----------+
|           name  |                        type |  null | key | extras | watermark |
+-----------------+-----------------------------+-------+-----+--------+-----------+
|    window_start |                TIMESTAMP(3) | false |     |        |           |
|      window_end |                TIMESTAMP(3) | false |     |        |           |
| window_proctime | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |        |           |
|            item |                      STRING | true |     |        |           |
|       max_price |                      DOUBLE |  true |     |        |           |
+-----------------+-----------------------------+-------+-----+--------+-----------+
```

在终端执行以下命令写入数据到 `MyTable1` 

```
> nc -lk 9999
A,1.1
B,1.2
A,1.8
B,2.5
C,3.8
```

```sql
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT * FROM MyView3;
```

```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_procime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:10:00.005 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:10:00.007 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:10:00.007 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai;
Flink SQL> SELECT * FROM MyView3;
```

返回和UTC时区计算下不同的窗口开始时间, 窗口结束时间和窗口处理时间.
```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_procime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.005 |    A |       1.8 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.007 |    B |       2.5 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.007 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

{{< hint info >}}
处理时间窗口是不确定的, 每次运行都会返回不同的窗口和聚合结果. 以上的示例只用于说明时区如何影响处理时间窗口.
{{< /hint >}}

### 事件时间和时区
Flink支持在 `TIMESTAMP` 列和 `TIMESTAMP_LTZ` 列上定义时间属性.

#### TIMESTAMP 上的事件时间属性
如果source中的时间用于表示年-月-日-小时-分钟-秒, 通常是一个不带时区的字符串, 例如: `2020-04-15 20:13:40.564`. 通常建议在一个 `TIMESTAMP` 列上定义事件时间属性.
```sql
Flink SQL> CREATE TABLE MyTable2 (
                  item STRING,
                  price DOUBLE,
                  ts TIMESTAMP(3), -- TIMESTAMP data type
                  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
            ) WITH (
                'connector' = 'socket',
                'hostname' = '127.0.0.1',
                'port' = '9999',
                'format' = 'csv'
           );

Flink SQL> CREATE VIEW MyView4 AS
            SELECT
                TUMBLE_START(ts, INTERVAL '10' MINUTES) AS window_start,
                TUMBLE_END(ts, INTERVAL '10' MINUTES) AS window_end,
                TUMBLE_ROWTIME(ts, INTERVAL '10' MINUTES) as window_rowtime,
                item,
                MAX(price) as max_price
            FROM MyTable2
                GROUP BY TUMBLE(ts, INTERVAL '10' MINUTES), item;

Flink SQL> DESC MyView4;
```

```
+----------------+------------------------+------+-----+--------+-----------+
|           name |                   type | null | key | extras | watermark |
+----------------+------------------------+------+-----+--------+-----------+
|   window_start |           TIMESTAMP(3) | true |     |        |           |
|     window_end |           TIMESTAMP(3) | true |     |        |           |
| window_rowtime | TIMESTAMP(3) *ROWTIME* | true |     |        |           |
|           item |                 STRING | true |     |        |           |
|      max_price |                 DOUBLE | true |     |        |           |
+----------------+------------------------+------+-----+--------+-----------+
```

在终端执行以下命令用于写入数据到 `MyTable2`:

```
> nc -lk 9999
A,1.1,2021-04-15 14:01:00
B,1.2,2021-04-15 14:02:00
A,1.8,2021-04-15 14:03:00 
B,2.5,2021-04-15 14:04:00
C,3.8,2021-04-15 14:05:00       
C,3.8,2021-04-15 14:11:00
```

```sql
Flink SQL> SET table.local-time-zone=UTC; 
Flink SQL> SELECT * FROM MyView4;
```
               
```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai; 
Flink SQL> SELECT * FROM MyView4;
```

返回和在UTC时区下计算时相同的窗口开始时间, 窗口结束时间和窗口的rowtime.
```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

#### TIMESTAMP_LTZ 上的事件时间属性
如果source中的时间为一个epoch时间, 通常是一个long值, 例如: `1618989564564`, 建议将event time属性定义为 `TIMESTAMP_LTZ` 列.
```sql
Flink SQL> CREATE TABLE MyTable3 (
                  item STRING,
                  price DOUBLE,
                  ts BIGINT, -- long time value in epoch milliseconds
                  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
                  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '10' SECOND
            ) WITH (
                'connector' = 'socket',
                'hostname' = '127.0.0.1',
                'port' = '9999',
                'format' = 'csv'
           );

Flink SQL> CREATE VIEW MyView5 AS 
            SELECT 
                TUMBLE_START(ts_ltz, INTERVAL '10' MINUTES) AS window_start,        
                TUMBLE_END(ts_ltz, INTERVAL '10' MINUTES) AS window_end,
                TUMBLE_ROWTIME(ts_ltz, INTERVAL '10' MINUTES) as window_rowtime,
                item,
                MAX(price) as max_price
            FROM MyTable3
                GROUP BY TUMBLE(ts_ltz, INTERVAL '10' MINUTES), item;

Flink SQL> DESC MyView5;
```

```
+----------------+----------------------------+-------+-----+--------+-----------+
|           name |                       type |  null | key | extras | watermark |
+----------------+----------------------------+-------+-----+--------+-----------+
|   window_start |               TIMESTAMP(3) | false |     |        |           |
|     window_end |               TIMESTAMP(3) | false |     |        |           |
| window_rowtime | TIMESTAMP_LTZ(3) *ROWTIME* |  true |     |        |           |
|           item |                     STRING |  true |     |        |           |
|      max_price |                     DOUBLE |  true |     |        |           |
+----------------+----------------------------+-------+-----+--------+-----------+
```

`MyTable3`的输入数据为:
```
A,1.1,1618495260000  # The corresponding utc timestamp is 2021-04-15 14:01:00
B,1.2,1618495320000  # The corresponding utc timestamp is 2021-04-15 14:02:00
A,1.8,1618495380000  # The corresponding utc timestamp is 2021-04-15 14:03:00
B,2.5,1618495440000  # The corresponding utc timestamp is 2021-04-15 14:04:00
C,3.8,1618495500000  # The corresponding utc timestamp is 2021-04-15 14:05:00       
C,3.8,1618495860000  # The corresponding utc timestamp is 2021-04-15 14:11:00
```    

```sql
Flink SQL> SET table.local-time-zone=UTC; 
Flink SQL> SELECT * FROM MyView5;
```                         
               
```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai; 
Flink SQL> SELECT * FROM MyView5;
```

返回和UTC时区下计算时的不同的窗口开始时间, 窗口结束时间和窗口的rowtime.
```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    A |       1.8 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    B |       2.5 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

## 夏令时支持
Flink SQL支持在 `TIMESTAMP_LTZ`列上定义时间属性, 因此Flink SQL可以在窗口中优雅地使用 `TIMESTAMP` 和 `TIMESTAMP_LTZ` 类型来支持夏令时.
   
Flink 使用时间的字符格式来分割窗口并通过row的epoch时间来分配窗口. 这意味着Flink窗口开始时间和窗口结束时间使用的是 `TIMESTAMP` 类型(例如: `TUMBLE_START` 和 `TUMBLE_END`), 将 `TIMESTAMP_LTZ`类型用于窗口的时间属性(例如: `TUMBLE_PROCTIME`, `TUMBLE_ROWTIME`).
给定一个tumble window示例, 在Los_Angele时区下夏令时从 `2021-03-14 02:00:00` 开始:
```
long epoch1 = 1615708800000L; // 2021-03-14 00:00:00
long epoch2 = 1615712400000L; // 2021-03-14 01:00:00
long epoch3 = 1615716000000L; // 2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
long epoch4 = 1615719600000L; // 2021-03-14 04:00:00 
```
在Los_angele时区下, tumble window [2021-03-14 00:00:00,  2021-03-14 00:04:00] 将会收集3个小时的数据, 在其他非DST的时区下将会收集4个小时的数据, 用户只需要在 `TIMESTAMP_LTZ` 列上声明时间属性即可.

Flink的所有窗口(如Hop window, Session window, Cumulative window)都会遵循这种方式, Flink SQL中的所有操作都很好的支持了 `TIMESTAMP_LTZ` ,  因此Flink可以非常优雅的支持夏令时.   


## Batch模式和Streaming模式的区别
以下函数: 
* LOCALTIME
* LOCALTIMESTAMP
* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* NOW()

Flink 会根据执行模式来进行不同计算. 在Streaming模式下会为每条记录都计算出一个值, 但在Batch模式下, 只会在query开始时计算一次, 所有行都使用相同的结果. 

以下时间函数无论是在Streaming模式还是Batch模式下, 都会为每条记录计算一次结果:

* CURRENT_ROW_TIMESTAMP()
* PROCTIME() 

{{< top >}}
