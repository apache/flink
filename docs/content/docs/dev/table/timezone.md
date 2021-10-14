---
title: "Time Zone"
weight: 22
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

# Time Zone

Flink provides rich data types for Date and Time, including `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ`, `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND` (please see [Date and Time]({{< ref "docs/dev/table/types" >}}#date-and-time) for detailed information).
Flink supports setting time zone in session level (please see [table.local-time-zone]({{< ref "docs/dev/table/config">}}#table-local-time-zone) for detailed information).
These timestamp data types and time zone support of Flink make it easy to process business data across time zones.

## TIMESTAMP vs TIMESTAMP_LTZ

### TIMESTAMP type
 - `TIMESTAMP(p)` is an abbreviation for `TIMESTAMP(p) WITHOUT TIME ZONE`, the precision `p` supports range is from 0 to 9, 6 by default.
 - `TIMESTAMP` describes a timestamp represents year, month, day, hour, minute, second and fractional seconds.
 - `TIMESTAMP` can be specified from a string literal, e.g.
 ```sql
Flink SQL> SELECT TIMESTAMP '1970-01-01 00:00:04.001';
+-------------------------+
| 1970-01-01 00:00:04.001 |
+-------------------------+
```

### TIMESTAMP_LTZ type
 - `TIMESTAMP_LTZ(p)` is an abbreviation for `TIMESTAMP(p) WITH LOCAL TIME ZONE`, the precision `p` supports range is from 0 to 9, 6 by default.
 - `TIMESTAMP_LTZ` describes an absolute time point on the time-line, it stores a long value representing epoch-milliseconds and an int representing nanosecond-of-millisecond. The epoch time is measured from the standard Java epoch of `1970-01-01T00:00:00Z`. Every datum of `TIMESTAMP_LTZ` type is interpreted in the local time zone configured in the current session for computation and visualization.
 - `TIMESTAMP_LTZ` has no literal representation and thus can not specify from literal, it can derives from a long epoch time(e.g. The long time produced by Java `System.currentTimeMillis()`)

 ```sql
Flink SQL> CREATE VIEW T1 AS SELECT TO_TIMESTAMP_LTZ(4001, 3);
Flink SQL> SET 'table.local-time-zone' = 'UTC';
Flink SQL> SELECT * FROM T1;
+---------------------------+
| TO_TIMESTAMP_LTZ(4001, 3) |
+---------------------------+
|   1970-01-01 00:00:04.001 |
+---------------------------+

Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM T1;
+---------------------------+
| TO_TIMESTAMP_LTZ(4001, 3) |
+---------------------------+
|   1970-01-01 08:00:04.001 |
+---------------------------+
```

- `TIMESTAMP_LTZ` can be used in cross time zones business because the absolute time point (e.g. above `4001` milliseconds) describes a same instantaneous point in different time zones.
Giving a background that at a same time point, the `System.currentTimeMillis()` of all machines in the world returns same value (e.g. the `4001` milliseconds in above example), this is absolute time point meaning.

## Time Zone Usage
The local time zone defines current session time zone id. You can config the time zone in Sql Client or Applications.

{{< tabs "SQL snippets" >}}
{{< tab "SQL Client" >}}
```sql
-- set to UTC time zone
Flink SQL> SET 'table.local-time-zone' = 'UTC';

-- set to Shanghai time zone
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';

-- set to Los_Angeles time zone
Flink SQL> SET 'table.local-time-zone' = 'America/Los_Angeles';
```
{{< /tab >}}
{{< tab "Java" >}}
```java
 EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
 TableEnvironment tEnv = TableEnvironment.create(envSetting);

 // set to UTC time zone
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

// set to Shanghai time zone
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

// set to Los_Angeles time zone
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("America/Los_Angeles"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val envSetting = EnvironmentSettings.inStreamingMode()
val tEnv = TableEnvironment.create(envSetting)

// set to UTC time zone
tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

// set to Shanghai time zone
tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

// set to Los_Angeles time zone
tEnv.getConfig.setLocalTimeZone(ZoneId.of("America/Los_Angeles"))
```
{{< /tab >}}
{{< /tabs >}}

The session time zone is useful in Flink SQL, the main usages are:

### Decide time functions return value
The following time functions are influenced by the configured time zone:
* LOCALTIME
* LOCALTIMESTAMP
* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* CURRENT_ROW_TIMESTAMP()
* NOW()
* PROCTIME()


```sql
Flink SQL> SET 'sql-client.execution.result-mode' = 'tableau';
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
Flink SQL> SET 'table.local-time-zone' = 'UTC';
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
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView1;
```

```
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| LOCALTIME |          LOCALTIMESTAMP | CURRENT_DATE | CURRENT_TIME |       CURRENT_TIMESTAMP | CURRENT_ROW_TIMESTAMP() |                   NOW() |              PROCTIME() |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
|  23:18:36 | 2021-04-15 23:18:36.384 |   2021-04-15 |     23:18:36 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
```

### `TIMESTAMP_LTZ` string representation
The session timezone is used when represents a `TIMESTAMP_LTZ` value to string format, i.e print the value, cast the value to `STRING` type, cast the value to `TIMESTAMP`, cast a `TIMESTAMP` value to `TIMESTAMP_LTZ`:
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
Flink SQL> SET 'table.local-time-zone' = 'UTC';
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
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
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

## Time Attribute and Time Zone

Please see [Time Attribute]({{< ref "docs/dev/table/concepts/time_attributes">}}#time-attributes) for more information about time attribute.

### Processing Time and Time Zone
Flink SQL defines process time attribute by function `PROCTIME()`, the function return type is `TIMESTAMP_LTZ`.

{{< hint info >}}
Before Flink 1.13, the function return type of `PROCTIME()` is `TIMESTAMP`, and the return value is the `TIMESTAMP` in UTC time zone,
e.g. the wall-clock shows `2021-03-01 12:00:00` at Shanghai, however the `PROCTIME()` displays `2021-03-01 04:00:00` which is wrong.
Flink 1.13 fixes this issue and uses `TIMESTAMP_LTZ` type as return type of `PROCTIME()`, users don't need to deal time zone problems anymore.
{{< /hint >}}

The PROCTIME() always represents your local timestamp value, using TIMESTAMP_LTZ type can also support DayLight Saving Time well.

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
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
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
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
|            item |                      STRING | true  |     |        |           |
|       max_price |                      DOUBLE |  true |     |        |           |
+-----------------+-----------------------------+-------+-----+--------+-----------+
```

Use the following command to ingest data for `MyTable1` in a terminal:

```
> nc -lk 9999
A,1.1
B,1.2
A,1.8
B,2.5
C,3.8
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
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
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView3;
```

Returns the different window start, window end and window proctime compared to calculation in UTC timezone.
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
Processing time window is non-deterministic, so each run will get different windows and different aggregations. The above example is just for explaining how time zone affects processing time window.
{{< /hint >}}

### Event Time and Time Zone
Flink supports defining event time attribute on TIMESTAMP column and TIMESTAMP_LTZ column.

#### Event Time Attribute on TIMESTAMP
If the timestamp data in the source is represented as year-month-day-hour-minute-second, usually a string value without time-zone information, e.g. `2020-04-15 20:13:40.564`, it's recommended to define the event time attribute as a `TIMESTAMP` column:
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

Use the following command to ingest data for `MyTable2` in a terminal:

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
Flink SQL> SET 'table.local-time-zone' = 'UTC';
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
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView4;
```

Returns the same window start, window end and window rowtime compared to calculation in UTC timezone.
```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

#### Event Time Attribute on TIMESTAMP_LTZ
If the timestamp data in the source is represented as a epoch time, usually a long value, e.g. `1618989564564`, it's recommended to define event time attribute as a `TIMESTAMP_LTZ` column.
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

The input data of MyTable3 is:
```
A,1.1,1618495260000  # The corresponding utc timestamp is 2021-04-15 14:01:00
B,1.2,1618495320000  # The corresponding utc timestamp is 2021-04-15 14:02:00
A,1.8,1618495380000  # The corresponding utc timestamp is 2021-04-15 14:03:00
B,2.5,1618495440000  # The corresponding utc timestamp is 2021-04-15 14:04:00
C,3.8,1618495500000  # The corresponding utc timestamp is 2021-04-15 14:05:00
C,3.8,1618495860000  # The corresponding utc timestamp is 2021-04-15 14:11:00
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
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
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView5;
```

Returns the different window start, window end and window rowtime compared to calculation in UTC timezone.
```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    A |       1.8 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    B |       2.5 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

## Daylight Saving Time Support
Flink SQL supports defining time attributes on TIMESTAMP_LTZ column, base on this, Flink SQL gracefully uses TIMESTAMP and TIMESTAMP_LTZ type in window processing to support the Daylight Saving Time.


Flink use timestamp literal to split the window and assigns window to data according to the epoch time of the each row. It means Flink uses `TIMESTAMP` type for window start and window end (e.g. `TUMBLE_START` and `TUMBLE_END`), uses `TIMESTAMP_LTZ` for window time attribute (e.g. `TUMBLE_PROCTIME`, `TUMBLE_ROWTIME`).
Given an example of tumble window, the DaylightTime in Los_Angeles starts at time 2021-03-14 02:00:00:
```
long epoch1 = 1615708800000L; // 2021-03-14 00:00:00
long epoch2 = 1615712400000L; // 2021-03-14 01:00:00
long epoch3 = 1615716000000L; // 2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
long epoch4 = 1615719600000L; // 2021-03-14 04:00:00
```
The tumble window [2021-03-14 00:00:00,  2021-03-14 00:04:00] will collect 3 hours' data in Los_angele time zone, but it collect 4 hours' data in other non-DST time zones, what user to do is only define time attribute on TIMESTAMP_LTZ column.

All windows in Flink like Hop window, Session window, Cumulative window follow this way, and all operations in Flink SQL support TIMESTAMP_LTZ well, thus Flink gracefully supports the Daylight Saving Time zone.   


## Difference between Batch and Streaming Mode
The following time functions:
* LOCALTIME
* LOCALTIMESTAMP
* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* NOW()

Flink evaluates their values according to execution mode. They are evaluated for each record in streaming mode. But in batch mode, they are evaluated once as the query starts and uses the same result for every row.

The following time functions are evaluated for each record no matter in batch or streaming mode:

* CURRENT_ROW_TIMESTAMP()
* PROCTIME()

{{< top >}}
