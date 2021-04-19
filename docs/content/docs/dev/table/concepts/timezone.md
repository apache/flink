---
title: "Timestamp DataTypes and Time Zone Support"
weight: 4
type: docs
aliases:
  - /dev/table/streaming/versioned_tables.html
  - /dev/table/streaming/temporal_tables.html
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

## Overview

Flink provides rich DataTypes for Date and Time, includes `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ`, `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`(please see [Date and Time]({{< ref "docs/dev/table/types" >}}#date-and-time) for detail information).
Flink supports setting time zone in session level (please see[table.local-time-zone]({{< ref "docs/dev/table/config">}}#table-local-time-zone) for detail information).
These Timestamp DataTypes and Time Zone Support of Flink makes business data processing cross time zones easy. 

## TIMESTAMP DataType and TIMESTAMP_LTZ DataType

### TIMESTAMP DataType
 - `TIMESTAMP(p)` is an abbreviation for `TIMESTAMP(p) WITHOUT TIME ZONE`, the precision `p` supports range is from 0 to 9, 6 by default.
 - `TIMESTAMP` describes a timestamp literal which stores year, month, day, hour, minute, second, fractional seconds.
 - `TIMESTAMP` can specify from a literal e.g.
 ```sql
Flink SQL> SELECT TIMESTAMP '1970-01-01 00:00:04.001';
+----+-------------------------+
| +I | 1970-01-01 00:00:04.001 |
+----+-------------------------+
```

### TIMESTAMP_LTZ DataType
 - `TIMESTAMP_LTZ(p)` is an abbreviation for `TIMESTAMP(p) WITH LOCAL TIME ZONE`, the precision `p` supports range is from 0 to 9, 6 by default.
 - `TIMESTAMP_LTZ` describes a absolute time point and stores only a long epoch time, it requires the session time zone when normalized(e.g select to console).
 - `TIMESTAMP_LTZ` has no literal representation and thus can not specify from literal, it can derives from a long epoch time(e.g. The long time produced by Java `System.currentTimeMillis()`)
 ```sql
Flink SQL> CREATE VIEW T1 AS SELECT 'flink' as a, 5 as b, TO_TIMESTAMP_LTZ(4001, 3) as c;
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT * FROM T1;
+----+----------------------+-------------+-------------------------+
| op |                    a |           b |                       c |
+----+----------------------+-------------+-------------------------+
| +I |                 flink |           5 | 1970-01-01 00:00:04.001 |
+----+----------------------+-------------+-------------------------+
 
Flink SQL> SET table.local-time-zone=Asia/Shanghai;
Flink SQL> SELECT * FROM T1;
+----+----------------------+-------------+-------------------------+
| op |                    a |           b |                       c |
+----+----------------------+-------------+-------------------------+
| +I |                 flink |           5 | 1970-01-01 08:00:04.001 |
+----+----------------------+-------------+-------------------------+
```
- `TIMESTAMP_LTZ` can used in cross time zones business because the absolute time point(above `4001` milliseconds) in different time zones is same. 
Giving a background that at a same time point, the `System.currentTimeMillis()` of all machines in the world returns same value(e.g. the `4001` milliseconds in above example.), this is absolute time point meaning.
The local time zone is used to normalize the long epoch time to timestamp literal, that's the `WITH LOCAL TIME ZONE` meaning in the type `TIMESTAMP(p) WITH LOCAL TIME ZONE`.

## Time Zone Usage
The local time zone defines current session time zone id. You can config the time zone in Sql Client or Applications.

**In Sql Client**
```sql
Flink SQL> SET table.local-time-zone=UTC;                  # set to UTC time zone
Flink SQL> SET table.local-time-zone=Asia/Shanghai;        # set to Shanghai time zone
Flink SQL> SET table.local-time-zone=America/Los_Angeles;  # set to Los_Angeles time zone
```

**In java Application**
```java
 EnvironmentSettings envSetting = EnvironmentSettings.newInstance().build();
 TableEnvironment tEnv = TableEnvironment.create(envSetting); 

 tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));                  // set to UTC time zone
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));        // set to Shanghai time zone
 tEnv.getConfig().setLocalTimeZone(ZoneId.of("America/Los_Angeles"));  // set to Los_Angeles time zone
```


The session time zone is useful in Flink SQL, the main usages are: 
### 1. Decide time functions return value
The following time functions is influenced by the configured time zone.
* LOCALTIME
* LOCALTIMESTAMP
* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* CURRENT_ROW_TIMESTAMP()
* NOW()
* PROCTIME()


```sql
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME();
```

```
+----+--------------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| op | LOCALTIME    |          LOCALTIMESTAMP | CURRENT_DATE | CURRENT_TIME |       CURRENT_TIMESTAMP |                  EXPR$5 |                  EXPR$6 |                  EXPR$7 |
+----+--------------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| +I | 15:18:36.384 | 2021-04-15 15:18:36.384 |   2021-04-15 | 15:18:36.384 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 |
+----+--------------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai;
Flink SQL> SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME();
```

```
+----+--------------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| op | LOCALTIME    |          LOCALTIMESTAMP | CURRENT_DATE | CURRENT_TIME |       CURRENT_TIMESTAMP |                  EXPR$5 |                  EXPR$6 |                  EXPR$7 |
+----+--------------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| +I | 23:17:09.326 | 2021-04-15 23:17:09.326 |   2021-04-15 | 23:17:09.326 | 2021-04-15 23:17:09.326 | 2021-04-15 23:17:09.326 | 2021-04-15 23:17:09.326 | 2021-04-15 23:17:09.326 |
+----+--------------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
```

### 2. String representation of `TIMESTAMP_LTZ` data value
The session timezone is used when represents a `TIMESTAMP_LTZ` value to string format, i.e print the value, cast the value to `STRING` type, cast the value to `TIMESTAMP`, cast a `TIMESTAMP` value to `TIMESTAMP_LTZ`:
```sql
Flink SQL> CREATE VIEW T1 AS SELECT 'flink' AS a, 5 AS b, TO_TIMESTAMP_LTZ(4001, 3) AS c, TIMESTAMP '1970-01-01 00:00:01.001'  AS d;
Flink SQL> SET table.local-time-zone=UTC;
Flink SQL> SELECT * FROM T1;
```

```
+----+----------------------+-------------+-------------------------+-------------------------+
| op |                    a |           b |                       c |                       d |
+----+----------------------+-------------+-------------------------+-------------------------+
| +I |                 flink |           5 | 1970-01-01 00:00:04.001 | 1970-01-01 00:00:01.001 |
+----+----------------------+-------------+-------------------------+-------------------------+
```

```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai; 
Flink SQL> SELECT * FROM T1;
```
```
+----+----------------------+-------------+-------------------------+-------------------------+
| op |                    a |           b |                       c |                       d |
+----+----------------------+-------------+-------------------------+-------------------------+
| +I |                 flink |           5 | 1970-01-01 08:00:04.001 | 1970-01-01 00:00:01.001 |
+----+----------------------+-------------+-------------------------+-------------------------+
```

```sql
Flink SQL> SELECT c, CAST(c AS TIMESTAMP(3)), CAST(c as STRING), CAST(d AS TIMESTAMP_LTZ(3)) FROM T1;
```

```
+----+-------------------------+-------------------------+-------------------------+-------------------------+
| op |                       c |                  EXPR$1 |               EXPR$2    |                  EXPR$3 |
+----+-------------------------+-------------------------+-------------------------+-------------------------+
| +I | 1970-01-01 08:00:04.001 | 1970-01-01 08:00:04.001 | 1970-01-01 08:00:04.001 | 1970-01-01 00:00:01.001 |
+----+-------------------------+-------------------------+-------------------------+-------------------------+
```

## Time Attribute and Time Zone

Please see [Time Attribute]({{< ref "docs/dev/table/concepts/time_attributes">}}#date-and-time) for more information about time attribute.

### Processing Time Attribute and Time Zone
Flink SQL definine process time attribute by function PROCTIME(), the function return type is TIMESTAMP_LTZ.

**NOTE**
Before Flink 1.13, the function return type of PROCTIME() is TIMESTAMP, and the return value is the TIMESTAMP in UTC time zone,
E.g. the PROCTIME() returns `2021-03-01 04:00:00` when user is in Shanghai and current timestamp is `2021-03-01 12:00:00 (UTC+8)`. 
Flin 1.13 fixes this issue and uses TIMESTAMP_LTZ type as return type of PROCTIME(), users don't need to deal time zone proctime anymore.

The PROCTIME() always represents your local timestamp value, using TIMESTAMP_LTZ type can also support DayLight Saving Time well. 

```sql
Flink SQL> SET table.local-time-zone=UTC; 
Flink SQL> SELECT PROCTIME();
```
```
+----+-------------------------+
| op |                  EXPR$0 |
+----+-------------------------+
| +I | 2021-04-15 14:48:31.387 |
+----+-------------------------+
```

```sql
Flink SQL> SET table.local-time-zone=UTC+8; 
Flink SQL> SELECT PROCTIME();
```
```
+----+-------------------------+
| op |                  EXPR$0 |
+----+-------------------------+
| +I | 2021-04-15 22:48:34.567 |
+----+-------------------------+
```
```sql
Flink SQL> CREATE TABLE src (
             a INT,
             b DOUBLE,
             proctime as PROCTIME()
           ) WITH (
             'connector' = 'kafka',
             ...);
Flink SQL>  SELECT 
                TUMBLE_START(proctime, INTERVAL '0.003' SECOND),
                TUMBLE_END(proctime, INTERVAL '0.003' SECOND),
                TUMBLE_PROCTIME(proctime, INTERVAL '0.003' SECOND),
                COUNT(a),
                SUM(b)
            FROM src
            GROUP BY TUMBLE(proctime, INTERVAL '0.003' SECOND);
 -- the TUMBLE_START    column will be close to 2021-04-15 22:48:34.567, the type of TUMBLE_START    column is TIMESTAMP
 -- the TUMBLE_END      column will be close to 2021-04-15 22:48:34.570, the type of TUMBLE_END      column is TIMESTAMP      
 -- the TUMBLE_PROCTIME column will be close to 2021-04-15 22:48:34.570, the type of TUMBLE_PROCTIME column is TIMESTAMP_LTZ      
          
```

### Event Time Attribute and Time Zone
Flink supports defining event time attribute on TIMESTAMP column and TIMESTAMP_LTZ column. It's recommended to defining event time attribute on TIMESTAMP column if the data source contains TIMESTAMP literal.  
```sql
Flink SQL> CREATE TABLE src (
             ts TIMESTAMP(3), -- TIMESTAMP DataType
             a INT,
             b DOUBLE,
             WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
           ) WITH (
             'connector' = 'kafka',
                          ...);
Flink SQL>  SELECT
                TUMBLE_START(ts, INTERVAL '0.003' SECOND), 
                TUMBLE_END(ts, INTERVAL '0.003' SECOND),
                TUMBLE_ROWTIME(ts, INTERVAL '0.003' SECOND),
                COUNT(a),
                SUM(b)
            FROM src
            GROUP BY TUMBLE(ts, INTERVAL '0.003' SECOND); 
 -- the TUMBLE_START    column will be close to ts,                           the type of TUMBLE_START    column is TIMESTAMP
 -- the TUMBLE_END      column will be close to ts + INTERVAL '0.003' SECOND, the type of TUMBLE_END      column is TIMESTAMP      
 -- the TUMBLE_ROWTIME  column will be close to ts + INTERVAL '0.002' SECOND, the type of TUMBLE_ROWTIME  column is TIMESTAMP                 
```

If the data source contains long epoch time, it's recommended to defining event time attribute on TIMESTAMP_LTZ.  
```sql
Flink SQL> SET table.local-time-zone=Asia/Shanghai; 
Flink SQL> CREATE TABLE src (
             ts BIGINT, -- The epoch time in milliseconds, BIGINT DataType
             a INT,
             b DOUBLE,
             ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- to TIMESTAMP_LTZ DataType
             WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '0.001' SECOND
           ) WITH (
             'connector' = 'kafka',
                         ...);
Flink SQL>  SELECT
                TUMBLE_START(ts_ltz, INTERVAL '0.003' SECOND), 
                TUMBLE_END(ts_ltz, INTERVAL '0.003' SECOND),
                TUMBLE_ROWTIME(ts_ltz, INTERVAL '0.003' SECOND),
                COUNT(a),
                SUM(b)            FROM src
            GROUP BY TUMBLE(ts_ltz, INTERVAL '0.003' SECOND);
 -- the TUMBLE_START    column will be close to ts_ltz (represents timestamp value in Asia/Shanghai), the type of TUMBLE_START    column is TIMESTAMP
 -- the TUMBLE_END      column will be close to ts_ltz + INTERVAL '0.003' SECOND,             the type of TUMBLE_END      column is TIMESTAMP      
 -- the TUMBLE_ROWTIME  column will be close to ts_ltz + INTERVAL '0.002' SECOND,             the type of TUMBLE_ROWTIME  column is TIMESTAMP_LTZ   
            
```

## Daylight Saving Time Support
Flink SQL supports defining time attributes on TIMESTAMP_LTZ column, base on this, Flink SQL gently uses TIMESTAMP and TIMESTAMP_LTZ type in window processing to support the Daylight Saving Time.
   
  
Flink use timestamp literal to split the window and assigns window to data according to the epoch time of the each row. It means Flink uses TIMESTAMP type for window start and window end(e.g. TUMBLE_START and TUMBLE_END), uses TIMESTAMP_LTZ for window time attribute(e.g. TUMBLE_PROCTIME, TUMBLE_ROWTIME).

Giving a example of tumble window, The DaylightTime in Los_Angele start at time 2021-03-14 02:00:00:
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

Flink evaluates their values according to execution mode, i.e. Flink evaluates time function value for row level in Streaming mode, evaluates the time function value at query start for batch mode.

{{< top >}}
