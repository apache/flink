---
title: "JOB Statements"
weight: 16
type: docs
aliases:
- /dev/table/sql/job.html
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

# JOB Statements

Job statements are used for management of Flink jobs.

Flink SQL supports the following JOB statements for now:
- SHOW JOBS
- STOP JOB

## Run a JOB statement

{{< tabs "show jobs statement" >}}
{{< tab "SQL CLI" >}}

The following examples show how to run `JOB` statements in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

{{< /tab >}}
{{< /tabs >}}

{{< tabs "show jobs" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> SHOW JOBS;
+----------------------------------+----------+---------+-------------------------+
|                           job id | job name |  status |              start time |
+----------------------------------+----------+---------+-------------------------+
| 228d70913eab60dda85c5e7f78b5782c |    myjob | RUNNING | 2023-02-11T05:03:51.523 |
+----------------------------------+----------+---------+-------------------------+

Flink SQL> SET 'state.savepoints.dir'='file:/tmp/';
[INFO] Execute statement succeed.

Flink SQL> STOP JOB '228d70913eab60dda85c5e7f78b5782c' WITH SAVEPOINT;
+-----------------------------------------+
|                          savepoint path |
+-----------------------------------------+
| file:/tmp/savepoint-3addd4-0b224d9311e6 |
+-----------------------------------------+
```
{{< /tab >}}
{{< /tabs >}}

## SHOW JOBS

```sql
SHOW JOBS
```

Show the jobs in the Flink cluster.

<span class="label label-danger">Attention</span> SHOW JOBS statements only work in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) or [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}).

## STOP JOB

```sql
STOP JOB '<job_id>' [WITH SAVEPOINT] [WITH DRAIN]
```

Stop the specified job. 

**WITH SAVEPOINT**
Perform a savepoint right before stopping the job. The savepoint path could be specified with
[state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir) either in
the cluster configuration or via `SET` statements (the latter would take precedence).

**WITH DRAIN**
Increase the watermark to the maximum value before the last checkpoint barrier. Use it when you
want to terminate the job permanently.

<span class="label label-danger">Attention</span> STOP JOB statements only work in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) or [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}).

{{< top >}}
