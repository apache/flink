---
title: "JOB 语句"
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

Job 语句用于管理作业的生命周期。

目前 Flink SQL 支持以下 JOB 语句：
- SHOW JOBS
- STOP JOB

## 执行 JOB 语句

{{< tabs "show jobs statement" >}}
{{< tab "SQL CLI" >}}

以下示例展示如何在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 JOB 语句.

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

展示 Flink 集群上的作业。

<span class="label label-danger">Attention</span> SHOW JOBS 语句仅适用于 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 或者 [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}).

## STOP JOB

```sql
STOP JOB '<job_id>' [WITH SAVEPOINT] [WITH DRAIN]
```

停止指定作业。

**WITH SAVEPOINT**
在作业停止之前执行 Savepoin。 Savepoint 的路径可以通过集群配置的
[state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir) 指定，
或者通过 `SET` 语句指定（后者有更高优先级）。

**WITH DRAIN**
在触发 savepoint 之前将 Watermark 提升至最大。该操作会可能会触发窗口的计算。请您注意该操作可能导致您之后从该创建的 savepoint 恢复的作业结果不正确。

<span class="label label-danger">Attention</span> STOP JOB 语句仅适用于 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 或者 [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}).

{{< top >}}
