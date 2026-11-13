---
title: Table API
weight: 5
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

`Table#materializeInto` declares the query of a `Table` as the definition of a materialized table,
and `execute()` submits the declaration with the semantics of
[`CREATE OR ALTER MATERIALIZED TABLE ... AS <query>`]({{< ref "docs/sql/materialized-table/statements" >}}#or-alter).

# Setup

Continuous refresh jobs need a session cluster as the execution target of the `TableEnvironment`,
and redeploying a running materialized table needs a savepoint directory. See
[Using Table API]({{< ref "docs/sql/materialized-table/deployment" >}}#using-table-api) for the
supported targets. The catalog must supply the storage of the materialized table.

# Creating a Materialized Table

```java
MaterializedTable revenue = tEnv.from("orders")
        .groupBy($("region"))
        .select($("region"), $("amount").sum().as("revenue"))
        .materializeInto("revenue", Duration.ofMinutes(1))
        .execute();
```

The schema is derived from the stored SQL text of the query, and the path is resolved against the
current catalog and database. The freshness is optional and defaults to
[materialized-table.default-freshness.continuous]({{< ref "docs/dev/table/config" >}}#materialized-table-default-freshness-continuous).
The refresh mode is derived from the freshness, unless a [`MaterializedTableDescriptor`](#declaring-a-materialized-table) declares it (see [Refresh Mode]({{< ref "docs/sql/materialized-table/overview" >}}#refresh-mode)).
Full refresh mode needs a workflow scheduler and cannot be used from a plain `TableEnvironment`.

By using a `MaterializedTableDescriptor` the clauses of the materialized table that the query
doesn't define can be declared.

```java
MaterializedTable ordersCurated = tEnv.from("orders")
        .select($("order_id"), $("region"), $("order_time"), $("amount"))
        .materializeInto(
                "orders_curated",
                MaterializedTableDescriptor.newBuilder()
                        .schema(Schema.newBuilder()
                                .primaryKey("order_id")
                                .watermark("order_time", $("order_time").minus(lit(5).seconds()))
                                .build())
                        .distributedByHash(4, "order_id")
                        .partitionedBy("region")
                        .option("format", "json")
                        .freshness(Duration.ofSeconds(30))
                        .continuousRefresh()
                        .build())
        .execute();
```

A declared schema is merged onto the schema of the query, and every declared physical or persisted
metadata column must be produced by the query. The query must produce the primary key columns as
`NOT NULL`. See
[CREATE MATERIALIZED TABLE]({{< ref "docs/sql/materialized-table/statements" >}}#create-or-alter-materialized-table)
for the semantics of the clauses.

# Redeploying

`execute()` on an existing materialized table alters it, and a changed query follows the
schema-evolution rules of
[ALTER MATERIALIZED TABLE AS]({{< ref "docs/sql/materialized-table/statements" >}}#as-select_statement-1).
A running continuous refresh job is stopped with a savepoint and restarted without restoring it,
even if the declaration is unchanged, so streaming state is reset.

An existing materialized table keeps its freshness, comment and partition keys, so declared ones are
ignored. Its options and distribution are replaced by the declared ones. This means options that the
descriptor doesn't declare are removed, and the distribution is dropped if the descriptor declares
none. To keep them, restate them in the descriptor on every redeploy.

If the declared schema starts with a physical column, it replaces the watermark, computed columns,
virtual metadata columns and primary key of the materialized table. Otherwise, the materialized
table keeps its watermark, and computed and virtual metadata columns that the schema no longer
declares, and keeps its primary key unless the schema declares one. For example, the schema of
`orders_curated` above declares no columns, so redeploying it with a different watermark keeps the
old one.

If the changes are rejected, for example because the new query drops a column, `execute()` fails
and leaves the materialized table suspended with its previous definition. Resume it with
`ALTER MATERIALIZED TABLE ... RESUME` before you redeploy, because redeploying a suspended
materialized table clears its savepoint.

If the new refresh job starts but the catalog cannot record it, the job is cancelled and the old
job restarts from its savepoint. If that cancel fails, the materialized table is rolled back and
left suspended instead of restarting the old job.

# Limitations

- The query of a refresh job is stored as SQL text, so it must be expressible in SQL. Inline
  functions (unless a custom `SqlFactory` serializes them), tables created from a `TableDescriptor`
  or a `DataStream`, and operations without a SQL form, such as session group windows, fail at
  `materializeInto`.
- As in SQL, referring to temporary tables, views, functions or models is not supported. They are
  not rejected, but a refresh job started from another session can't resolve them.
