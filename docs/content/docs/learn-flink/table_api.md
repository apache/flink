---
title: Learn the Table API
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

# Learn the Table API

The focus of this training is to broadly cover the Table API well enough that you will be able
to get started writing streaming analytics applications.

The Table API is Flink's declarative, relational API. You describe *what* you want to compute,
and Flink figures out *how* to compute it efficiently. The same queries work on both batch and
streaming data without modification, and Flink's optimizer automatically selects efficient
execution plans.

## What Can Be Represented as a Table?

The Table API works with structured data that has a defined schema. Every table has named columns
with specific data types.

Flink supports a rich set of data types for table columns:

- Primitive types: `STRING`, `INT`, `BIGINT`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`
- Complex types: `ARRAY`, `MAP`, `ROW` (for nested structures)
- Structured types: Java POJOs with named fields
- Special types: `RAW` (for opaque byte data in UDFs), `INTERVAL`, `NULL`

Tables can be created from [external systems]({{< ref "docs/connectors/table/overview" >}})
(Kafka, files, databases), from DataStreams, or inline using values.

{{< top >}}

## A Complete Example

This example takes a table of records about people as input and filters it to only include adults.

```java
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class Example {

    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.inStreamingMode());

        Table people = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT())
                ),
                Row.of("Alice", 35),
                Row.of("Bob", 35),
                Row.of("Charlie", 2)
        );

        Table adults = people
                .filter($("age").isGreaterOrEqual(18))
                .select($("name"), $("age"));

        adults.execute().print();
    }
}
```

The output will look something like this:

```
+----+--------------------------------+-------------+
| op |                           name |         age |
+----+--------------------------------+-------------+
| +I |                          Alice |          35 |
| +I |                            Bob |          35 |
+----+--------------------------------+-------------+
```

The `op` column shows the operation type (`+I` means INSERT). The `+I` flag indicates that
these rows are being inserted into the result table. The section on changelogs below explains
these flags in more detail.

{{< top >}}

## Table Environment

Every Table API program needs a `TableEnvironment`. This is the central entry point for:

- Creating and registering tables
- Executing queries
- Registering user-defined functions
- Converting between Table API and DataStream API

```java
// For streaming applications
TableEnvironment tableEnv = TableEnvironment.create(
        EnvironmentSettings.inStreamingMode());

// For batch applications
TableEnvironment tableEnv = TableEnvironment.create(
        EnvironmentSettings.inBatchMode());
```

When you call `execute()` on a table, Flink compiles your table program into an optimized job
graph and submits it for execution.

{{< top >}}

## Basic Operations

### Select and Projection

Use `select()` to choose which columns to include and to create new computed columns.
The `$()` function references columns by name.

```java
Table result = orders
        .select($("product"), $("amount"), $("price").times($("amount")).as("total"));
```

### Filtering

Use `filter()` or `where()` (they are equivalent) to keep only rows matching a condition.

```java
Table filtered = orders
        .filter($("amount").isGreater(0))
        .filter($("status").isNotEqual("cancelled"));
```

### Adding and Modifying Columns

Use `addColumns()` to add new computed columns, `renameColumns()` to rename existing ones,
and `dropColumns()` to remove columns.

```java
Table result = orders
        .addColumns($("price").times($("amount")).as("total"))
        .renameColumns($("product").as("item"))
        .dropColumns($("internal_id"));
```

### Built-in Functions

The Table API includes many built-in functions for common operations:

```java
import static org.apache.flink.table.api.Expressions.*;

Table result = products
        .select(
            $("name").upperCase(),                    // String functions
            $("price").round(2),                      // Math functions
            $("created_at").extract(TimeIntervalUnit.HOUR)  // Temporal functions
        );
```

For a complete list of built-in functions, see the [Built-in Functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}) reference.

{{< top >}}

## Aggregation and Grouping

Use `groupBy()` with aggregate functions to compute summary statistics.

```java
Table counts = orders
        .groupBy($("product"))
        .select($("product"), $("amount").sum().as("total_sold"), $("product").count().as("order_count"));
```

Common aggregate functions include `sum()`, `count()`, `avg()`, `min()`, `max()`.

In streaming mode, aggregations produce updating results. Each time a new row arrives for
a group, Flink updates the aggregate and emits a new result for that group.

{{< top >}}

## Understanding Changelogs

One of the most important concepts to understand when using the Table API for streaming is
*stream-table duality*: a stream can be viewed as a table (with rows being inserted over time),
and a table can be viewed as a changelog stream (each change to the table is an event).

### Append-Only Tables

Simple operations like `filter()` and `select()` produce *append-only* tables. New rows are
inserted, but existing rows are never modified or deleted. These produce only `+I` (INSERT) flags.

### Updating Tables

Aggregations and other stateful operations produce *updating* tables. When the aggregated result
for a group changes, Flink must retract the old value and insert the new one.

The changelog flags you may see are:

| Flag | Meaning |
|------|---------|
| `+I` | INSERT: A new row is added |
| `-D` | DELETE: An existing row is removed |
| `-U` | UPDATE_BEFORE: The old value before an update (retraction) |
| `+U` | UPDATE_AFTER: The new value after an update |

For example, if you count orders by product and three orders for "widget" arrive:

```
+I [widget, 1]    -- First order: count is 1
-U [widget, 1]    -- Retract previous count
+U [widget, 2]    -- Second order: count is now 2
-U [widget, 2]    -- Retract previous count
+U [widget, 3]    -- Third order: count is now 3
```

This matters when connecting to sinks. Append-only sinks (like files) can only accept `+I`
operations. Updating sinks (like databases or Kafka upsert topics) can handle all operations.

{{< top >}}

## Windowed Aggregation

Unbounded aggregations keep state forever, which is not practical for many streaming use cases.
Windows let you aggregate over bounded portions of the stream.

Here's an example using a tumbling window to count orders per hour:

```java
import static org.apache.flink.table.api.Expressions.*;

Table hourlyStats = orders
        .window(Tumble.over(lit(1).hours()).on($("order_time")).as("w"))
        .groupBy($("product"), $("w"))
        .select(
            $("product"),
            $("w").start().as("window_start"),
            $("w").end().as("window_end"),
            $("amount").sum().as("total_sold")
        );
```

Windowed aggregations produce append-only results because each window produces a final result
once and never updates it. This makes them ideal for sinks that don't support updates.

For more on windows, see the [Group Aggregation]({{< ref "docs/dev/table/tableApi" >}}#group-windows)
documentation.

{{< top >}}

## User-Defined Functions

When built-in functions don't meet your needs, you can create user-defined functions (UDFs).

### Scalar Functions

A `ScalarFunction` takes one row and produces one value.

```java
import org.apache.flink.table.functions.ScalarFunction;

public class HashFunction extends ScalarFunction {
    public String eval(String input) {
        return Integer.toHexString(input.hashCode());
    }
}

// Register and use
tableEnv.createTemporaryFunction("hash", HashFunction.class);
Table result = orders.select($("id"), call("hash", $("customer_name")).as("hashed_name"));
```

### Table Functions

A `TableFunction` takes one row and produces zero or more rows. Use `joinLateral()` to apply it.

```java
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : str.split(",")) {
            collect(Row.of(s.trim()));
        }
    }
}

// Use with joinLateral
tableEnv.createTemporaryFunction("split", SplitFunction.class);
Table result = orders.joinLateral(call("split", $("tags")).as("tag"));
```

For more details on UDFs, see the [User-defined Functions]({{< ref "docs/dev/table/functions/udfs" >}}) documentation.

{{< top >}}

## Process Table Functions

When declarative operations aren't enough, Process Table Functions (PTFs) provide full control
over processing. PTFs are the Table API's "escape hatch" for complex logic, similar to how
`ProcessFunction` works in the DataStream API.

PTFs can:

- Access and manage **state** across multiple rows
- Register **timers** to trigger processing at specific times
- Handle **partitioned** data with custom per-partition logic

Here's a simple stateful PTF that counts occurrences per key:

```java
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

public class CountingFunction extends ProcessTableFunction<String> {

    public static class CountState {
        public long count = 0L;
    }

    public void eval(
            @StateHint CountState state,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        state.count++;
        collect("Seen " + state.count + " rows for this partition");
    }
}

// Use with partitionBy for per-key state
Table result = events
        .partitionBy($("user_id"))
        .process(CountingFunction.class);
```

PTFs bridge the gap between the declarative Table API and low-level stream processing. Use them
when you need fine-grained control over state, timers, or event-by-event processing logic that
cannot be expressed with standard operations.

For the complete PTF guide, see [Process Table Functions]({{< ref "docs/dev/table/functions/ptfs" >}}).

{{< top >}}

## Hands-on

At this point you know enough to get started coding and running a simple Table API application.
Clone the {{< training_repo >}}, and after following the
instructions in the README, try the Table API exercises.

{{< top >}}

## Further Reading

- [Table API Operations]({{< ref "docs/dev/table/tableApi" >}}) - Complete reference for all operations
- [Built-in Functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}) - All available functions
- [User-Defined Functions]({{< ref "docs/dev/table/functions/udfs" >}}) - Creating custom functions
- [Process Table Functions]({{< ref "docs/dev/table/functions/ptfs" >}}) - Advanced stateful processing
- [SQL & Table API Connectors]({{< ref "docs/connectors/table/overview" >}}) - Reading from and writing to external systems
- [Common Concepts]({{< ref "docs/dev/table/common" >}}) - Shared concepts between Table API and SQL

{{< top >}}
