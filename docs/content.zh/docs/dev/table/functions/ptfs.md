---
title: "Process Table Functions"
weight: 52
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

# Process Table Functions (PTFs)

Process Table Functions (PTFs) are the most powerful function kind for Flink SQL and Table API. They enable implementing
user-defined operators that can be as feature-rich as built-in operations. PTFs can take (partitioned) tables to produce
a new table. They have access to Flink's managed state, event-time and timer services, and underlying table changelogs.

Conceptually, a PTF is itself a [user-defined function]({{< ref "docs/dev/table/functions/udfs" >}}) that is a superset of all
other user-defined functions. It maps zero, one, or multiple tables to zero, one, or multiple rows (or structured types).
Scalar arguments are supported. Due to its stateful nature, implementing aggregating behavior is possible as well.

A PTF enables the following tasks:
- Apply transformations on each row of a table.
- Logically partition the table into distinct sets and apply transformations per set.
- Store seen events for repeated access.
- Continue the processing at a later point in time enabling waiting, synchronization, or timeouts.
- Buffer and aggregate events using complex state machines or rule-based conditional logic.

{{< top >}}

Polymorphic Table Functions
---------------------------

The PTF query syntax and semantics are derived from SQL:2016's *Polymorphic Table Functions*.
Detailed information on the expected behavior and integration of polymorphic table functions within the SQL language can
be found in [ISO/IEC 19075-7:2021 (Part 7)](https://www.iso.org/standard/78938.html). A publicly available
summary is provided in [Section 3](https://sigmodrecord.org/publications/sigmodRecord/1806/pdfs/08_Industry_Michels.pdf)
of the related SIGMOD paper.

While both share the same abbreviation (*PTF*), process table functions in Flink enhance polymorphic table functions by
incorporating Flink-specific features, such as state management, time, and timer services. Call characteristics, including
table arguments with row or set semantics, descriptor arguments, and processing concepts related to virtual processors,
are aligned with the SQL standard.

{{< top >}}

Motivating Examples
-------------------

The following examples demonstrate how a PTF can accept and transform tables. The `@ArgumentHint` specifies that the function
accepts a table as an argument, rather than just a scalar row value. In both examples, the `eval()` method is invoked for
each row in the input table. Additionally, the `@ArgumentHint` not only indicates that the function can process a table,
but also defines how the function interprets the table - whether using row or set semantics.

### Greeting

An example that adds a greeting to each incoming customer.

{{< tabs "0137eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.*;

// A PTF that takes a table argument, conceptually viewing the table as a row.
// The result is never stateful and derived purely based on the current row.
public static class Greeting extends ProcessTableFunction<String> {
    public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) Row input) {
        collect("Hello " + input.getFieldAs("name") + "!");
    }
}

TableEnvironment env = TableEnvironment.create(...);

// Call the PTF with row semantics "inline" (without registration) in Table API
env.fromValues("Bob", "Alice", "Bob")
  .as("name")
  .process(Greeting.class)
  .execute()
  .print();

// For SQL, register the PTF upfront
env.executeSql("CREATE VIEW Names(name) AS VALUES ('Bob'), ('Alice'), ('Bob')");
env.createFunction("Greeting", Greeting.class);

// Call the PTF with row semantics in SQL
env.executeSql("SELECT * FROM Greeting(TABLE Names)").print();
```
{{< /tab >}}
{{< /tabs >}}

The results of both Table API and SQL look similar to:

```text
+----+--------------------------------+
| op |                         EXPR$0 |
+----+--------------------------------+
| +I |                     Hello Bob! |
| +I |                   Hello Alice! |
| +I |                     Hello Bob! |
+----+--------------------------------+
```

### Greeting with Memory

An example that adds a greeting for each incoming customer, taking into account whether the customer has been greeted
previously.

{{< tabs "0237eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.*;

// A PTF that takes a table argument, conceptually viewing the table as a set.
// The result can be stateful and derived based on the current row and/or
// previous rows in the set.
// The call's partitioning defines the size of the set.
public static class GreetingWithMemory extends ProcessTableFunction<String> {
  public static class CountState {
    public long counter = 0L;
  }

  public void eval(@StateHint CountState state, @ArgumentHint(ArgumentTrait.TABLE_AS_SET) Row input) {
    state.counter++;
    collect("Hello " + input.getFieldAs("name") + ", your " + state.counter + " time?");
  }
}

TableEnvironment env = TableEnvironment.create(...);

// Call the PTF with set semantics "inline" (without registration) in Table API
env.fromValues("Bob", "Alice", "Bob")
  .as("name")
  .partitionBy($("name"))
  .process(GreetingWithMemory.class)
  .execute()
  .print();

// For SQL, register the PTF upfront
env.executeSql("CREATE VIEW Names(name) AS VALUES ('Bob'), ('Alice'), ('Bob')");
env.createFunction("GreetingWithMemory", GreetingWithMemory.class);

// Call the PTF with set semantics in SQL
env.executeSql("SELECT * FROM GreetingWithMemory(TABLE Names PARTITION BY name)").print();
```
{{< /tab >}}
{{< /tabs >}}

The results of both Table API and SQL look similar to:

```text
+----+--------------------------------+--------------------------------+
| op |                           name |                         EXPR$0 |
+----+--------------------------------+--------------------------------+
| +I |                            Bob |        Hello Bob, your 1 time? |
| +I |                          Alice |      Hello Alice, your 1 time? |
| +I |                            Bob |        Hello Bob, your 2 time? |
+----+--------------------------------+--------------------------------+
```

### Greeting with Follow Up

An example that adds a greeting for each incoming customer and sends out a follow-up notification after some time. The
example illustrates the use of time and unnamed timers.

{{< tabs "0337eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.*;

// A stateful PTF with event-time timers.
// Every incoming row not only greets the customer but also registers a timer to follow
// up on the customer after 1 minute.
public static class GreetingWithFollowUp extends ProcessTableFunction<String> {
  public static class StayState {
    public String name;
    public long counter = 0L;
  }

  public void eval(
    Context ctx,
    @StateHint StayState state,
    @ArgumentHint(ArgumentTrait.TABLE_AS_SET) Row input
  ) {
    state.name = input.getFieldAs("name");
    state.counter++;
    collect("Hello " + state.name + ", your " + state.counter + " time?");

    TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
    timeCtx.registerOnTime(timeCtx.time().plus(Duration.ofMinutes(1)));
  }

  public void onTimer(StayState state) {
    collect("Hello " + state.name + ", I hope you enjoyed your stay! "
      + "Please let us know if there’s anything we could have done better.");
    }
}

TableEnvironment env = TableEnvironment.create(...);

// Create a watermarked table with events from Bob and Alice
env.createTable(
  "Names",
  TableDescriptor.forConnector("datagen")
    .schema(
      Schema.newBuilder()
        .column("ts", "TIMESTAMP_LTZ(3)")
        .column("random", "INT")
        .columnByExpression("name", "IF(random % 2 = 0, 'Bob', 'Alice')")
        .watermark("ts", "ts - INTERVAL '1' SECOND")
        .build())
    .option("rows-per-second", "1")
    .build());

// Call the PTF in Table API and pass the watermarked time column
env.from("Names")
  .partitionBy($("name"))
  .process(GreetingWithCatchingUp.class, descriptor("ts").asArgument("on_time"))
  .execute()
  .print();

// For SQL, register the PTF upfront
env.createFunction("GreetingWithFollowUp", GreetingWithFollowUp.class);

// Call the PTF in SQL and pass the watermarked time column
env.executeSql("SELECT * FROM GreetingWithFollowUp(input => TABLE Names PARTITION BY name, on_time => DESCRIPTOR(ts))").print();
```
{{< /tab >}}
{{< /tabs >}}

The results of both Table API and SQL look similar to:

```text
+----+--------------------------------+--------------------------------+-------------------------+
| op |                           name |                         EXPR$0 |                 rowtime |
+----+--------------------------------+--------------------------------+-------------------------+
| +I |                            Bob |        Hello Bob, your 1 time? | 2025-03-27 07:38:31.134 |
| +I |                            Bob |        Hello Bob, your 2 time? | 2025-03-27 07:38:31.134 |
| +I |                          Alice |      Hello Alice, your 1 time? | 2025-03-27 07:38:31.134 |
| +I |                          Alice |      Hello Alice, your 2 time? | 2025-03-27 07:38:31.134 |
...
| +I |                            Bob | Hello Bob, I hope you enjoy... | 2025-03-27 07:39:31.134 |
| +I |                          Alice | Hello Alice, I hope you enj... | 2025-03-27 07:39:31.134 |
| +I |                            Bob | Hello Bob, I hope you enjoy... | 2025-03-27 07:39:31.135 |
```

{{< top >}}

Table Semantics and Virtual Processors
--------------------------------------

PTFs can produce a new table by consuming tables as arguments. For scalability, input tables are distributed across
so-called "virtual processors". A virtual processor, as defined by the SQL standard, executes a PTF instance and has
access only to a portion of the entire table. The argument declaration decides about the size of the portion and
co-location of data. Conceptually, tables can be processed either "as row" (i.e. with row semantics) or "as set"
(i.e. with set semantics).

{{<img alt="PTF Table Semantics" src="/fig/table-streaming/ptf_table_semantics.png" width="80%">}}

*Note*: Accessing, in this context, means that the individual rows are streamed through the virtual processor. It is
the responsibility of the PTF to store past events for repeated access using state.

### Table Argument with Row Semantics

A PTF that takes a table with row semantics assumes that there is no correlation between rows and each row can be
processed independently. The framework is free in how to distribute rows across virtual processors and each virtual
processor has access only to the currently processed row.

### Table Argument with Set Semantics

A PTF that takes a table with set semantics assumes that there is a correlation between rows. When calling the function,
the PARTITION BY clause defines the columns for correlation. The framework ensures that all rows belonging to same set
are co-located. A PTF instance is able to access all rows belonging to the same set. In other words: The virtual processor
is scoped by a key context.

It is also possible not to provide a key (if the argument is declared with `ArgumentTrait.OPTIONAL_PARTITION_BY`), in
which case only one virtual processor handles the entire table, thereby losing scalability benefits.

Call Syntax
-----------

When invoking a PTF, the system automatically adds implicit arguments for state and time management alongside the
user-defined input arguments.

Given a PTF `TableFilter` that has been implemented as:

{{< tabs "0437eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<threshold INT, score BIGINT>")
public static class TableFilter extends ProcessTableFunction<Row> {
  public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) Row input, int threshold) {
    long score = input.getFieldAs("score");
    if (score > threshold) {
      collect(Row.of(threshold, score));
    }
  }
}
```
{{< /tab >}}
{{< /tabs >}}

The effective call signature is:
```text
TableFilter(input => {TABLE, TABLE AS ROW}, threshold => INT NOT NULL, on_time => DESCRIPTOR, uid => STRING)
```

Both `on_time` and `uid` are optional by default. The `on_time` is required if time semantics are needed. The `uid` must
be provided for stateful transformations.

Both SQL and Table API users can call the function using either position-based or name-based syntax. For better readability
and future function evolution, we recommend the name-based syntax. It offers better support for optional arguments and
eliminates the need to maintain a specific argument order.

SQL:
```text
-- Position-based
SELECT * FROM TableFilter(TABLE t, 100)
SELECT * FROM TableFilter(TABLE t, 100, DEFAULT, 'my-ptf')
SELECT * FROM TableFilter(TABLE t, 100, DEFAULT, DEFAULT)

-- Name-based
SELECT * FROM TableFilter(input => TABLE t, threshold => 100)
SELECT * FROM TableFilter(input => TABLE t, uid => 'my-ptf')
```

Table API:
```java
// Position-based
env.from("t").process(TableFilter.class, 100)
env.from("t").process(TableFilter.class, 100, null, "my-ptf")
env.from("t").process(TableFilter.class, 100, null, null)

// Name-based
env.from("t").process(TableFilter.class, lit(100).asArgument("threshold"))
env.from("t").process(TableFilter.class, lit(100).asArgument("threshold"), lit("my-ptf").asArgument("uid"))

// Fully name-based (including the table itself)
env.fromCall(
  TableFilter.class,
  env.from("t").asArgument("input"),
  lit(100).asArgument("threshold"),
  lit("my-ptf").asArgument("uid"))
```

### Function Chaining

Multiple PTFs can be invoked after each other.

In SQL, we recommend using common table expressions (i.e., WITH) to enhance code readability by applying the Divide and
Conquer strategy, breaking the problem down into smaller, more manageable parts:
```text
WITH
ptf1 AS (
  SELECT * FROM f1(input => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts))
),
ptf2 AS (
  SELECT * FROM f2(input => TABLE ptf1 PARTITION BY name, on_time => DESCRIPTOR(rowtime))
)
SELECT * FROM ptf2;
```

In the Table API, the framework enables a consecutive application of functions:
```java
env.from("t")
  .partitionBy($("name"))
  .process("f1", descriptor("ts").asArgument("on_time"))
  .partitionBy($("name"))
  .process("f2", descriptor("rowtime").asArgument("on_time"))
```

{{< top >}}

Implementation Guide
--------------------

{{< hint info >}}
PTFs follow similar implementation principles as other user-defined functions in Flink. Please see
[implementation guide for user-defined functions]({{< ref "docs/dev/table/functions/udfs" >}}#implementation-guide)
for more details. This page focuses on PTF specifics.
{{< /hint >}}

In order to define a process table function, one has to extend the base class `ProcessTableFunction` in `org.apache.flink.table.functions`
and implement an evaluation method named `eval(...)`. The `eval()` method declares the supported input arguments, and
state entries in case of stateful PTF.

The signature for `eval()` should follow the pattern:
```text
eval( <context>? , <state entry>* , <call argument>* )
```

The evaluation method must be declared publicly and should not be static. Overloading is not supported.

For storing a user-defined function in a catalog, the class must have a default constructor and must be instantiable during
runtime. Anonymous, inline functions in Table API can only be persisted if the function object is not stateful (i.e. containing
only transient and static fields).

### Data Types

By default, input and output data types are automatically extracted using reflection. This includes the generic argument
`T` of the class for determining an output data type. Input arguments are derived from the `eval()` method. If the reflective
information is not sufficient, it can be supported and enriched with `@FunctionHint`, `@ArgumentHint`, and
`@DataTypeHint` annotations.

In contrast to scalar functions, the evaluation method itself must not have a return type, instead, table functions provide
a `collect(T)` method that can be called within the evaluation method for emitting zero, one, or more records. A returned
record may consist of one or more fields. If an output record consists of only a single field, the structured record can
be omitted, and a scalar value can be emitted that will be implicitly wrapped into a row by the runtime.

The following examples show how to specify data types:

{{< tabs "0537eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that accepts two scalar INT arguments and emits them as an implicit ROW<INT>
class AdditionFunction extends ProcessTableFunction<Integer> {
  public void eval(Integer a, Integer b) {
    collect(a + b);
  }
}

// Function that produces an explicit ROW<i INT, s STRING> from scalar arguments,
// the function hint helps in declaring the row's fields
@DataTypeHint("ROW<i INT, s STRING>")
class DuplicatorFunction extends ProcessTableFunction<Row> {
  public void eval(Integer i, String s) {
    collect(Row.of(i, s));
    collect(Row.of(i, s));
  }
}

// Function that accepts a scalar DECIMAL(10, 4) and emits it as
// an explicit ROW<DECIMAL(10, 4)>
@FunctionHint(output = @DataTypeHint("ROW<d DECIMAL(10, 4)>"))
class DuplicatorFunction extends ProcessTableFunction<Row> {
  public void eval(@DataTypeHint("DECIMAL(10, 4)") BigDecimal d) {
    collect(Row.of(d));
    collect(Row.of(d));
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### Arguments

The `@ArgumentHint` annotation enables declaring the name, data type, and traits of each argument.

In most cases, the system can automatically infer the name and data type reflectively, so they do not need to be
specified. However, traits must be provided explicitly, particularly when defining the argument's kind. The argument
of a PTF can be set to `ArgumentTrait.SCALAR`, `ArgumentTrait.TABLE_AS_SET`, or `ArgumentTrait.TABLE_AS_ROW`. By default,
arguments are treated as scalar values.

The following examples show usages of the `@ArgumentHint` annotation:

{{< tabs "0637eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that has two arguments:
// "input_table" (a table with set semantics) and "threshold" (a scalar value)
class ThresholdFunction extends ProcessTableFunction<Integer> {
  public void eval(
    // For table arguments, a data type for Row is optional (leading to polymorphic behavior)
    @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET, name = "input_table") Row t,
    // Scalar arguments require a data type either explicit or via reflection
    @ArgumentHint(value = ArgumentTrait.SCALAR, name = "threshold") Integer threshold
  ) {
    int amount = t.getFieldAs("amount");
    if (amount >= threshold) {
      collect(amount);
    }
  }
}
```
{{< /tab >}}
{{< /tabs >}}

#### Table Arguments

The traits `ArgumentTrait.TABLE_AS_SET` and `ArgumentTrait.TABLE_AS_ROW` define table arguments.

Table arguments can declare a concrete data type (of either row or structured type) or accept any type of row in a
polymorphic fashion.

{{< hint warning >}}
The `Row` class might be declared by a table argument or scalar argument.

For scalar arguments, the data type must be fully specified and values have to be
provided, e.g. `f(my_scalar_arg => ROW(12)`.

For table arguments, a full data type is optional and expects a table instead of one row,
e.g. `f(my_table_arg => TABLE t)`.
{{< /hint >}}

{{< tabs "0737eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function with explicit table argument type of row
class MyPTF extends ProcessTableFunction<String> {
  public void eval(
    Context ctx,
    @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET, type = @DataTypeHint("ROW<s STRING>")) Row t
  ) {
    TableSemantics semantics = ctx.tableSemanticsFor("t");
    // Always returns "ROW < s STRING >"
    semantics.dataType();
    ...
  }
}

// Function with explicit table argument type of structured type "Customer"
class MyPTF extends ProcessTableFunction<String> {
  public void eval(
    Context ctx,
    @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET) Customer c
  ) {
    TableSemantics semantics = ctx.tableSemanticsFor("c");
    // Always returns structured type of "Customer"
    semantics.dataType();
    ...
  }
}

// Function with polymorphic table argument
class MyPTF extends ProcessTableFunction<String> {
  public void eval(
    Context ctx,
    @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET) Row t
  ) {
    TableSemantics semantics = ctx.tableSemanticsFor("t");
    // Always returns "ROW" but content depends on the table that is passed into the call
    semantics.dataType();
    ...
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### Context

A `Context` can be added as a first argument to the `eval()` method for additional
information about the input tables and other services provided by the framework.

{{< tabs "0837eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that accesses the Context for reading the PARTITION BY columns and
// excluding them when building a result string
class ConcatNonKeysFunction extends ProcessTableFunction<String> {
  public void eval(Context ctx, @ArgumentHint(ArgumentTrait.TABLE_AS_SET) Row inputTable) {
    TableSemantics semantics = ctx.tableSemanticsFor("inputTable");

    List<Integer> keys = Arrays.asList(semantics.partitionByColumns());

    return IntStream.range(0, inputTable.getArity())
      .filter(pos -> !keys.contains(pos))
      .mapToObj(inputTable::getField)
      .map(Object::toString)
      .collect(Collectors.joining(", "));
  }
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

State
-----

A PTF that takes set semantic tables can be stateful. Intermediate results can be buffered,
cached, aggregated, or simply stored for repeated access. A function can have one or more state
entries which are managed by the framework. Flink takes care of storing and restoring those
during failures or restarts (i.e. Flink managed state).

A state entry is partitioned by a key and cannot be accessed globally. The partitioning (or a
single partition in case of no partitioning) is defined by the corresponding function call. In
other words: Similar to how a virtual processor has access only to a portion of the entire table,
a PTF has access only to a portion of the entire state defined by the PARTITION BY clause. In
Flink, this concept is also known as keyed state.

State entries can be added as a mutable parameter to the `eval()` method. In order to
distinguish them from call arguments, they must be declared before any other argument, but after
an optional `Context` parameter. Furthermore, they must be annotated either via `@StateHint` or declared
as part of `@FunctionHint(state = ...)`.

For read and write access, only row or structured types (i.e. POJOs with default constructor)
qualify as a data type. If no state is present, all fields are set to null (in case of a row
type) or fields are set to their default value (in case of a structured type). For state
efficiency, it is recommended to keep all fields nullable.

{{< tabs "0937eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that counts and stores its intermediate result in the CountState object
// which will be persisted by Flink
class CountingFunction extends ProcessTableFunction<String> {
  public static class CountState {
    public long count = 0L;
  }

  public void eval(
    @StateHint CountState memory,
    @ArgumentHint(TABLE_AS_SET) Row input
  ) {
    memory.count++;
    collect("Seen rows: " + memory.count);
  }
}

// Function that waits for a second event coming in
class CountingFunction extends ProcessTableFunction<String> {
  public static class SeenState {
    public String first;
  }

  public void eval(
    @StateHint SeenState memory,
    @ArgumentHint(TABLE_AS_SET) Row input
  ) {
    if (memory.first == null) {
      memory.first = input.toString();
    } else {
      collect("Event 1: " + memory.first + " and Event 2: " + input.toString());
    }
  }
}

// Function that uses Row for state
class CountingFunction extends ProcessTableFunction<String> {
  public void eval(
    @StateHint(type = @DataTypeHint("ROW<count BIGINT>")) Row memory,
    @ArgumentHint(TABLE_AS_SET) Row input
  ) {
    Long newCount = 1L;
    if (memory.getField("count") != null) {
      newCount += memory.getFieldAs("count");
    }
    memory.setField("count", newCount);
    collect("Seen rows: " + newCount);
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### State TTL

A time-to-live (TTL) duration can be specified for each state entry, and Flink's state backend will automatically clean
up the entry once the TTL expires.

The `@StateHint(ttl = "...")` annotation specifies a minimum time interval for how long idle state (i.e., state which was not
updated by a create or write operation) will be retained. State will never be cleared until it was idle for less than the
minimum time, and will be cleared at some time after it was idle.

Use TTL for being able to efficiently manage an ever-growing state size or for complying with data protection requirements.

The cleanup is based on processing time, which effectively corresponds to the wall clock time as defined by `System.currentTimeMillis()`.

The provided string must use Flink's duration syntax (e.g., "3 days", "45 min", "3 hours", "60 s"). If no unit is specified,
the value is interpreted as milliseconds. The TTL setting on a state entry has higher precedence than the global state TTL
configuration `table.exec.state.ttl` for the entire pipeline.

By default, the TTL is set to `Long.MAX_VALUE` to allow for future adjustment of a reasonable value in the state layout.
If state size is a concern and TTL is unnecessary, it can be set to 0, effectively excluding the TTL from the state layout.

{{< tabs "1037eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function with 3 state entries each using a different TTL.
class CountingFunction extends ProcessTableFunction<String> {
  public void eval(
    Context ctx,
    @StateHint(ttl = "1 hour") SomeState shortTermState,
    @StateHint(ttl = "1 day") SomeState longTermState,
    @StateHint SomeState infiniteState, // potentially influenced by table.exec.state.ttl
    @ArgumentHint(TABLE_AS_SET) Row input
  ) {
    ...
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### Large State

Flink's state backends provide different types of state to efficiently handle large state.

Currently, PTFs support three types of state:

- **Value state**: Represents a single value.
- **List state**: Represents a list of values, supporting operations like appending, removing, and iterating.
- **Map state**: Represents a map (key-value pair) for efficient lookups, modifications, and removal of individual entries.

By default, state entries in a PTF are represented as value state. This means that every state entry is fully read from
the state backend when the evaluation method is called, and the value is written back to the state backend once the
evaluation method finishes.

To optimize state access and avoid unnecessary (de)serialization, state entries can be declared as:
- `org.apache.flink.table.api.dataview.ListView` (for list state)
- `org.apache.flink.table.api.dataview.MapView` (for map state)

These provide direct views to the underlying Flink state backend.

For example, when using a `MapView`, accessing a value via `MapView#get` will only deserialize the value associated with
the specified key. This allows for efficient access to individual entries without needing to load the entire map. This
approach is particularly useful when the map does not fit entirely into memory.

{{< hint info >}}
State TTL is applied individually to each entry in a list or map, allowing for fine-grained expiration control over state
elements.
{{< /hint >}}

The following example demonstrates how to declare and use a `MapView`. It assumes the PTF processes a table with the
schema `(userId, eventId, ...)`, partitioned by `userId`, with a high cardinality of distinct `eventId` values. For this
use case, it is generally recommended to partition the table by both `userId` and `eventId`. For example purposes, the
large state is stored as a map state.

{{< tabs "1837eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that uses a map view for storing a large map for an event history per user
class LargeHistoryFunction extends ProcessTableFunction<String> {
  public void eval(
    @StateHint MapView<String, Integer> largeMemory,
    @ArgumentHint(TABLE_AS_SET) Row input
  ) {
    String eventId = input.getFieldAs("eventId");
    Integer count = largeMemory.get(eventId);
    if (count == null) {
      largeMemory.put(eventId, 1);
    } else {
      if (count > 1000) {
        collect("Anomaly detected: " + eventId);
      }
      largeMemory.put(eventId, count + 1);
    }
  }
}
```
{{< /tab >}}
{{< /tabs >}}

Similar to other data types, reflection is used to extract the necessary type information. If reflection is not
feasible - such as when a `Row` object is involved - type hints can be provided. Use the `ARRAY` data type for list views
and the `MAP` data type for map views.

{{< tabs "1937eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that uses a list view of rows
class LargeHistoryFunction extends ProcessTableFunction<String> {
  public void eval(
    @StateHint(type = @DataTypeHint("ARRAY<ROW<s STRING, i INT>>")) ListView<Row> largeMemory,
    @ArgumentHint(TABLE_AS_SET) Row input
  ) {
    ...
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### Efficiency and Design Principles

A stateful function also means that data layout and data retention should be well thought
through. An ever-growing state can happen by an unlimited number of partitions (i.e. an open
keyspace) or even within a partition. Consider setting a `@StateHint(ttl = ... )` or
call `Context.clearAllState()` eventually.

{{< tabs "1137eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that waits for a second event coming in BUT with better state efficiency
class CountingFunction extends ProcessTableFunction<String> {
  public static class SeenState {
    public String first;
  }

  public void eval(
    Context ctx,
    @StateHint(ttl = "1 day") SeenState memory,
    @ArgumentHint(TABLE_AS_SET) Row input
  ) {
    if (memory.first == null) {
      memory.first = input.toString();
    } else {
      collect("Event 1: " + memory.first + " and Event 2: " + input.toString());
      ctx.clearAllState();
    }
  }
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

Time and Timers
---------------

A PTF natively supports event time. Time-based operations can be accessed via `Context#timeContext(Class)`.

The time context is always scoped to the currently processed event. The event could be either the current input row
or a firing timer.

Timestamps for time and timers can be represented as either `java.time.Instant`, `java.time.LocalDateTime`, or `Long`.
These timestamps are based on milliseconds since the epoch and do not account for the local session timezone. The time
class can be passed as an argument to `timeContext()`.

### Time

Every PTF takes an optional `on_time` argument. The `on_time` argument in the function call declares the time attribute
column for which a watermark has been declared. When processing a table's row, this timestamp can be accessed via
`TimeContext#time()` and the watermark via `TimeContext#currentWatermark()` respectively.

Specifying an `on_time` argument in the function call instructs the framework to return a `rowtime` column in the
function's output for subsequent time-based operations.

SQL syntax for declaring an `on_time` attribute:
```text
SELECT * FROM f(..., on_time => DESCRIPTOR(`my_timestamp`));
```

Table API for declaring an `on_time` attribute:
```java
.process(MyFunction.class, ..., descriptor("my_timestamp").asArgument("on_time"));
```

The `ArgumentTrait.REQUIRE_ON_TIME` makes the `on_time` argument mandatory if necessary.

Once an `on_time` argument is provided, timers can be used. The following motivating example illustrates how `eval()` and
`onTimer()` work together:

{{< tabs "1237eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that sends out a ping for the given key.
// The ping is sent one minute after the last event for this key was observed.
public static class PingLaterFunction extends ProcessTableFunction<String> {
  public void eval(
    Context ctx,
    @ArgumentHint({ArgumentTrait.TABLE_AS_SET, ArgumentTrait.REQUIRE_ON_TIME}) Row input
    ) {
      TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
      // Replaces an existing timer and thus potentially resets the minute if necessary
      timeCtx.registerOnTime("ping", timeCtx.time().plus(Duration.ofMinutes(1)));
    }

    public void onTimer(OnTimerContext onTimerCtx) {
        collect("ping");
    }
}

TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

// Create a table of watermarked events
env.executeSql(
  "CREATE TABLE Events (ts TIMESTAMP_LTZ(3), id STRING, val INT, WATERMARK FOR ts AS ts - INTERVAL '2' SECONDS) " +
    "WITH ('connector' = 'datagen')");

// Use Expressions.descriptor("ts") to mark the "on_time" argument in Table API
env.from("Events")
  .partitionBy($("id"))
  .process(PingLaterFunction.class, descriptor("ts").asArgument("on_time"))
  .execute()
  .print();

// For SQL register the function and pass the DESCRIPTOR argument
env.createFunction("PingLaterFunction", PingLaterFunction.class);
env.executeSql("SELECT * FROM PingLaterFunction(input => TABLE Events PARTITION BY id, on_time => DESCRIPTOR(`ts`))").print();
```
{{< /tab >}}
{{< /tabs >}}

The result will look similar to:

```text
| op |                             id |                         EXPR$0 |                 rowtime |
+----+--------------------------------+--------------------------------+-------------------------+
| +I | a1b9204c341c2d136c7d494fe29... |                           ping | 2025-03-26 16:56:47.842 |
| +I | 45a864eed208f4c0eebb40d55fd... |                           ping | 2025-03-26 16:56:47.842 |
| +I | 2039c6d08896a3255b9bfc38ad0... |                           ping | 2025-03-26 16:56:47.842 |
| +I | 41a9269ed057793a0ea97c31f61... |                           ping | 2025-03-26 16:56:47.842 |
```

With the `on_time` attribute declared, the output includes a `rowtime` column at the end. This column represents another
watermarked time attribute, which can be used for subsequent PTF calls or time-based operations. The data type of `rowtime`
is derived from the input's time attribute.

#### Current Timestamp

`TimeContext#time()` returns the timestamp of the currently processed event.

An event can be either the row of a table or a firing timer:

*1. Row event timestamp*

The timestamp of the row currently being processed within the `eval()` method.

Powered by the function call's `on_time` argument, this method will return the content of the referenced time attribute
column. Returns `null` if the `on_time` argument doesn't reference a time attribute column in the currently processed
table.

*2. Timer event timestamp*

The timestamp of the firing timer currently being processed within the `onTimer()` method.

#### Current Watermark

`TimeContext#currentWatermark()` returns the current event-time watermark.

Watermarks are generated in sources and sent through the topology for advancing the logical clock in each Flink subtask.
The current watermark of a Flink subtask is the global minimum watermark of all inputs (i.e. across all parallel inputs
and table partitions).

This method returns the current watermark of the Flink subtask that evaluates the PTF. Thus, the returned timestamp
represents the entire Flink subtask, independent of the currently processed partition. This behavior is similar to a call
to `SELECT CURRENT_WATERMARK(...)` in SQL.

If a watermark was not received from all inputs, the method returns `null`.

In case this method is called within the `onTimer()` method, the returned watermark is the triggering watermark
that currently fires the timer.

### Timers

A PTF that takes set semantic tables can support timers. Timers allow for continuing the processing at a later point in
time. This makes waiting, synchronization, or timeouts possible. A timer fires for the registered time when the watermark
progresses the logical clock.

Timers can be named (`TimeContext#registerOnTime(String, TimeType)`) or unnamed (`TimeContext#registerOnTime(TimeType)`).
The name of a timer can be useful for replacing or deleting an existing timer, or for identifying multiple timers via
`OnTimerContext#currentTimer()` when they fire.

An `onTimer()` method must be declared next to the `eval()` method for reacting to timer events. The signature of the
`onTimer()` method must contain an optional `OnTimerContext` followed by all state entries (as declared in the `eval()`
method).

The signature for `onTimer()` should follow the pattern:
```java
onTimer( <on timer context>? , <state entry>* )
```

Flink takes care of storing and restoring timers during failures or restarts. Thus, timers are a special kind of state.
Similarly, timers are scoped to a virtual processor defined by the `PARTITION BY` clause. A timer can only be registered
and deleted in the current virtual processor.

The following example illustrates how to register and clear timers:

{{< tabs "1337eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that waits for a second event or timeouts after 60 seconds
class TimerFunction extends ProcessTableFunction<String> {
  public static class SeenState {
    public String seen = null;
  }

  public void eval(
    Context ctx,
    @StateHint SeenState memory,
    @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row input
  ) {
    TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
      if (memory.seen == null) {
        memory.seen = input.getField(0).toString();
        timeCtx.registerOnTimer("timeout", timeCtx.time().plusSeconds(60));
      } else {
        collect("Second event arrived for: " + memory.seen);
        ctx.clearAll();
      }
  }

  public void onTimer(OnTimerContext onTimerCtx, SeenState memory) {
    collect("Timeout for: " + memory.seen);
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### Efficiency and Design Principles

Registering too many timers might affect performance. An ever-growing timer state can happen
by an unlimited number of partitions (i.e. an open keyspace) or even within a partition. Thus,
reduce the number of registered timers to a minimum and consider cleaning up timers if they are
not needed anymore via `Context#clearAllTimers()` or `TimeContext#clearTimer(String)`.

{{< top >}}

Multiple Tables
---------------

A PTF can process multiple tables simultaneously. This enables a variety of use cases, including:

- Implementing **custom joins** that efficiently manage state.
- Enriching the main table with information from dimension tables as **side inputs**.
- Sending **control events** to the keyed virtual processor during runtime.

The `eval()` method can specify multiple table arguments to support multiple inputs. All table arguments must be declared
with set semantics and use consistent partitioning. In other words, the number of columns and their data types in the
`PARTITION BY` clause must match across all involved table arguments.

Rows from either input are passed to the function one at a time. Thus, only one table argument is non-null at a time. Use
null checks to determine which input is currently being processed.

{{< hint warning >}}
The system decides which input row is streamed through the virtual processor next. If not handled properly in the PTF,
this can lead to race conditions between inputs and, consequently, to non-deterministic results. It is recommended to
design the function in such a way that the join is either time-based (i.e., waiting for all rows to arrive up to a given
watermark) or condition-based, where the PTF buffers one or more input rows until a specific condition is met.
{{< /hint >}}

### Example: Custom Join

The following example illustrates how to implement a custom join between two tables:

{{< tabs "2137eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

env.executeSql("CREATE VIEW Visits(name) AS VALUES ('Bob'), ('Alice'), ('Bob')");
env.executeSql("CREATE VIEW Purchases(customer, item) AS VALUES ('Alice', 'milk')");

env.createFunction("Greeting", GreetingWithLastPurchase.class);

env
  .executeSql("SELECT * FROM Greeting(TABLE Visits PARTITION BY name, TABLE Purchases PARTITION BY customer)")
  .print();

// --------------------
// Function declaration
// --------------------

// Function that greets a customer and suggests the last purchase made, if available.
public static class GreetingWithLastPurchase extends ProcessTableFunction<String> {

  // Keep the last purchased item in state
  public static class LastItemState {
    public String lastItem;
  }

  // The eval() method takes two @ArgumentHint(TABLE_AS_SET) arguments
  public void eval(
      @StateHint LastItemState state,
      @ArgumentHint(TABLE_AS_SET) Row visit,
      @ArgumentHint(TABLE_AS_SET) Row purchase) {

    // Process row from table Purchases
    if (purchase != null) {
      state.lastItem = purchase.getFieldAs("item");
    }

    // Process row from table Visits
    else if (visit != null) {
      if (state.lastItem == null) {
        collect("Hello " + visit.getFieldAs("name") + ", let me know if I can help!");
      } else {
        collect("Hello " + visit.getFieldAs("name") + ", here to buy " + state.lastItem + " again?");
      }
    }
  }
}
```
{{< /tab >}}
{{< /tabs >}}

The result will look similar to:

```text
+----+--------------------------------+--------------------------------+--------------------------------+
| op |                           name |                       customer |                         EXPR$0 |
+----+--------------------------------+--------------------------------+--------------------------------+
| +I |                            Bob |                            Bob | Hello Bob, let me know if I... |
| +I |                          Alice |                          Alice | Hello Alice, here to buy Pr... |
| +I |                            Bob |                            Bob | Hello Bob, let me know if I... |
+----+--------------------------------+--------------------------------+--------------------------------+
```

### Efficiency and Design Principles

A high number of input tables can negatively impact a single TaskManager or subtask. Network buffers must be allocated
for each input, resulting in increased memory consumption which is why the number of table arguments is limited to a
maximum of 20 tables.

Unevenly distributed keys may overload a single virtual processor, leading to backpressure. It is important to select
appropriate partition keys.

{{< top >}}

Query Evolution with UIDs
-------------------------

Unlike other SQL operators, PTFs support stateful query evolution.

From the planner's perspective, a PTF is a stateful building block that remains unoptimized, while the planner optimizes
the surrounding operators of the query. The state entries of a PTF can be persisted and restored by Flink, even if the
surrounding query or the PTF itself changes. As long as the schema of the state entries remains unchanged.

For future query evolution, the framework enforces a unique identifier (UID) for all PTFs that operate on tables with set
semantics. The UID can be provided through the implicit `uid` string argument. It is used when persisting the PTF's state
entries to checkpoints or savepoints. If the `uid` argument is not specified, the function name will be used by the framework,
ensuring one unique PTF invocation per statement. If a PTF is invoked multiple times, validation will require a manually
specified UID to ensure it is unique across the entire Flink job.

Additionally, the UID helps the optimizer determine whether to merge common parts of the pipeline. A shared UID enables
fan-out behavior while maintaining a single stateful PTF.

### Fan-out Example

In the following example, the optimizer detects the shared pipeline part `SELECT * FROM f(..., uid => 'same')`
in both `INSERT INTO` statements, allowing it to maintain a single stateful PTF operator. The result of the PTF is then
split and sent to two destinations based on a filter condition.

```text
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO bob_sink SELECT * FROM f(r => TABLE t PARTITION BY name, uid => 'same') WHERE name = 'Bob';

  INSERT INTO alice_sink SELECT * FROM f(r => TABLE t PARTITION BY name, uid => 'same') WHERE name = 'Alice';
END;
```

Different UIDs disable this optimization and two stateful blocks are maintained that consume a shared table `t`.

```text
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO bob_sink SELECT * FROM f(r => TABLE t PARTITION BY name, uid => 'ptf1') WHERE name = 'Bob';

  INSERT INTO alice_sink SELECT * FROM f(r => TABLE t PARTITION BY name, uid => 'ptf2') WHERE name = 'Alice';
END;
```

{{< top >}}

Pass-Through Columns
--------------------

Depending on the table semantics and whether an `on_time` argument has been defined, the system adds addition columns for
every function output.

For table arguments with set semantics, the output is prefixed with the `PARTITION BY` columns.

For invocations with `on_time` arguments, the output is suffixed with `rowtime`.

To summarize, the default pattern is as follows:

```text
<PARTITION BY keys> | <function output> | <rowtime>
```

The `ArgumentTrait.PASS_COLUMNS_THROUGH` instructs the system to include all columns of a table argument in the output of
the PTF.

Given a table `t` (containing columns `k` and `v`), and a PTF `f()` (producing columns `c1` and `c2`),
the output of a `SELECT * FROM f(table_arg => TABLE t PARTITION BY k)` uses the following order:

```text
Default: | k | c1 | c2 |
With pass-through columns: | k | v | c1 | c2 |
```

This allows the PTF to focus on the main aggregation without the need to manually forward input columns.

*Note*: Pass-through columns are only available for append-only PTFs taking a single table argument and don't use timers.

{{< top >}}

Updates and Changelogs
----------------------

By default, PTFs assume that table arguments are backed by append-only tables, where new records are inserted to the table
without any updates to existing records. PTFs then produce new append-only tables as output.

While append-only tables are ideal and work seamlessly with event-time and watermarks, there are scenarios that require
working with updating tables. In these cases, records can be updated or deleted after their initial insertion.
This impacts several aspects:

- **State Management**: Operations must accommodate the possibility that any record can be updated again, potentially requiring
  a larger state footprint.
- **Pipeline Complexity**: Since records are not final and can be changed subsequently, the entire pipeline result remains
  in-flight.
- **Downstream Systems**: In-flight data can lead to issues, not only in Flink but also in downstream systems where consistency
  and finality of data are critical.

{{< hint info >}}
For efficient and high-performance data processing, it is recommended to design pipelines using append-only tables whenever
feasible to simplify state management and avoid complexities associated with updating tables.
{{< /hint >}}

A PTF can consume and/or produce updating tables if it is configured to do so. This section provides a brief overview of
CDC (Change Data Capture) with PTFs.

### Change Data Capture Basics

Under the hood, tables in Flink's SQL engine are backed by changelogs. These changelogs encode CDC (Change Data Capture)
information containing *INSERT* (`+I`), *UPDATE_BEFORE* (`-U`), *UPDATE_AFTER* (`+U`), or *DELETE* (`-D`) messages.

The existence of these flags in the changelog constitutes the *Changelog Mode* of a consumer or producer:

**Append Mode `{+I}`**
- All messages are insert-only.
- Every insertion message is an immutable fact.
- Messages can be distributed in an arbitrary fashion across partitions and processors because they are unrelated.

**Upsert Mode `{+I, +U, -D}`**
- Messages can contain updates leading to an updating table.
- Updates are related using a key (i.e. the *upsert key*).
- Every message is either an upsert or delete message for a result under the upsert key.
- Messages for the same upsert key should land at the same partition and processor.
- Deletions can contain only values for upsert key columns (i.e. *partial deletes*) or values for
  all columns (i.e. *full deletes*).
- The mode is also known as *partial image* in the literature because `-U` messages are missing.

**Retract Mode `{+I, -U, +U, -D}`**
- Messages can contain updates leading to an updating table.
- Every insertion or update event is a fact that can be "undone" (i.e. retracted).
- Updates are related by all columns. In simplified words: The entire row is kind of the key but duplicates are supported.
  For example: `+I['Bob', 42]` is related to `-D['Bob', 42]` and `+U['Alice', 13]` is related to `-U['Alice', 13]`.
- Thus, every message is either an insertion (`+`) or its retraction (`-`).
- The mode is known as *full image* in the literature.

### Updating Input Tables

The `ArgumentTrait.SUPPORTS_UPDATES` instructs the system that updates are allowed as input to the given table argument.
By default, a table argument is insert-only and updates will be rejected.

Input tables become updating when sub queries such as aggregations or outer joins force an incremental computation. For
example, the following query only works if the function is able to digest retraction messages:

```text
// The change +I[1] followed by -U[1], +U[2], -U[2], +U[3] will enter the function
// if `table_arg` is declared with SUPPORTS_UPDATES
WITH UpdatingTable AS (
  SELECT COUNT(*) FROM (VALUES 1, 2, 3)
)
SELECT * FROM f(table_arg => TABLE UpdatingTable)
```

If updates should be supported, ensure that the data type of the table argument is chosen in a way that it can encode
changes. In other words: choose a `Row` type that exposes the `RowKind` change flag.

The changelog of the backing input table decides which kinds of changes enter the function. The function receives `{+I}`
when the input table is append-only. The function receives `{+I,+U,-D}` if the input table is upserting using the same
upsert key as the partition key. Otherwise, retractions `{+I,-U,+U,-D}` (i.e. including `RowKind.UPDATE_BEFORE`) enter
the function. Use `ArgumentTrait.REQUIRE_UPDATE_BEFORE` to enforce retractions for all updating cases.

For upserting tables, if the changelog contains key-only deletions (also known as partial deletions), only upsert key
fields are set when a row enters the function. Non-key fields are set to `null`, regardless of `NOT NULL` constraints.
Use `ArgumentTrait.REQUIRE_FULL_DELETE` to enforce that only full deletes enter the function.

The `SUPPORTS_UPDATES` trait is intended for advanced use cases. Please note that inputs are always insert-only in batch
mode. Thus, if the PTF should produce the same results in both batch and streaming mode, results should be emitted based
on watermarks and event-time.

#### Enforcing Retract Mode

The `ArgumentTrait.REQUIRE_UPDATE_BEFORE` instructs the system that a table argument which `SUPPORT_UPDATES` should include
a `RowKind.UPDATE_BEFORE` message when encoding updates. In other words: it enforces presenting the updating table in
retract changelog mode.

By default, updates are encoded as emitted by the input operation. Thus, the updating table might be encoded in upsert
changelog mode and deletes might only contain keys.

The following example shows how the input changelog encodes updates differently:

```text
// Given a table UpdatingTable(name STRING PRIMARY KEY, score INT)
// backed by upsert changelog with changes
// +I[Alice, 42], +I[Bob, 0], +U[Bob, 2], +U[Bob, 100], -D[Bob, NULL].

// Given a function `f` that declares `table_arg` with REQUIRE_UPDATE_BEFORE.
SELECT * FROM f(table_arg => TABLE UpdatingTable PARTITION BY name)

// The following changes will enter the function:
// +I[Alice, 42], +I[Bob, 0], -U[Bob, 0], +U[Bob, 2], -U[Bob, 2], +U[Bob, 100], -U[Bob, 100]

// In both encodings, a materialized table would only contain a row for Alice.
```

#### Enforcing Upserts with Full Deletes

The `ArgumentTrait.REQUIRE_FULL_DELETE` instructs the system that a table argument which `SUPPORT_UPDATES` should include
all fields in the `RowKind.DELETE` message if the updating table is backed by an upsert changelog.

For upserting tables, if the changelog contains key-only deletes (also known as partial deletes), only upsert key fields
are set when a row enters the function. Non-key fields are set to null, regardless of NOT NULL constraints.

The following example shows how the input changelog encodes updates differently:

```text
// Given a table UpdatingTable(name STRING PRIMARY KEY, score INT)
// backed by upsert changelog with changes
// +I[Alice, 42], +I[Bob, 0], +U[Bob, 2], +U[Bob, 100], -D[Bob, NULL].

// Given a function `f` that declares `table_arg` with REQUIRE_FULL_DELETE.
SELECT * FROM f(table_arg => TABLE UpdatingTable PARTITION BY name)

// The following changes will enter the function:
// +I[Alice, 42], +I[Bob, 0], +U[Bob, 2], +U[Bob, 100], -D[Bob, 100].

// In both encodings, a materialized table would only contain a row for Alice.
```

#### Example: Changelog Filtering

The following function demonstrates how a PTF can transform an updating table into an append-only table. Instead of
applying updates encoded in each `Row`, it incorporates the changelog flag into the payload. The rows emitted by the PTF
are guaranteed to be of `RowKind.INSERT`. By preserving the original changelog flag in the payload, it permits filtering
of specific update types. In this example, it filters out all deletions.

{{< tabs "1937eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

Table data = env
  .fromValues(
    Row.of("Bob", 23),
    Row.of("Alice", 42),
    Row.of("Alice", 2))
  .as("name", "score");

// Since the aggregation is not windowed and potentially unbounded,
// the result is an updating table. Usually, this means that all following
// operations and sinks need to support updates.
Table aggregated = data
  .groupBy($("name"))
  .select($("name"), $("score").sum().as("sum"));

// However, the PTF will convert the updating table into an insert-only result.
// Subsequent operations and sinks can easily digest the resulting table.
Table changelog = aggregated
  .partitionBy($("name"))
  .process(ToChangelogFunction.class);

// For event-driven applications, filtering on certain CDC events is possible.
Table upsertsOnly = changelog.filter($("flag").in("INSERT", "UPDATE_AFTER"));

upsertsOnly.execute().print();

// --------------------
// Function declaration
// --------------------

@DataTypeHint("ROW<flag STRING, sum INT>")
public static class ToChangelogFunction extends ProcessTableFunction<Row> {
  public void eval(@ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES}) Row input) {
    // Forwards the sum column and includes the row's kind as a string column.
    Row changelogRow =
      Row.of(
        input.getKind().toString(),
        input.getField("sum"));

    collect(changelogRow);
  }
}
```
{{< /tab >}}
{{< /tabs >}}

The PTF produces the following output when debugging in a console. The `op` section indicates that the result is append-only. The
original flag is encoded in the `flag` column.

```text
+----+--------------------------------+--------------------------------+-------------+
| op |                           name |                           flag |         sum |
+----+--------------------------------+--------------------------------+-------------+
| +I |                            Bob |                         INSERT |          23 |
| +I |                          Alice |                         INSERT |          42 |
| +I |                          Alice |                   UPDATE_AFTER |          44 |
+----+--------------------------------+--------------------------------+-------------+
```

#### Limitations
- The `ArgumentTrait.PASS_COLUMNS_THROUGH` is not supported if `ArgumentTrait.SUPPORTS_UPDATES` is declared.
- The `on_time` argument is not supported if the PTF receives updates.

### Updating Function Output

The `ChangelogFunction` interface makes it possible for a function to declare the types of changes (e.g., inserts, updates,
deletes) that it may emit, allowing the planner to make informed decisions during query planning.

{{< hint info >}}
The interface is intended for advanced use cases and should be implemented with care. Emitting an incorrect changelog
from the PTF may lead to undefined behavior in the overall query.
{{< /hint >}}

The resulting changelog mode can be influenced by:
- The changelog mode of the input table arguments, accessible via `ChangelogContext.getTableChangelogMode(int)`.
- The changelog mode required by downstream operators, accessible via `ChangelogContext.getRequiredChangelogMode()`.

Changelog mode inference in the planner involves several steps. The `getChangelogMode(ChangelogContext)` method is
called for each step:

1. The planner checks whether the PTF emits updates or inserts-only.
2. If updates are emitted, the planner determines whether the updates include {@link
   RowKind#UPDATE_BEFORE} messages (retract mode), or whether {@link RowKind#UPDATE_AFTER}
   messages are sufficient (upsert mode). For this, {@link #getChangelogMode} might be called
   twice to query both retract mode and upsert mode capabilities as indicated by {@link
   ChangelogContext#getRequiredChangelogMode()}.
3. If in upsert mode, the planner checks whether {@link RowKind#DELETE} messages contain all
   fields (full deletes) or only key fields (partial deletes). In the case of partial deletes,
   only the upsert key fields are set when a row is removed; all non-key fields are null,
   regardless of nullability constraints. {@link ChangelogContext#getRequiredChangelogMode()}
   indicates whether a downstream operator requires full deletes.

Emitting changelogs is only valid for PTFs that take table arguments with set semantics (see `ArgumentTrait.TABLE_AS_SET`).
In case of upserts, the upsert key must be equal to the PARTITION BY key.

It is perfectly valid for a `ChangelogFunction` implementation to return a fixed `ChangelogMode`, regardless of the
`ChangelogContext`. This approach may be appropriate when the PTF is designed for a specific scenario or pipeline setup,
and does not need to adapt dynamically to different input modes. Note that in such cases, the PTFs applicability is limited,
as it may only function correctly within the predefined context for which it was designed.

In some cases, this interface should be used in combination with `SpecializedFunction`
to reconfigure the PTF after the final changelog mode for the specific call location has been
determined. The final changelog mode is also available during runtime via
`ProcessTableFunction.Context.getChangelogMode()`.

#### Example: Custom Aggregation

The following function demonstrates how a PTF can implement an aggregation function that is able to emit updates based
on custom conditional logic. The function takes a table of score results partitioned by `name` and maintains a sum per
partition. Scores that are lower than `0` are treated as incorrect and invalidate the entire aggregation for this key.

{{< tabs "2037eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

Table data = env
  .fromValues(
    Row.of("Bob", 23),
    Row.of("Alice", 42),
    Row.of("Alice", 2),
    Row.of("Bob", -1),
    Row.of("Bob", 45))
  .as("name", "score");

// Call the PTF on an append-only table
Table aggregated = data
  .partitionBy($("name"))
  .process(CustomAggregation.class);

aggregated.execute().print();

// --------------------
// Function declaration
// --------------------

@DataTypeHint("ROW<sum INT>")
public static class CustomAggregation
  extends ProcessTableFunction<Row>
  implements ChangelogFunction {

  @Override
  public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
    // Tells the system that the PTF produces updates encoded as retractions
    return ChangelogMode.all();
  }

  public static class Accumulator {
    public Integer sum = 0;
  }

  public void eval(@StateHint Accumulator state, @ArgumentHint(TABLE_AS_SET) Row input) {
    int score = input.getFieldAs("score");

    // A negative state indicates that the partition
    // key has been marked as invalid before
    if (state.sum == -1) {
      return;
    }

    // A negative score marks the entire aggregation result as invalid.
    if (score < 0) {
      // Send out a -D for the affected partition key and
      // mark the invalidation in state. All subsequent operations
      // and sinks will remove the aggregation result.
      collect(Row.ofKind(RowKind.DELETE, state.sum));
      state.sum = -1;
    } else {
      if (state.sum == 0) {
        // Emit +I for the first valid aggregation result.
        state.sum += score;
        collect(Row.ofKind(RowKind.INSERT, state.sum));
      } else {
        // Emit -U (with old aggregation result) and +U (with new aggregation result)
        // for encoding the update.
        collect(Row.ofKind(RowKind.UPDATE_BEFORE, state.sum));
        state.sum += score;
        collect(Row.ofKind(RowKind.UPDATE_AFTER, state.sum));
      }
    }
  }
}
```
{{< /tab >}}
{{< /tabs >}}

The PTF produces the following output when debugging in a console. The `op` section indicates that the result is updating. However,
no updates to `Bob` are forwarded after the invalid `-1` is received, causing the PTF to ignore the update with value `45`.
The aggregation results for `Alice` contain only valid scores and are preserved in a materialized table.

```text
+----+--------------------------------+-------------+
| op |                           name |         sum |
+----+--------------------------------+-------------+
| +I |                            Bob |          23 |
| +I |                          Alice |          42 |
| -U |                          Alice |          42 |
| +U |                          Alice |          44 |
| -D |                            Bob |          23 |
+----+--------------------------------+-------------+
```

#### Limitations
- The `on_time` argument is not supported if the PTF emits updates.
- Currently, it is difficult to test upsert PTFs because debugging sinks such as `collect()` operate in retract mode and
  will request retract support from any PTF. Upserting PTFs have to be tested with upserting sinks (e.g. `kafka-upsert` connector).

{{< top >}}

Advanced Examples
-----------------

### Shopping Cart

The following example shows a typical PTF use case for modelling a shopping cart. Different events influence the content
of the cart. In this example, each user might `ADD` or `REMOVE` items. In the success case, the user completes the transaction
with `CHECKOUT.`

Take the following input table:

```text
+----+--------------------------------+--------------------------------+----------------------+-------------------------+
| op |                           user |                      eventType |            productId |                      ts |
+----+--------------------------------+--------------------------------+----------------------+-------------------------+
| +I |                            Bob |                            ADD |                    1 | 2025-03-27 12:00:11.000 |
| +I |                          Alice |                            ADD |                    1 | 2025-03-27 12:00:21.000 |
| +I |                            Bob |                         REMOVE |                    1 | 2025-03-27 12:00:51.000 |
| +I |                            Bob |                            ADD |                    2 | 2025-03-27 12:00:55.000 |
| +I |                            Bob |                            ADD |                    5 | 2025-03-27 12:00:56.000 |
| +I |                            Bob |                       CHECKOUT |               <NULL> | 2025-03-27 12:01:50.000 |
```

The `CheckoutProcessor` PTF is designed to process these events and store the shopping cart content in state until checkout
is completed. It also incorporates reminder and timeout logic. If the user remains inactive for a specified duration, a
`REMINDER` event with the current cart content is emitted. Upon receiving the `CHECKOUT` event, the PTF is cleared and
the checkout event is sent.

{{< tabs "1537eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that implements the core business logic of a shopping cart.
// The PTF takes ADD, REMOVE, CHECKOUT events and can send out either a REMINDER or CHECKOUT event.
@DataTypeHint("ROW<checkout_type STRING, items MAP<BIGINT, INT>>")
public static class CheckoutProcessor extends ProcessTableFunction<Row> {

  // Object that is stored in state.
  public static class ShoppingCart {

    // The system needs to be able to access all fields for persistence and restore.

    // A map for product IDs to number of items.
    public Map<Long, Integer> content = new HashMap<>();

    // Arbitrary helper methods can be added for structuring the code.

    public void addItem(long productId) {
      content.compute(productId, (k, v) -> (v == null) ? 1 : v + 1);
    }

    public void removeItem(long productId) {
      content.compute(productId, (k, v) -> (v == null || v == 1) ? null : v - 1);
    }

    public boolean hasContent() {
      return !content.isEmpty();
    }
  }

  // Main processing logic
  public void eval(
    Context ctx,
    @StateHint ShoppingCart cart,
    @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row events,
    Duration reminderInterval,
    Duration timeoutInterval
  ) {
    String eventType = events.getFieldAs("eventType");
    Long productId = events.getFieldAs("productId");
    switch (eventType) {
      // ADD item
      case "ADD":
        cart.addItem(productId);
        updateTimers(ctx, reminderInterval, timeoutInterval);
        break;

      // REMOVE item
      case "REMOVE":
        cart.removeItem(productId);
        if (cart.hasContent()) {
          updateTimers(ctx, reminderInterval, timeoutInterval);
        } else {
          ctx.clearAll();
        }
        break;

      // CHECKOUT process
      case "CHECKOUT":
        if (cart.hasContent()) {
          collect(Row.of("CHECKOUT", cart.content));
        }
        ctx.clearAll();
        break;
    }
  }

  // Executes REMINDER and TIMEOUT events
  public void onTimer(OnTimerContext ctx, ShoppingCart cart) {
    switch (ctx.currentTimer()) {
      // Send reminder event
      case "REMINDER":
        collect(Row.of("REMINDER", cart.content));
        break;

      // Cancel transaction
      case "TIMEOUT":
        ctx.clearAll();
        break;
    }
  }

  // Helper method that sets or replaces timers for REMINDER and TIMEOUT
  private void updateTimers(Context ctx, Duration reminderInterval, Duration timeoutInterval) {
    TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
    timeCtx.registerOnTime("REMINDER", timeCtx.time().plus(reminderInterval));
    timeCtx.registerOnTime("TIMEOUT", timeCtx.time().plus(timeoutInterval));
  }
}
```
{{< /tab >}}
{{< /tabs >}}

The output could look similar to the following. Here we assume a very short reminder interval of 1 second.

```text
+----+--------------------------------+--------------------------------+--------------------------------+-------------------------+
| op |                           user |                  checkout_type |                          items |                 rowtime |
+----+--------------------------------+--------------------------------+--------------------------------+-------------------------+
| +I |                            Bob |                       REMINDER |                          {1=1} | 2025-03-27 12:00:12.000 |
| +I |                          Alice |                       REMINDER |                          {1=1} | 2025-03-27 12:00:22.000 |
| +I |                            Bob |                       CHECKOUT |                     {2=1, 5=1} | 2025-03-27 12:01:50.000 |
```

In a real-world scenario, the output would likely be split across two separate systems. Reminders may be placed in an email
notification queue, while the checkout process would be finalized by a separate downstream system.

```sql
CREATE VIEW Checkouts AS SELECT * FROM CheckoutProcessor(
  events => TABLE Events PARTITION BY `user`,
  on_time => DESCRIPTOR(ts),
  reminderInterval => INTERVAL '1' DAY,
  timeoutInterval => INTERVAL '2' DAY, uid => 'cart-processor'
)

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO EmailNotifications SELECT * FROM Checkouts WHERE `checkout_type` = 'REMINDER';
  INSERT INTO CheckoutEvents SELECT * FROM Checkouts WHERE `checkout_type` = 'CHECKOUT';
END;
```

By defining a view and using the same `uid` in both `INSERT INTO` paths, the resulting Flink job uses a split behavior
while the PTF exists once in the pipeline.

{{< top >}}

### Payment Joining

The following example shows how a PTF can be used for joining. Additionally, it also showcases how a PTF can be used as
a data generator for creating bounded tables with dummy data.

{{< tabs "1637eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// ---------------------------
// Table program
// ---------------------------
TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

// Generate data with a unified schema
Table orders = env.fromCall(OrderGenerator.class);
Table payments = env.fromCall(PaymentGenerator.class);

// Partition orders and payments and pass them into the Joiner function
Table joined = env.fromCall(
  Joiner.class,
  orders.partitionBy($("id")).asArgument("order"),
  payments.partitionBy($("orderId")).asArgument("payment"));

joined.execute().print();

// ---------------------------
// Data Generation
// ---------------------------

// A PTF that generates Orders
public static class OrderGenerator extends ProcessTableFunction<Order> {
  public void eval() {
    Stream.of(
      Order.of("Bob", 1000001, 23.46, "USD"),
      Order.of("Bob", 1000021, 6.99, "USD"),
      Order.of("Alice", 1000601, 0.79, "EUR"),
      Order.of("Charly", 1000703, 100.60, "EUR")
    )
    .forEach(this::collect);
  }
}

// A PTF that generates Payments
public static class PaymentGenerator extends ProcessTableFunction<Payment> {
  public void eval() {
    Stream.of(
      Payment.of(999997870, 1000001),
      Payment.of(999997870, 1000001),
      Payment.of(999993331, 1000021),
      Payment.of(999994111, 1000601)
    )
    .forEach(this::collect);
  }
}

// Order POJO
public static class Order {
  public String userId;
  public int id;
  public double amount;
  public String currency;

  public static Order of(String userId, int id, double amount, String currency) {
    Order order = new Order();
    order.userId = userId;
    order.id = id;
    order.amount = amount;
    order.currency = currency;
    return order;
  }
}

// Payment POJO
public static class Payment {
  public int id;
  public int orderId;

  public static Payment of(int id, int orderId) {
    Payment payment = new Payment();
    payment.id = id;
    payment.orderId = orderId;
    return payment;
  }
}
```
{{< /tab >}}
{{< /tabs >}}

After generating the data, the stateful Joiner buffers events until a matching pair is found. Any duplicates in either
of the input tables are ignored.

{{< tabs "1737eeed-3d13-455c-8e2f-5e164da9f844" >}}
{{< tab "Java" >}}
```java
// Function that buffers one object of each side to find exactly one join result.
// The function expects that a payment event enters within 1 hour.
// Otherwise, state is discarded using TTL.
public static class Joiner extends ProcessTableFunction<JoinResult> {
  public void eval(
    Context ctx,
    @StateHint(ttl = "1 hour") JoinResult seen,
    @ArgumentHint(TABLE_AS_SET) Order order,
    @ArgumentHint(TABLE_AS_SET) Payment payment
  ) {
    if (input.order != null) {
      if (seen.order != null) {
        // skip duplicates
        return;
      } else {
        // wait for matching payment
        seen.order = input.order;
      }
    } else if (input.payment != null) {
      if (seen.payment != null) {
        // skip duplicates
        return;
      } else {
        // wait for matching order
        seen.payment = input.payment;
      }
    }

    if (seen.order != null && seen.payment != null) {
      // Send out the final join result
      collect(seen);
    }
  }
}

// POJO for the output of Joiner
public static class JoinResult {
  public Order order;
  public Payment payment;
}
```
{{< /tab >}}
{{< /tabs >}}

The output could look similar to the following. Duplicate events for payment `999997870` have been filtered out. A match
for `Charly` could not be found.

```text
+----+-------------+-------------+--------------------------------+--------------------------------+
| op |          id |     orderId |                          order |                        payment |
+----+-------------+-------------+--------------------------------+--------------------------------+
| +I |     1000021 |     1000021 | (amount=6.99, currency=USD,... | (id=999993331, orderId=1000... |
| +I |     1000601 |     1000601 | (amount=0.79, currency=EUR,... | (id=999994111, orderId=1000... |
| +I |     1000001 |     1000001 | (amount=23.46, currency=USD... | (id=999997870, orderId=1000... |
+----+-------------+-------------+--------------------------------+--------------------------------+
```

Limitations
-----------

PTFs are in an early stage. The following limitations apply:
- PTFs cannot run in batch mode.
- Broadcast state
