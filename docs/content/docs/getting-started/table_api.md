---
title: 'Table API Tutorial'
nav-title: 'Table API Tutorial'
weight: 3
type: docs
aliases:
  - /try-flink/table_api.html
  - /getting-started/walkthroughs/table_api.html
  - /dev/python/table_api_tutorial.html
  - /tutorials/python_table_api.html
  - /getting-started/walkthroughs/python_table_api.html
  - /try-flink/python_table_api.html
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

# Table API Tutorial

Apache Flink offers a Table API as a unified, relational API for batch and stream processing. Queries are executed with the same semantics on unbounded, real-time streams or bounded, batch data sets and produce the same results. The Table API in Flink is commonly used for data analytics, data pipelining, and ETL applications.

## What You'll Build

In this tutorial, you will build a spend report that aggregates transaction amounts by account and hour:

```
Transactions (generated data) → Flink (Table API aggregation) → Console (results)
```

You'll learn how to:

- Set up a Table API streaming environment
- Create tables using the Table API
- Write continuous aggregations using Table API operations
- Implement user-defined functions (UDFs)
- Use time-based windows for aggregation
- Test streaming applications in batch mode

## Prerequisites

This walkthrough assumes that you have some familiarity with Java or Python, but you should be able to follow along even if you come from a different programming language. It also assumes that you are familiar with basic relational concepts such as `SELECT` and `GROUP BY` clauses.

## Help, I'm Stuck!

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly.

## How To Follow Along

If you want to follow along, you will require a computer with:

{{< tabs "prerequisites" >}}
{{< tab "Java" >}}
* Java 11, 17, or 21
* Maven
{{< /tab >}}
{{< tab "Python" >}}
* Java 11, 17, or 21
* Python 3.9, 3.10, 3.11, or 3.12
{{< /tab >}}
{{< /tabs >}}

{{< tabs "setup" >}}
{{< tab "Java" >}}

A provided Flink Maven Archetype will create a skeleton project with all the necessary dependencies quickly, so you only need to focus on filling out the business logic.

{{< unstable >}}
{{< hint warning >}}
**Attention:** The Maven archetype is only available for released versions of Apache Flink.

Since you are currently looking at the latest SNAPSHOT
version of the documentation, all version references below will not work.
Please switch the documentation to the latest released version via the release picker which you find on the left side below the menu.
{{< /hint >}}
{{< /unstable >}}

```bash
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-table-java \
    -DarchetypeVersion={{< version >}} \
    -DgroupId=spendreport \
    -DartifactId=spendreport \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
```

You can edit the `groupId`, `artifactId` and `package` if you like. With the above parameters, Maven will create a folder named `spendreport` that contains a project with all the dependencies to complete this tutorial.

After importing the project into your editor, you can find a file `SpendReport.java` with the following code which you can run directly inside your IDE.

{{< hint info >}}
**Running in an IDE:** If you encounter a `java.lang.NoClassDefFoundError` exception, this is likely because you do not have all required Flink dependencies on the classpath.

* **IntelliJ IDEA:** Go to Run > Edit Configurations > Modify options > Select "include dependencies with 'Provided' scope".
{{< /hint >}}

{{< /tab >}}
{{< tab "Python" >}}

Using Python Table API requires installing PyFlink, which is available on [PyPI](https://pypi.org/project/apache-flink/) and can be easily installed using `pip`:

```bash
$ python -m pip install apache-flink
```

{{< hint info >}}
**Tip:** We recommend installing PyFlink in a [virtual environment](https://docs.python.org/3/library/venv.html) to keep your project dependencies isolated.
{{< /hint >}}

Once PyFlink is installed, create a new file called `spend_report.py` where you will write the Table API program.

{{< /tab >}}
{{< /tabs >}}

## The Complete Program

Here is the complete code for the spend report program:

{{< tabs "complete-program" >}}
{{< tab "Java" >}}

```java
public class SpendReport {

    public static void main(String[] args) throws Exception {
        // Create a Table environment for streaming
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create the source table
        // The DataGen connector generates an infinite stream of transactions
        tEnv.createTemporaryTable("transactions",
            TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                    .column("accountId", DataTypes.BIGINT())
                    .column("amount", DataTypes.BIGINT())
                    .column("transactionTime", DataTypes.TIMESTAMP(3))
                    .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
                    .build())
                .option("rows-per-second", "100")
                .option("fields.accountId.min", "1")
                .option("fields.accountId.max", "5")
                .option("fields.amount.min", "1")
                .option("fields.amount.max", "1000")
                .build());

        // Read from the source table
        Table transactions = tEnv.from("transactions");

        // Apply the business logic
        Table result = report(transactions);

        // Print the results to the console
        result.execute().print();
    }

    public static Table report(Table transactions) {
        return transactions
            .select(
                $("accountId"),
                $("transactionTime").floor(TimeIntervalUnit.HOUR).as("logTs"),
                $("amount"))
            .groupBy($("accountId"), $("logTs"))
            .select(
                $("accountId"),
                $("logTs"),
                $("amount").sum().as("amount"));
    }
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table import TableEnvironment, EnvironmentSettings, TableDescriptor, Schema, DataTypes
from pyflink.table.expression import TimeIntervalUnit
from pyflink.table.expressions import col

def main():
    # Create a Table environment for streaming
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # Write all results to one file for easier viewing
    t_env.get_config().set("parallelism.default", "1")

    # Create the source table
    # The DataGen connector generates an infinite stream of transactions
    t_env.create_temporary_table(
        "transactions",
        TableDescriptor.for_connector("datagen")
            .schema(Schema.new_builder()
                .column("accountId", DataTypes.BIGINT())
                .column("amount", DataTypes.BIGINT())
                .column("transactionTime", DataTypes.TIMESTAMP(3))
                .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
                .build())
            .option("rows-per-second", "100")
            .option("fields.accountId.min", "1")
            .option("fields.accountId.max", "5")
            .option("fields.amount.min", "1")
            .option("fields.amount.max", "1000")
            .build())

    # Read from the source table
    transactions = t_env.from_path("transactions")

    # Apply the business logic
    result = report(transactions)

    # Print the results to the console
    result.execute().print()

def report(transactions):
    return transactions \
        .select(
            col("accountId"),
            col("transactionTime").floor(TimeIntervalUnit.HOUR).alias("logTs"),
            col("amount")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs"),
            col("amount").sum.alias("amount"))

if __name__ == '__main__':
    main()
```

{{< /tab >}}
{{< /tabs >}}

## Breaking Down The Code

### The Execution Environment

The first lines set up your `TableEnvironment`. The table environment is how you set properties for your Job, specify whether you are writing a batch or a streaming application, and create your sources. This walkthrough creates a standard table environment that uses streaming execution.

{{< tabs "environment" >}}
{{< tab "Java" >}}

```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

{{< /tab >}}
{{< tab "Python" >}}

```python
settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)
```

{{< /tab >}}
{{< /tabs >}}

### Creating Tables

Next, a table is created that represents the transactions data. The DataGen connector generates an infinite stream of random transactions.

{{< tabs "create-table" >}}
{{< tab "Java" >}}

Using `TableDescriptor`, you can define tables programmatically:

```java
tEnv.createTemporaryTable("transactions",
    TableDescriptor.forConnector("datagen")
        .schema(Schema.newBuilder()
            .column("accountId", DataTypes.BIGINT())
            .column("amount", DataTypes.BIGINT())
            .column("transactionTime", DataTypes.TIMESTAMP(3))
            .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
            .build())
        .option("rows-per-second", "100")
        .option("fields.accountId.min", "1")
        .option("fields.accountId.max", "5")
        .option("fields.amount.min", "1")
        .option("fields.amount.max", "1000")
        .build());
```

{{< /tab >}}
{{< tab "Python" >}}

Using `TableDescriptor`, you can define tables programmatically:

```python
t_env.create_temporary_table(
    "transactions",
    TableDescriptor.for_connector("datagen")
        .schema(Schema.new_builder()
            .column("accountId", DataTypes.BIGINT())
            .column("amount", DataTypes.BIGINT())
            .column("transactionTime", DataTypes.TIMESTAMP(3))
            .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
            .build())
        .option("rows-per-second", "100")
        .option("fields.accountId.min", "1")
        .option("fields.accountId.max", "5")
        .option("fields.amount.min", "1")
        .option("fields.amount.max", "1000")
        .build())
```

{{< /tab >}}
{{< /tabs >}}

The transactions table generates credit card transactions with:
- `accountId`: Account ID between 1 and 5
- `amount`: Transaction amount between 1 and 1000
- `transactionTime`: Timestamp with a watermark for handling late data

### The Query

With the environment configured and tables registered, you are ready to build your first application.
From the `TableEnvironment` you can read `from` an input table and apply Table API operations.
The `report` function is where you implement your business logic.

{{< tabs "query" >}}
{{< tab "Java" >}}

```java
Table transactions = tEnv.from("transactions");
Table result = report(transactions);
result.execute().print();
```

{{< /tab >}}
{{< tab "Python" >}}

```python
transactions = t_env.from_path("transactions")
result = report(transactions)
result.execute().print()
```

{{< /tab >}}
{{< /tabs >}}

## Implementing the Report

Now with the skeleton of a Job set up, you are ready to add some business logic. The goal is to build a report that shows the total spend for each account across each hour of the day. This means the timestamp column needs to be rounded down from millisecond to hour granularity.

Flink supports developing relational applications in pure [SQL]({{< ref "docs/sql/reference/overview" >}}) or using the [Table API]({{< ref "docs/dev/table/tableApi" >}}). The Table API is a fluent DSL inspired by SQL that can be written in Java or Python and supports strong IDE integration. Just like a SQL query, Table programs can select the required fields and group by your keys. These features, along with [built-in functions]({{< ref "docs/sql/functions/built-in-functions" >}}) like `floor` and `sum`, enable you to write this report.

{{< tabs "report-impl" >}}
{{< tab "Java" >}}

```java
public static Table report(Table transactions) {
    return transactions
        .select(
            $("accountId"),
            $("transactionTime").floor(TimeIntervalUnit.HOUR).as("logTs"),
            $("amount"))
        .groupBy($("accountId"), $("logTs"))
        .select(
            $("accountId"),
            $("logTs"),
            $("amount").sum().as("amount"));
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
def report(transactions):
    return transactions \
        .select(
            col("accountId"),
            col("transactionTime").floor(TimeIntervalUnit.HOUR).alias("logTs"),
            col("amount")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs"),
            col("amount").sum.alias("amount"))
```

{{< /tab >}}
{{< /tabs >}}

## Testing

{{< tabs "testing" >}}
{{< tab "Java" >}}

The project contains a test class `SpendReportTest` that validates the logic of the report using batch mode with static data.

```java
EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
TableEnvironment tEnv = TableEnvironment.create(settings);

// Create test data using fromValues
Table transactions = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("accountId", DataTypes.BIGINT()),
        DataTypes.FIELD("amount", DataTypes.BIGINT()),
        DataTypes.FIELD("transactionTime", DataTypes.TIMESTAMP(3))
    ),
    Row.of(1L, 188L, LocalDateTime.of(2024, 1, 1, 9, 0, 0)),
    Row.of(1L, 374L, LocalDateTime.of(2024, 1, 1, 9, 30, 0)),
    // ... more test data
);
```

{{< /tab >}}
{{< tab "Python" >}}

You can test the `report` function by switching to batch mode and using static test data. Create a separate test file (e.g., `test_spend_report.py`):

```python
from datetime import datetime
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes

from spend_report import report

def test_report():
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)

    # Create test data using from_elements
    transactions = t_env.from_elements(
        [
            (1, 188, datetime(2024, 1, 1, 9, 0, 0)),
            (1, 374, datetime(2024, 1, 1, 9, 30, 0)),
            (2, 200, datetime(2024, 1, 1, 9, 15, 0)),
        ],
        DataTypes.ROW([
            DataTypes.FIELD("accountId", DataTypes.BIGINT()),
            DataTypes.FIELD("amount", DataTypes.BIGINT()),
            DataTypes.FIELD("transactionTime", DataTypes.TIMESTAMP(3))
        ])
    )

    # Test the report function
    result = report(transactions)

    # Collect results and verify
    rows = [row for row in result.execute().collect()]
    assert len(rows) == 2  # Two accounts

if __name__ == '__main__':
    test_report()
    print("All tests passed!")
```

Run with `python test_spend_report.py` or `pytest test_spend_report.py`.

{{< /tab >}}
{{< /tabs >}}

One of Flink's unique properties is that it provides consistent semantics across batch and streaming. This means you can develop and test applications in batch mode on static datasets, and deploy to production as streaming applications.

## User Defined Functions

Flink contains a number of built-in functions, and sometimes you need to extend it with a [user-defined function]({{< ref "docs/dev/table/functions/udfs" >}}). If `floor` wasn't predefined, you could implement it yourself.

{{< tabs "udf" >}}
{{< tab "Java" >}}

```java
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class MyFloor extends ScalarFunction {

    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
        @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}
```

And then quickly integrate it in your application:

```java
public static Table report(Table transactions) {
    return transactions
        .select(
            $("accountId"),
            call(MyFloor.class, $("transactionTime")).as("logTs"),
            $("amount"))
        .groupBy($("accountId"), $("logTs"))
        .select(
            $("accountId"),
            $("logTs"),
            $("amount").sum().as("amount"));
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table.udf import udf
from datetime import datetime

@udf(result_type=DataTypes.TIMESTAMP(3))
def my_floor(timestamp: datetime) -> datetime:
    return timestamp.replace(minute=0, second=0, microsecond=0)
```

And then integrate it in your application:

```python
def report(transactions):
    return transactions \
        .select(
            col("accountId"),
            my_floor(col("transactionTime")).alias("logTs"),
            col("amount")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs"),
            col("amount").sum.alias("amount"))
```

{{< /tab >}}
{{< /tabs >}}

This query consumes all records from the `transactions` table, calculates the report, and outputs the results in an efficient, scalable manner. Running the test with this implementation will pass.

## Process Table Functions (Java only)

For more advanced row-by-row processing, Flink provides [Process Table Functions]({{< ref "docs/dev/table/functions/ptfs" >}}) (PTFs). PTFs can transform each row of a table and have access to powerful features like state and timers. Here's a simple stateless example that filters and formats high-value transactions:

```java
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

public class HighValueAlerts extends ProcessTableFunction<String> {

    private static final long HIGH_VALUE_THRESHOLD = 500;

    public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row transaction) {
        Long amount = transaction.getFieldAs("amount");
        if (amount > HIGH_VALUE_THRESHOLD) {
            Long accountId = transaction.getFieldAs("accountId");
            collect("Alert: Account " + accountId + " made a high-value transaction of " + amount);
        }
    }
}
```

You can try this PTF by modifying your `main()` method. Replace or add alongside the existing `report()` call:

```java
// Instead of (or in addition to) the aggregation report:
// Table result = report(transactions);
// result.execute().print();

// Try the PTF to see high-value transaction alerts:
Table alerts = transactions.process(HighValueAlerts.class);
alerts.execute().print();
```

This will output alerts only for transactions exceeding 500. PTFs become even more powerful when combined with state and timers for implementing complex event-driven logic - see the [PTF documentation]({{< ref "docs/dev/table/functions/ptfs" >}}) for more advanced examples.

{{< hint info >}}
**Note:** Process Table Functions are currently only available in Java. For Python, you can use [User-Defined Table Functions]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions) (UDTFs) for similar row-by-row processing.
{{< /hint >}}

## Adding Windows

Grouping data based on time is a typical operation in data processing, especially when working with infinite streams. A grouping based on time is called a [window]({{< ref "docs/dev/table/tableApi" >}}#groupby-window-aggregation) and Flink offers flexible windowing semantics. The most basic type of window is called a `Tumble` window, which has a fixed size and whose buckets do not overlap.

**Try modifying your `report()` function to use windows instead of `floor()`:**

{{< tabs "windows" >}}
{{< tab "Java" >}}

```java
public static Table report(Table transactions) {
    return transactions
        .window(Tumble.over(lit(10).seconds()).on($("transactionTime")).as("logTs"))
        .groupBy($("accountId"), $("logTs"))
        .select(
            $("accountId"),
            $("logTs").start().as("logTs"),
            $("amount").sum().as("amount"));
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

def report(transactions):
    return transactions \
        .window(Tumble.over(lit(10).seconds).on(col("transactionTime")).alias("logTs")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs").start.alias("logTs"),
            col("amount").sum.alias("amount"))
```

{{< /tab >}}
{{< /tabs >}}

This defines your application as using 10-second tumbling windows based on the timestamp column. So a row with timestamp `2024-01-01 01:23:47` is put in the `2024-01-01 01:23:40` window.

Aggregations based on time are unique because time, as opposed to other attributes, generally moves forward in a continuous streaming application. Unlike `floor` and your UDF, window functions are [intrinsics](https://en.wikipedia.org/wiki/Intrinsic_function), which allows the runtime to apply additional optimizations. In a batch context, windows offer a convenient API for grouping records by a timestamp attribute.

After making this change, run the application to see windowed results emitted every 10 seconds.

## Running the Application

And that's it, a fully functional, stateful, distributed streaming application! The query continuously generates transactions, computes the hourly spendings, and emits results as soon as they are ready. Since the input is unbounded, the query keeps running until it is manually stopped.

{{< tabs "running" >}}
{{< tab "Java" >}}

Run the `SpendReport` class in your IDE to see the streaming results printed to the console.

{{< /tab >}}
{{< tab "Python" >}}

Run the program from the command line:

```bash
$ python spend_report.py
```

The command builds and runs the Python Table API program in a local mini cluster. You can also submit the Python Table API program to a remote cluster. Refer to [Job Submission Examples]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs) for more details.

{{< /tab >}}
{{< /tabs >}}

## Next Steps

Congratulations on completing this tutorial! Here are some ways to continue learning:

### Learn More About the Table API

- [Table API Overview]({{< ref "docs/dev/table/tableApi" >}}): Complete Table API reference
- [User-Defined Functions]({{< ref "docs/dev/table/functions/udfs" >}}): Create custom functions for your pipelines
- [Streaming Concepts]({{< ref "docs/concepts/sql-table-concepts/overview" >}}): Understand dynamic tables, time attributes, and more

### Explore Other Tutorials

- [Flink SQL Tutorial]({{< ref "docs/getting-started/quickstart-sql" >}}): Interactive SQL queries without coding
- [DataStream API Tutorial]({{< ref "docs/getting-started/datastream" >}}): Build stateful streaming applications with the DataStream API
- [Flink Operations Playground]({{< ref "docs/getting-started/flink-operations-playground" >}}): Learn to operate Flink clusters

### Production Deployment

- [Deployment Overview]({{< ref "docs/deployment/overview" >}}): Deploy Flink in production
- [Connectors]({{< ref "docs/connectors/table/overview" >}}): Connect to Kafka, databases, filesystems, and more
