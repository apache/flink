---
title: "SQL Client"
nav-parent_id: tableapi
nav-pos: 100
is_beta: true
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


Flink’s Table & SQL API makes it possible to work with queries written in the SQL language, but these queries need to be embedded within a table program that is written in either Java or Scala. Moreover, these programs need to be packaged with a build tool before being submitted to a cluster. This more or less limits the usage of Flink to Java/Scala programmers.

The *SQL Client* aims to provide an easy way of writing, debugging, and submitting table programs to a Flink cluster without a single line of Java or Scala code. The *SQL Client CLI* allows for retrieving and visualizing real-time results from the running distributed application on the command line.

<a href="{{ site.baseurl }}/fig/sql_client_demo.gif"><img class="offset" src="{{ site.baseurl }}/fig/sql_client_demo.gif" alt="Animated demo of the Flink SQL Client CLI running table programs on a cluster" width="80%" /></a>

<span class="label label-danger">Attention</span> The SQL Client is in an early development phase. Even though the application is not production-ready yet, it can be a quite useful tool for prototyping and playing around with Flink SQL. In the future, the community plans to extend its functionality by providing a REST-based [SQL Client Gateway](sqlClient.html#limitations--future).

* This will be replaced by the TOC
{:toc}

Getting Started
---------------

This section describes how to setup and run your first Flink SQL program from the command-line.

The SQL Client is bundled in the regular Flink distribution and thus runnable out-of-the-box. It requires only a running Flink cluster where table programs can be executed. For more information about setting up a Flink cluster see the [Cluster & Deployment]({{ site.baseurl }}/ops/deployment/cluster_setup.html) part. If you simply want to try out the SQL Client, you can also start a local cluster with one worker using the following command:

{% highlight bash %}
./bin/start-cluster.sh
{% endhighlight %}

### Starting the SQL Client CLI

The SQL Client scripts are also located in the binary directory of Flink. [In the future](sqlClient.html#limitations--future), a user will have two possibilities of starting the SQL Client CLI either by starting an embedded standalone process or by connecting to a remote SQL Client Gateway. At the moment only the `embedded` mode is supported. You can start the CLI by calling:

{% highlight bash %}
./bin/sql-client.sh embedded
{% endhighlight %}

By default, the SQL Client will read its configuration from the environment file located in `./conf/sql-client-defaults.yaml`. See the [configuration part](sqlClient.html#environment-files) for more information about the structure of environment files.

### Running SQL Queries

Once the CLI has been started, you can use the `HELP` command to list all available SQL statements. For validating your setup and cluster connection, you can enter your first SQL query and press the `Enter` key to execute it:

{% highlight sql %}
SELECT 'Hello World'
{% endhighlight %}

This query requires no table source and produces a single row result. The CLI will retrieve results from the cluster and visualize them. You can close the result view by pressing the `Q` key.

The CLI supports **two modes** for maintaining and visualizing results.

The **table mode** materializes results in memory and visualizes them in a regular, paginated table representation. It can be enabled by executing the following command in the CLI:

{% highlight text %}
SET execution.result-mode=table
{% endhighlight %}

The **changelog mode** does not materialize results and visualizes the result stream that is produced by a [continuous query](streaming.html#dynamic-tables--continuous-queries) consisting of insertions (`+`) and retractions (`-`).

{% highlight text %}
SET execution.result-mode=changelog
{% endhighlight %}

You can use the following query to see both result modes in action:

{% highlight sql %}
SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name 
{% endhighlight %}

This query performs a bounded word count example.

In *changelog mode*, the visualized changelog should be similar to:

{% highlight text %}
+ Bob, 1
+ Alice, 1
+ Greg, 1
- Bob, 1
+ Bob, 2
{% endhighlight %}

In *table mode*, the visualized result table is continuously updated until the table program ends with:

{% highlight text %}
Bob, 2
Alice, 1
Greg, 1
{% endhighlight %}

The [configuration section](sqlClient.html#configuration) explains how to read from table sources and configure other table program properties.

{% top %}

Configuration
-------------

The SQL Client can be started with the following optional CLI commands. They are discussed in detail in the subsequent paragraphs.

{% highlight text %}
./bin/sql-client.sh embedded --help

Mode "embedded" submits Flink jobs from the local machine.

  Syntax: embedded [OPTIONS]
  "embedded" mode options:
     -d,--defaults <environment file>      The environment properties with which
                                           every new session is initialized.
                                           Properties might be overwritten by
                                           session properties.
     -e,--environment <environment file>   The environment properties to be
                                           imported into the session. It might
                                           overwrite default environment
                                           properties.
     -h,--help                             Show the help message with
                                           descriptions of all options.
     -j,--jar <JAR file>                   A JAR file to be imported into the
                                           session. The file might contain
                                           user-defined classes needed for the
                                           execution of statements such as
                                           functions, table sources, or sinks.
                                           Can be used multiple times.
     -l,--library <JAR directory>          A JAR file directory with which every
                                           new session is initialized. The files
                                           might contain user-defined classes
                                           needed for the execution of
                                           statements such as functions, table
                                           sources, or sinks. Can be used
                                           multiple times.
     -s,--session <session identifier>     The identifier for a session.
                                           'default' is the default identifier.
{% endhighlight %}

{% top %}

### Environment Files

A SQL query needs a configuration environment in which it is executed. The so-called *environment files* define available table sources and sinks, external catalogs, user-defined functions, and other properties required for execution and deployment.

Every environment file is a regular [YAML file](http://yaml.org/). An example of such a file is presented below.

{% highlight yaml %}
# Define table sources here.

tables:
  - name: MyTableName
    type: source
    connector:
      type: filesystem
      path: "/path/to/something.csv"
    format:
      type: csv
      fields:
        - name: MyField1
          type: INT
        - name: MyField2
          type: VARCHAR
      line-delimiter: "\n"
      comment-prefix: "#"
    schema:
      - name: MyField1
        type: INT
      - name: MyField2
        type: VARCHAR

# Define user-defined functions here.

functions:
  - name: myUDF
    from: class
    class: foo.bar.AggregateUDF
    constructor:
      - 7.6
      - false

# Execution properties allow for changing the behavior of a table program.

execution:
  type: streaming                   # required: execution mode either 'batch' or 'streaming'
  result-mode: table                # required: either 'table' or 'changelog'
  time-characteristic: event-time   # optional: 'processing-time' or 'event-time' (default)
  parallelism: 1                    # optional: Flink's parallelism (1 by default)
  periodic-watermarks-interval: 200 # optional: interval for periodic watermarks (200 ms by default)
  max-parallelism: 16               # optional: Flink's maximum parallelism (128 by default)
  min-idle-state-retention: 0       # optional: table program's minimum idle state time
  max-idle-state-retention: 0       # optional: table program's maximum idle state time

# Deployment properties allow for describing the cluster to which table programs are submitted to.

deployment:
  response-timeout: 5000
{% endhighlight %}

This configuration:

- defines an environment with a table source `MyTableName` that reads from a CSV file,
- defines a user-defined function `myUDF` that can be instantiated using the class name and two constructor parameters,
- specifies a parallelism of 1 for queries executed in this streaming environment,
- specifies an event-time characteristic, and
- runs queries in the `table` result mode.

Depending on the use case, a configuration can be split into multiple files. Therefore, environment files can be created for general purposes (*defaults environment file* using `--defaults`) as well as on a per-session basis (*session environment file* using `--environment`). Every CLI session is initialized with the default properties followed by the session properties. For example, the defaults environment file could specify all table sources that should be available for querying in every session whereas the session environment file only declares a specific state retention time and parallelism. Both default and session environment files can be passed when starting the CLI application. If no default environment file has been specified, the SQL Client searches for `./conf/sql-client-defaults.yaml` in Flink's configuration directory.

<span class="label label-danger">Attention</span> Properties that have been set within a CLI session (e.g. using the `SET` command) have highest precedence:

{% highlight text %}
CLI commands > session environment file > defaults environment file
{% endhighlight %}

Queries that are executed in a batch environment, can only be retrieved using the `table` result mode. 

{% top %}

### Dependencies

The SQL Client does not require to setup a Java project using Maven or SBT. Instead, you can pass the dependencies as regular JAR files that get submitted to the cluster. You can either specify each JAR file separately (using `--jar`) or define entire library directories (using `--library`). For connectors to external systems (such as Apache Kafka) and corresponding data formats (such as JSON), Flink provides **ready-to-use JAR bundles**. These JAR files are suffixed with `sql-jar` and can be downloaded for each release from the Maven central repository.

The full list of offered SQL JARs and documentation about how to use them can be found on the [connection to external systems page](connect.html).

The following example shows an environment file that defines a table source reading JSON data from Apache Kafka.

{% highlight yaml %}
tables:
  - name: TaxiRides
    type: source
    update-mode: append
    connector:
      property-version: 1
      type: kafka
      version: 0.11
      topic: TaxiRides
      startup-mode: earliest-offset
      properties:
        - key: zookeeper.connect
          value: localhost:2181
        - key: bootstrap.servers
          value: localhost:9092
        - key: group.id
          value: testGroup
    format:
      property-version: 1
      type: json
      schema: "ROW(rideId LONG, lon FLOAT, lat FLOAT, rideTime TIMESTAMP)"
    schema:
      - name: rideId
        type: LONG
      - name: lon
        type: FLOAT
      - name: lat
        type: FLOAT
      - name: rowTime
        type: TIMESTAMP
        rowtime:
          timestamps:
            type: "from-field"
            from: "rideTime"
          watermarks:
            type: "periodic-bounded"
            delay: "60000"
      - name: procTime
        type: TIMESTAMP
        proctime: true
{% endhighlight %}

The resulting schema of the `TaxiRide` table contains most of the fields of the JSON schema. Furthermore, it adds a rowtime attribute `rowTime` and processing-time attribute `procTime`.

Both `connector` and `format` allow to define a property version (which is currently version `1`) for future backwards compatibility.

{% top %}

User-defined Functions
--------------------
The SQL Client allows users to create custom, user-defined functions to be used in SQL queries. Currently, these functions are restricted to be defined programmatically in Java/Scala classes.

In order to provide a user-defined function, you need to first implement and compile a function class that extends `ScalarFunction`, `AggregateFunction` or `TableFunction` (see [User-defined Functions]({{ site.baseurl }}/dev/table/udfs.html)). One or more functions can then be packaged into a dependency JAR for the SQL Client.

All functions must be declared in an environment file before being called. For each item in the list of `functions`, one must specify

- a `name` under which the function is registered,
- the source of the function using `from` (restricted to be `class` for now),
- the `class` which indicates the fully qualified class name of the function and an optional list of `constructor` parameters for instantiation.

{% highlight yaml %}
functions:
  - name: ...               # required: name of the function
    from: class             # required: source of the function (can only be "class" for now)
    class: ...              # required: fully qualified class name of the function
    constructor:            # optimal: constructor parameters of the function class
      - ...                 # optimal: a literal parameter with implicit type
      - class: ...          # optimal: full class name of the parameter
        constructor:        # optimal: constructor parameters of the parameter's class
          - type: ...       # optimal: type of the literal parameter
            value: ...      # optimal: value of the literal parameter
{% endhighlight %}

Make sure that the order and types of the specified parameters strictly match one of the constructors of your function class.

### Constructor Parameters

Depending on the user-defined function, it might be necessary to parameterize the implementation before using it in SQL statements.

As shown in the example before, when declaring a user-defined function, a class can be configured using constructor parameters in one of the following three ways:

**A literal value with implicit type:** The SQL Client will automatically derive the type according to the literal value itself. Currently, only values of `BOOLEAN`, `INT`, `DOUBLE` and `VARCHAR` are supported here.
If the automatic derivation does not work as expected (e.g., you need a VARCHAR `false`), use explicit types instead.

{% highlight yaml %}
- true         # -> BOOLEAN (case sensitive)
- 42           # -> INT
- 1234.222     # -> DOUBLE
- foo          # -> VARCHAR
{% endhighlight %}

**A literal value with explicit type:** Explicitly declare the parameter with `type` and `value` properties for type-safety.

{% highlight yaml %}
- type: DECIMAL
  value: 11111111111111111
{% endhighlight %}

The table below illustrates the supported Java parameter types and the corresponding SQL type strings.

| Java type               |  SQL type         |
| :---------------------- | :---------------- |
| `java.math.BigDecimal`  | `DECIMAL`         |
| `java.lang.Boolean`     | `BOOLEAN`         |
| `java.lang.Byte`        | `TINYINT`         |
| `java.lang.Double`      | `DOUBLE`          |
| `java.lang.Float`       | `REAL`, `FLOAT`   |
| `java.lang.Integer`     | `INTEGER`, `INT`  |
| `java.lang.Long`        | `BIGINT`          |
| `java.lang.Short`       | `SMALLINT`        |
| `java.lang.String`      | `VARCHAR`         |

More types (e.g., `TIMESTAMP` or `ARRAY`), primitive types, and `null` are not supported yet.

**A (nested) class instance:** Besides literal values, you can also create (nested) class instances for constructor parameters by specifying the `class` and `constructor` properties.
This process can be recursively performed until all the constructor parameters are represented with literal values.

{% highlight yaml %}
- class: foo.bar.paramClass
  constructor:
    - StarryName
    - class: java.lang.Integer
      constructor:
        - class: java.lang.String
          constructor:
            - type: VARCHAR
              value: 3
{% endhighlight %}

{% top %}

Limitations & Future
--------------------

The current SQL Client implementation is in a very early development stage and might change in the future as part of the bigger Flink Improvement Proposal 24 ([FLIP-24](https://cwiki.apache.org/confluence/display/FLINK/FLIP-24+-+SQL+Client)). Feel free to join the discussion and open issue about bugs and features that you find useful.

{% top %}
