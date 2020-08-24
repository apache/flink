---
title: "SQL Client"
nav-parent_id: tableapi
nav-pos: 90
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
SELECT 'Hello World';
{% endhighlight %}

This query requires no table source and produces a single row result. The CLI will retrieve results from the cluster and visualize them. You can close the result view by pressing the `Q` key.

The CLI supports **three modes** for maintaining and visualizing results.

The **table mode** materializes results in memory and visualizes them in a regular, paginated table representation. It can be enabled by executing the following command in the CLI:

{% highlight text %}
SET execution.result-mode=table;
{% endhighlight %}

The **changelog mode** does not materialize results and visualizes the result stream that is produced by a [continuous query](streaming/dynamic_tables.html#continuous-queries) consisting of insertions (`+`) and retractions (`-`).

{% highlight text %}
SET execution.result-mode=changelog;
{% endhighlight %}

The **tableau mode** is more like a traditional way which will display the results in the screen directly with a tableau format.
The displaying content will be influenced by the query execution type(`execution.type`).

{% highlight text %}
SET execution.result-mode=tableau;
{% endhighlight %}

Note that when you use this mode with streaming query, the result will be continuously printed on the console. If the input data of 
this query is bounded, the job will terminate after Flink processed all input data, and the printing will also be stopped automatically.
Otherwise, if you want to terminate a running query, just type `CTRL-C` in this case, the job and the printing will be stopped.

You can use the following query to see all the result modes in action:

{% highlight sql %}
SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name;
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

In *tableau mode*, if you ran the query in streaming mode, the displayed result would be:
{% highlight text %}
+-----+----------------------+----------------------+
| +/- |                 name |                  cnt |
+-----+----------------------+----------------------+
|   + |                  Bob |                    1 |
|   + |                Alice |                    1 |
|   + |                 Greg |                    1 |
|   - |                  Bob |                    1 |
|   + |                  Bob |                    2 |
+-----+----------------------+----------------------+
Received a total of 5 rows
{% endhighlight %}

And if you ran the query in batch mode, the displayed result would be:
{% highlight text %}
+-------+-----+
|  name | cnt |
+-------+-----+
| Alice |   1 |
|   Bob |   2 |
|  Greg |   1 |
+-------+-----+
3 rows in set
{% endhighlight %}

All these result modes can be useful during the prototyping of SQL queries. In all these modes, results are stored in the Java heap memory of the SQL Client. In order to keep the CLI interface responsive, the changelog mode only shows the latest 1000 changes. The table mode allows for navigating through bigger results that are only limited by the available main memory and the configured [maximum number of rows](sqlClient.html#configuration) (`max-table-result-rows`).

<span class="label label-danger">Attention</span> Queries that are executed in a batch environment, can only be retrieved using the `table` or `tableau` result mode.

After a query is defined, it can be submitted to the cluster as a long-running, detached Flink job. For this, a target system that stores the results needs to be specified using the [INSERT INTO statement](sqlClient.html#detached-sql-queries). The [configuration section](sqlClient.html#configuration) explains how to declare table sources for reading data, how to declare table sinks for writing data, and how to configure other table program properties.

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
     -hist,--history <History file path>   The file which you want to save the
                                           command history into. If not
                                           specified, we will auto-generate one
                                           under your user's home directory.
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
     -pyarch,--pyArchives <arg>            Add python archive files for job. The
                                           archive files will be extracted to
                                           the working directory of python UDF
                                           worker. Currently only zip-format is
                                           supported. For each archive file, a
                                           target directory be specified. If the
                                           target directory name is specified,
                                           the archive file will be extracted to
                                           a name can directory with the
                                           specified name. Otherwise, the
                                           archive file will be extracted to a
                                           directory with the same name of the
                                           archive file. The files uploaded via
                                           this option are accessible via
                                           relative path. '#' could be used as
                                           the separator of the archive file
                                           path and the target directory name.
                                           Comma (',') could be used as the
                                           separator to specify multiple archive
                                           files. This option can be used to
                                           upload the virtual environment, the
                                           data files used in Python UDF (e.g.:
                                           --pyArchives
                                           file:///tmp/py37.zip,file:///tmp/data
                                           .zip#data --pyExecutable
                                           py37.zip/py37/bin/python). The data
                                           files could be accessed in Python
                                           UDF, e.g.: f = open('data/data.txt',
                                           'r').
     -pyexec,--pyExecutable <arg>          Specify the path of the python
                                           interpreter used to execute the
                                           python UDF worker (e.g.:
                                           --pyExecutable
                                           /usr/local/bin/python3). The python
                                           UDF worker depends on Python 3.5+,
                                           Apache Beam (version == 2.19.0), Pip
                                           (version >= 7.1.0) and SetupTools
                                           (version >= 37.0.0). Please ensure
                                           that the specified environment meets
                                           the above requirements.
     -pyfs,--pyFiles <pythonFiles>         Attach custom python files for job.
                                           These files will be added to the
                                           PYTHONPATH of both the local client
                                           and the remote python UDF worker. The
                                           standard python resource file
                                           suffixes such as .py/.egg/.zip or
                                           directory are all supported. Comma
                                           (',') could be used as the separator
                                           to specify multiple files (e.g.:
                                           --pyFiles
                                           file:///tmp/myresource.zip,hdfs:///$n
                                           amenode_address/myresource2.zip).
     -pyreq,--pyRequirements <arg>         Specify a requirements.txt file which
                                           defines the third-party dependencies.
                                           These dependencies will be installed
                                           and added to the PYTHONPATH of the
                                           python UDF worker. A directory which
                                           contains the installation packages of
                                           these dependencies could be specified
                                           optionally. Use '#' as the separator
                                           if the optional parameter exists
                                           (e.g.: --pyRequirements
                                           file:///tmp/requirements.txt#file:///
                                           tmp/cached_dir).
     -s,--session <session identifier>     The identifier for a session.
                                           'default' is the default identifier.
     -u,--update <SQL update statement>    Experimental (for testing only!):
                                           Instructs the SQL Client to
                                           immediately execute the given update
                                           statement after starting up. The
                                           process is shut down after the
                                           statement has been submitted to the
                                           cluster and returns an appropriate
                                           return code. Currently, this feature
                                           is only supported for INSERT INTO
                                           statements that declare the target
                                           sink table.
{% endhighlight %}

{% top %}

### Environment Files

A SQL query needs a configuration environment in which it is executed. The so-called *environment files* define available catalogs, table sources and sinks, user-defined functions, and other properties required for execution and deployment.

Every environment file is a regular [YAML file](http://yaml.org/). An example of such a file is presented below.

{% highlight yaml %}
# Define tables here such as sources, sinks, views, or temporal tables.

tables:
  - name: MyTableSource
    type: source-table
    update-mode: append
    connector:
      type: filesystem
      path: "/path/to/something.csv"
    format:
      type: csv
      fields:
        - name: MyField1
          data-type: INT
        - name: MyField2
          data-type: VARCHAR
      line-delimiter: "\n"
      comment-prefix: "#"
    schema:
      - name: MyField1
        data-type: INT
      - name: MyField2
        data-type: VARCHAR
  - name: MyCustomView
    type: view
    query: "SELECT MyField2 FROM MyTableSource"

# Define user-defined functions here.

functions:
  - name: myUDF
    from: class
    class: foo.bar.AggregateUDF
    constructor:
      - 7.6
      - false

# Define available catalogs

catalogs:
   - name: catalog_1
     type: hive
     property-version: 1
     hive-conf-dir: ...
   - name: catalog_2
     type: hive
     property-version: 1
     default-database: mydb2
     hive-conf-dir: ...

# Properties that change the fundamental execution behavior of a table program.

execution:
  planner: blink                    # optional: either 'blink' (default) or 'old'
  type: streaming                   # required: execution mode either 'batch' or 'streaming'
  result-mode: table                # required: either 'table' or 'changelog'
  max-table-result-rows: 1000000    # optional: maximum number of maintained rows in
                                    #   'table' mode (1000000 by default, smaller 1 means unlimited)
  time-characteristic: event-time   # optional: 'processing-time' or 'event-time' (default)
  parallelism: 1                    # optional: Flink's parallelism (1 by default)
  periodic-watermarks-interval: 200 # optional: interval for periodic watermarks (200 ms by default)
  max-parallelism: 16               # optional: Flink's maximum parallelism (128 by default)
  min-idle-state-retention: 0       # optional: table program's minimum idle state time
  max-idle-state-retention: 0       # optional: table program's maximum idle state time
  current-catalog: catalog_1        # optional: name of the current catalog of the session ('default_catalog' by default)
  current-database: mydb1           # optional: name of the current database of the current catalog
                                    #   (default database of the current catalog by default)
  restart-strategy:                 # optional: restart strategy
    type: fallback                  #   "fallback" to global restart strategy by default

# Configuration options for adjusting and tuning table programs.

# A full list of options and their default values can be found
# on the dedicated "Configuration" page.
configuration:
  table.optimizer.join-reorder-enabled: true
  table.exec.spill-compression.enabled: true
  table.exec.spill-compression.block-size: 128kb

# Properties that describe the cluster to which table programs are submitted to.

deployment:
  response-timeout: 5000
{% endhighlight %}

This configuration:

- defines an environment with a table source `MyTableSource` that reads from a CSV file,
- defines a view `MyCustomView` that declares a virtual table using a SQL query,
- defines a user-defined function `myUDF` that can be instantiated using the class name and two constructor parameters,
- connects to two Hive catalogs and uses `catalog_1` as the current catalog with `mydb1` as the current database of the catalog,
- uses the blink planner in streaming mode for running statements with event-time characteristic and a parallelism of 1,
- runs exploratory queries in the `table` result mode,
- and makes some planner adjustments around join reordering and spilling via configuration options.

Depending on the use case, a configuration can be split into multiple files. Therefore, environment files can be created for general purposes (*defaults environment file* using `--defaults`) as well as on a per-session basis (*session environment file* using `--environment`). Every CLI session is initialized with the default properties followed by the session properties. For example, the defaults environment file could specify all table sources that should be available for querying in every session whereas the session environment file only declares a specific state retention time and parallelism. Both default and session environment files can be passed when starting the CLI application. If no default environment file has been specified, the SQL Client searches for `./conf/sql-client-defaults.yaml` in Flink's configuration directory.

<span class="label label-danger">Attention</span> Properties that have been set within a CLI session (e.g. using the `SET` command) have highest precedence:

{% highlight text %}
CLI commands > session environment file > defaults environment file
{% endhighlight %}

#### Restart Strategies

Restart strategies control how Flink jobs are restarted in case of a failure. Similar to [global restart strategies]({{ site.baseurl }}/dev/restart_strategies.html) for a Flink cluster, a more fine-grained restart configuration can be declared in an environment file.

The following strategies are supported:

{% highlight yaml %}
execution:
  # falls back to the global strategy defined in flink-conf.yaml
  restart-strategy:
    type: fallback

  # job fails directly and no restart is attempted
  restart-strategy:
    type: none

  # attempts a given number of times to restart the job
  restart-strategy:
    type: fixed-delay
    attempts: 3      # retries before job is declared as failed (default: Integer.MAX_VALUE)
    delay: 10000     # delay in ms between retries (default: 10 s)

  # attempts as long as the maximum number of failures per time interval is not exceeded
  restart-strategy:
    type: failure-rate
    max-failures-per-interval: 1   # retries in interval until failing (default: 1)
    failure-rate-interval: 60000   # measuring interval in ms for failure rate
    delay: 10000                   # delay in ms between retries (default: 10 s)
{% endhighlight %}

{% top %}

### Dependencies

The SQL Client does not require to setup a Java project using Maven or SBT. Instead, you can pass the dependencies as regular JAR files that get submitted to the cluster. You can either specify each JAR file separately (using `--jar`) or define entire library directories (using `--library`). For connectors to external systems (such as Apache Kafka) and corresponding data formats (such as JSON), Flink provides **ready-to-use JAR bundles**. These JAR files can be downloaded for each release from the Maven central repository.

The full list of offered SQL JARs and documentation about how to use them can be found on the [connection to external systems page](connect.html).

The following example shows an environment file that defines a table source reading JSON data from Apache Kafka.

{% highlight yaml %}
tables:
  - name: TaxiRides
    type: source-table
    update-mode: append
    connector:
      property-version: 1
      type: kafka
      version: "0.11"
      topic: TaxiRides
      startup-mode: earliest-offset
      properties:
        bootstrap.servers: localhost:9092
        group.id: testGroup
    format:
      property-version: 1
      type: json
      schema: "ROW<rideId LONG, lon FLOAT, lat FLOAT, rideTime TIMESTAMP>"
    schema:
      - name: rideId
        data-type: BIGINT
      - name: lon
        data-type: FLOAT
      - name: lat
        data-type: FLOAT
      - name: rowTime
        data-type: TIMESTAMP(3)
        rowtime:
          timestamps:
            type: "from-field"
            from: "rideTime"
          watermarks:
            type: "periodic-bounded"
            delay: "60000"
      - name: procTime
        data-type: TIMESTAMP(3)
        proctime: true
{% endhighlight %}

The resulting schema of the `TaxiRide` table contains most of the fields of the JSON schema. Furthermore, it adds a rowtime attribute `rowTime` and processing-time attribute `procTime`.

Both `connector` and `format` allow to define a property version (which is currently version `1`) for future backwards compatibility.

{% top %}

### User-defined Functions

The SQL Client allows users to create custom, user-defined functions to be used in SQL queries. Currently, these functions are restricted to be defined programmatically in Java/Scala classes or Python files.

In order to provide a Java/Scala user-defined function, you need to first implement and compile a function class that extends `ScalarFunction`, `AggregateFunction` or `TableFunction` (see [User-defined Functions]({{ site.baseurl }}/dev/table/functions/udfs.html)). One or more functions can then be packaged into a dependency JAR for the SQL Client.

In order to provide a Python user-defined function, you need to write a Python function and decorate it with the `pyflink.table.udf.udf` or `pyflink.table.udf.udtf` decorator (see [Python UDFs]({% link dev/python/user-guide/table/udfs/python_udfs.md %})). One or more functions can then be placed into a Python file. The Python file and related dependencies need to be specified via the configuration (see [Python Configuration]({% link dev/python/user-guide/table/python_config.md %})) in environment file or the command line options (see [Command Line Usage]({{ site.baseurl }}/ops/cli.html#usage)).

All functions must be declared in an environment file before being called. For each item in the list of `functions`, one must specify

- a `name` under which the function is registered,
- the source of the function using `from` (restricted to be `class` (Java/Scala UDF) or `python` (Python UDF) for now),

The Java/Scala UDF must specify:

- the `class` which indicates the fully qualified class name of the function and an optional list of `constructor` parameters for instantiation.

The Python UDF must specify:

- the `fully-qualified-name` which indicates the fully qualified name, i.e the "[module name].[object name]" of the function.

{% highlight yaml %}
functions:
  - name: java_udf               # required: name of the function
    from: class                  # required: source of the function
    class: ...                   # required: fully qualified class name of the function
    constructor:                 # optional: constructor parameters of the function class
      - ...                      # optional: a literal parameter with implicit type
      - class: ...               # optional: full class name of the parameter
        constructor:             # optional: constructor parameters of the parameter's class
          - type: ...            # optional: type of the literal parameter
            value: ...           # optional: value of the literal parameter
  - name: python_udf             # required: name of the function
    from: python                 # required: source of the function 
    fully-qualified-name: ...    # required: fully qualified class name of the function      
{% endhighlight %}

For Java/Scala UDF, make sure that the order and types of the specified parameters strictly match one of the constructors of your function class.

#### Constructor Parameters

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

Catalogs
--------

Catalogs can be defined as a set of YAML properties and are automatically registered to the environment upon starting SQL Client.

Users can specify which catalog they want to use as the current catalog in SQL CLI, and which database of the catalog they want to use as the current database.

{% highlight yaml %}
catalogs:
   - name: catalog_1
     type: hive
     property-version: 1
     default-database: mydb2
     hive-conf-dir: <path of Hive conf directory>
   - name: catalog_2
     type: hive
     property-version: 1
     hive-conf-dir: <path of Hive conf directory>

execution:
   ...
   current-catalog: catalog_1
   current-database: mydb1
{% endhighlight %}

For more information about catalogs, see [Catalogs]({{ site.baseurl }}/dev/table/catalogs.html).

Detached SQL Queries
--------------------

In order to define end-to-end SQL pipelines, SQL's `INSERT INTO` statement can be used for submitting long-running, detached queries to a Flink cluster. These queries produce their results into an external system instead of the SQL Client. This allows for dealing with higher parallelism and larger amounts of data. The CLI itself does not have any control over a detached query after submission.

{% highlight sql %}
INSERT INTO MyTableSink SELECT * FROM MyTableSource
{% endhighlight %}

The table sink `MyTableSink` has to be declared in the environment file. See the [connection page](connect.html) for more information about supported external systems and their configuration. An example for an Apache Kafka table sink is shown below.

{% highlight yaml %}
tables:
  - name: MyTableSink
    type: sink-table
    update-mode: append
    connector:
      property-version: 1
      type: kafka
      version: "0.11"
      topic: OutputTopic
      properties:
        bootstrap.servers: localhost:9092
        group.id: testGroup
    format:
      property-version: 1
      type: json
      derive-schema: true
    schema:
      - name: rideId
        data-type: BIGINT
      - name: lon
        data-type: FLOAT
      - name: lat
        data-type: FLOAT
      - name: rideTime
        data-type: TIMESTAMP(3)
{% endhighlight %}

The SQL Client makes sure that a statement is successfully submitted to the cluster. Once the query is submitted, the CLI will show information about the Flink job.

{% highlight text %}
[INFO] Table update statement has been successfully submitted to the cluster:
Cluster ID: StandaloneClusterId
Job ID: 6f922fe5cba87406ff23ae4a7bb79044
Web interface: http://localhost:8081
{% endhighlight %}

<span class="label label-danger">Attention</span> The SQL Client does not track the status of the running Flink job after submission. The CLI process can be shutdown after the submission without affecting the detached query. Flink's [restart strategy]({{ site.baseurl }}/dev/restart_strategies.html) takes care of the fault-tolerance. A query can be cancelled using Flink's web interface, command-line, or REST API.

{% top %}

SQL Views
---------

Views allow to define virtual tables from SQL queries. The view definition is parsed and validated immediately. However, the actual execution happens when the view is accessed during the submission of a general `INSERT INTO` or `SELECT` statement.

Views can either be defined in [environment files](sqlClient.html#environment-files) or within the CLI session.

The following example shows how to define multiple views in a file. The views are registered in the order in which they are defined in the environment file. Reference chains such as _view A depends on view B depends on view C_ are supported.

{% highlight yaml %}
tables:
  - name: MyTableSource
    # ...
  - name: MyRestrictedView
    type: view
    query: "SELECT MyField2 FROM MyTableSource"
  - name: MyComplexView
    type: view
    query: >
      SELECT MyField2 + 42, CAST(MyField1 AS VARCHAR)
      FROM MyTableSource
      WHERE MyField2 > 200
{% endhighlight %}

Similar to table sources and sinks, views defined in a session environment file have highest precedence.

Views can also be created within a CLI session using the `CREATE VIEW` statement:

{% highlight text %}
CREATE VIEW MyNewView AS SELECT MyField2 FROM MyTableSource;
{% endhighlight %}

Views created within a CLI session can also be removed again using the `DROP VIEW` statement:

{% highlight text %}
DROP VIEW MyNewView;
{% endhighlight %}

<span class="label label-danger">Attention</span> The definition of views in the CLI is limited to the mentioned syntax above. Defining a schema for views or escaping whitespaces in table names will be supported in future versions.

{% top %}

Temporal Tables
---------------

A [temporal table](./streaming/temporal_tables.html) allows for a (parameterized) view on a changing history table that returns the content of a table at a specific point in time. This is especially useful for joining a table with the content of another table at a particular timestamp. More information can be found in the [temporal table joins](./streaming/joins.html#join-with-a-temporal-table) page.

The following example shows how to define a temporal table `SourceTemporalTable`:

{% highlight yaml %}
tables:

  # Define the table source (or view) that contains updates to a temporal table
  - name: HistorySource
    type: source-table
    update-mode: append
    connector: # ...
    format: # ...
    schema:
      - name: integerField
        data-type: INT
      - name: stringField
        data-type: STRING
      - name: rowtimeField
        data-type: TIMESTAMP(3)
        rowtime:
          timestamps:
            type: from-field
            from: rowtimeField
          watermarks:
            type: from-source

  # Define a temporal table over the changing history table with time attribute and primary key
  - name: SourceTemporalTable
    type: temporal-table
    history-table: HistorySource
    primary-key: integerField
    time-attribute: rowtimeField  # could also be a proctime field
{% endhighlight %}

As shown in the example, definitions of table sources, views, and temporal tables can be mixed with each other. They are registered in the order in which they are defined in the environment file. For example, a temporal table can reference a view which can depend on another view or table source.

{% top %}

Limitations & Future
--------------------

The current SQL Client only supports embedded mode. In the future, the community plans to extend its functionality by providing a REST-based SQL Client Gateway, see more in [FLIP-24](https://cwiki.apache.org/confluence/display/FLINK/FLIP-24+-+SQL+Client) and [FLIP-91](https://cwiki.apache.org/confluence/display/FLINK/FLIP-91%3A+Support+SQL+Client+Gateway).

{% top %}
