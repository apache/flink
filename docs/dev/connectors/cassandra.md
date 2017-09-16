---
title: "Apache Cassandra Connector"
nav-title: Cassandra
nav-parent_id: connectors
nav-pos: 2
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

* This will be replaced by the TOC
{:toc}


This connector provides sinks that writes data into a [Apache Cassandra](https://cassandra.apache.org/) database.

<!--
  TODO: Perhaps worth mentioning current DataStax Java Driver version to match Cassandra versoin on user side.
-->

To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-cassandra{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently __NOT__ part of the binary distribution. See how to link with them for cluster execution [here]({{ site.baseurl}}/dev/linking.html).

## Installing Apache Cassandra
There are multiple ways to bring up a Cassandra instance on local machine:

1. Follow the instructions from [Cassandra Getting Started page](http://cassandra.apache.org/doc/latest/getting_started/index.html).
2. Launch a container running Cassandra from [Official Docker Repository](https://hub.docker.com/_/cassandra/)

## Cassandra Sink
Flink Cassandra connector currently supports Apache Cassandra as a data sink.

### Configurations

Flink's Cassandra sink are created by using the static CassandraSink.addSink(DataStream<IN> input) method.
This method returns a CassandraSinkBuilder, which offers methods to further configure the sink, and finally `build()` the sink instance.

The following configuration methods can be used:

1. _setQuery(String query)_
    * sets the upsert query that is executed for every record the sink receives.
    * internally treated as CQL prepared statement, in which parameters could be shared or anonymous.
    * __DO__ set the upsert query for processing __Java Tuple__ data type
    * __DO NOT__ set the query for processing __POJO__ data type.
2. _setClusterBuilder()_
    * sets the cluster builder that is used to configure the connection to cassandra with more sophisticated settings such as consistency level, retry policy and etc.
3. _setHost(String host[, int port])_
    * simple version of setClusterBuilder() with host/port information to connect to Cassandra instances
4. _enableWriteAheadLog([CheckpointCommitter committer])_
    * an __optional__ setting
    * allows exactly-once processing for non-deterministic algorithms.
5. _build()_
    * finalizes the configuration and constructs the CassandraSink instance.

### Write-ahead Log

A checkpoint committer stores additional information about completed checkpoints
in some resource. This information is used to prevent a full replay of the last
completed checkpoint in case of a failure.
You can use a `CassandraCommitter` to store these in a separate table in cassandra.
Note that this table will NOT be cleaned up by Flink.

Flink can provide exactly-once guarantees if the query is idempotent (meaning it can be applied multiple
times without changing the result) and checkpointing is enabled. In case of a failure the failed
checkpoint will be replayed completely.

Furthermore, for non-deterministic programs the write-ahead log has to be enabled. For such a program
the replayed checkpoint may be completely different than the previous attempt, which may leave the
database in an inconsitent state since part of the first attempt may already be written.
The write-ahead log guarantees that the replayed checkpoint is identical to the first attempt.
Note that that enabling this feature will have an adverse impact on latency.

<p style="border-radius: 5px; padding: 5px" class="bg-danger"><b>Note</b>: The write-ahead log functionality is currently experimental. In many cases it is sufficent to use the connector without enabling it. Please report problems to the development mailing list.</p>

### Checkpointing and Fault Tolerance
With checkpointing enabled, Cassandra Sink guarantees at-least-once delivery of action requests to C* instance.

<p style="border-radius: 5px; padding: 5px" class="bg-danger"><b>Note</b>:However, current Cassandra Sink implementation does not flush the pending mutations  before the checkpoint was triggered. Thus, some in-flight mutations might not be replayed when the job recovered. </p>

More details on [checkpoints docs]({{ site.baseurl }}/dev/stream/state/checkpointing.html) and [fault tolerance guarantee docs]({{ site.baseurl }}/dev/connectors/guarantees.html)

To enable fault tolerant guarantee, checkpointing of the topology needs to be enabled at the execution environment:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // checkpoint every 5000 msecs
{% endhighlight %}
</div>
</div>

## Examples

The Cassandra sinks currently support both Java Tuple and POJO data types, and Flink automatically detects which type of input is used. For general use case of those streaming data type, please refer to [Supported Data Types]({{ site.baseurl }}/dev/api_concepts.html). We show two implementations based on [SocketWindowWordCount](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java), for Pojo and Java Tuple data types respectively.

In all these examples, we assumed the associated Keyspace `example` and Table `wordcount` have been created.

<div class="codetabs" markdown="1">
<div data-lang="CQL" markdown="1">
{% highlight sql %}
CREATE KEYSPACE IF NOT EXISTS example
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE IF NOT EXISTS example.wordcount (
    word text,
    count bigint,
    PRIMARY KEY(word)
    );
{% endhighlight %}
</div>
</div>

### Cassandra Sink Example for Streaming Java Tuple Data Type
While storing the result with Java Tuple data type to a Cassandra sink, it is required to set a CQL upsert statement (via setQuery('stmt')) to persist each record back to database. With the upsert query cached as `PreparedStatement`, Cassandra connector internally converts each Tuple elements as parameters to the statement.

For details about `PreparedStatement` and `BoundStatement`, please visit [DataStax Java Driver manual](https://docs.datastax.com/en/developer/java-driver/2.1/manual/statements/prepared/)

Please note that if the upsert query were not set, an `IllegalArgumentException` would be thrown with the following error message `Query must not be null or empty.`

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get the execution environment
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data by connecting to the socket
DataStream<String> text = env.socketTextStream(hostname, port, "\n");

// parse the data, group it, window it, and aggregate the counts
DataStream<Tuple2<String, Long>> result = text

        .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                // normalize and split the line
                String[] words = value.toLowerCase().split("\\W+");

                // emit the pairs
                for (String word : words) {
                    //Do not accept empty word, since word is defined as primary key in C* table
                    if (!word.isEmpty()) {
                        out.collect(new Tuple2<String, Long>(word, 1L));
                    }
                }
            }
        })

        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .sum(1)
        ;

CassandraSink.addSink(result)
        .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
        .setHost("127.0.0.1")
        .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
CassandraSink.addSink(input.javaStream)
    .setQuery("INSERT INTO test.wordcount(word, count) values (?, ?);")
    .setClusterBuilder(new ClusterBuilder() {
        @Override
        def buildCluster(builder: Cluster.Builder): Cluster = {
            builder.addContactPoint("127.0.0.1").build()
        }
    })
    .build()
{% endhighlight %}
</div>
</div>


### Cassandra Sink Example for Streaming POJO Data Type
An example of streaming a POJO data type and store the same POJO entity back to Cassandra. In addition, this POJO implementation needs to follow [DataStax Java Driver Manual](http://docs.datastax.com/en/developer/java-driver/2.1/manual/object_mapper/creating/) to annotate the class as Cassandra connector internally maps each field of this entity to an associated column of the desginated Table using `com.datastax.driver.mapping.Mapper` class of DataStax Java Driver.

Please note that if the upsert query was set, an `IllegalArgumentException` would be thrown with the following error message `Specifying a query is not allowed when using a Pojo-Stream as input.`

For each CQL defined data type for columns, please refer to [CQL Documentation](https://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html)

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get the execution environment
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data by connecting to the socket
DataStream<String> text = env.socketTextStream(hostname, port, "\n");

// parse the data, group it, window it, and aggregate the counts
DataStream<WordCount> result = text

        .flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) {
                for (String word : value.split("\\s")) {
                    if (!word.isEmpty()) {
                        //Do not accept empty word, since word is defined as primary key in C* table
                        out.collect(new WordCount(word, 1L));
                    }
                }
            }
        })

        .keyBy("word")
        .timeWindow(Time.seconds(5))

        .reduce(new ReduceFunction<WordCount>() {
            @Override
            public WordCount reduce(WordCount a, WordCount b) {
                return new WordCount(a.getWord(), a.getCount() + b.getCount());
            }
        });

CassandraSink.addSink(result)
        .setHost("127.0.0.1")
        .build();


@Table(keyspace = "example", name = "wordcount")
public class WordCount {

    @Column(name = "word")
    private String word = "";

    @Column(name = "count")
    private long count = 0;

    public WordCount() {}

    public WordCount(String word, long count) {
        this.setWord(word);
        this.setCount(count);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return getWord() + " : " + getCount();
    }
}
{% endhighlight %}
</div>
</div>
