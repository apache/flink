---
title: "Apache Cassandra Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 1
sub-nav-title: Cassandra
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

This connector provides sinks that writes data into a [Cassandra](https://cassandra.apache.org/) database.

To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-cassandra{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See how to link with them for cluster execution [here]({{ site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Apache Cassandra
Follow the instructions from the [Cassandra Getting Started page](http://wiki.apache.org/cassandra/GettingStarted).

#### Cassandra Sink

Flink's Cassandra sink are created by using the static CassandraSink.addSink(DataStream<IN> input) method.
This method returns a CassandraSinkBuilder, which offers methods to further configure the sink.

The following configuration methods can be used:

1. setQuery(String query)
2. setHost(String host[, int port])
3. setClusterBuilder(ClusterBuilder builder)
4. enableWriteAheadLog([CheckpointCommitter committer])
5. build()

*setQuery()* sets the query that is executed for every value the sink receives.
*setHost()* sets the cassandra host/port to connect to. This method is intended for simple use-cases.
*setClusterBuilder()* sets the cluster builder that is used to configure the connection to cassandra. The *setHost()* functionality can be subsumed with this method.
*enableWriteAheadLog()* is an optional method, that allows exactly-once processing for non-deterministic algorithms.

A checkpoint committer stores additional information about completed checkpoints
in some resource. This information is used to prevent a full replay of the last
completed checkpoint in case of a failure.
You can use a `CassandraCommitter` to store these in a separate table in cassandra.
Note that this table will NOT be cleaned up by Flink.

*build()* finalizes the configuration and returns the CassandraSink.

Flink can provide exactly-once guarantees if the query is idempotent (meaning it can be applied multiple
times without changing the result) and checkpointing is enabled. In case of a failure the failed
checkpoint will be replayed completely.

Furthermore, for non-deterministic programs the write-ahead log has to be enabled. For such a program
the replayed checkpoint may be completely different than the previous attempt, which may leave the
database in an inconsitent state since part of the first attempt may already be written.
The write-ahead log guarantees that the replayed checkpoint is identical to the first attempt. 
Note that that enabling this feature will have an adverse impact on latency.

<p style="border-radius: 5px; padding: 5px" class="bg-danger"><b>Note</b>: The write-ahead log functionality is currently experimental. In many cases it is sufficent to use the connector without enabling it. Please report problems to the development mailing list.</p>


#### Example

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
CassandraSink.addSink(input)
  .setQuery("INSERT INTO example.values (id, counter) values (?, ?);")
  .setClusterBuilder(new ClusterBuilder() {
    @Override
    public Cluster buildCluster(Cluster.Builder builder) {
      return builder.addContactPoint("127.0.0.1").build();
    }
  })
  .build();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
CassandraSink.addSink(input)
  .setQuery("INSERT INTO example.values (id, counter) values (?, ?);")
  .setClusterBuilder(new ClusterBuilder() {
    @Override
    public Cluster buildCluster(Cluster.Builder builder) {
      return builder.addContactPoint("127.0.0.1").build();
    }
  })
  .build();
{% endhighlight %}
</div>
</div>

The Cassandra sinks support both tuples and POJO's that use DataStax annotations.
Flink automatically detects which type of input is used.

Example for such a Pojo:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

@Table(keyspace= "test", name = "mappersink")
public class Pojo implements Serializable {

	private static final long serialVersionUID = 1038054554690916991L;

	@Column(name = "id")
	private long id;
	@Column(name = "value")
	private String value;

	public Pojo(long id, String value){
		this.id = id;
		this.value = value;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
{% endhighlight %}
</div>
</div>
