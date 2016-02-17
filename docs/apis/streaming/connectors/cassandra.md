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

This connector provides a sink that writes data into a [Cassandra](https://cassandra.apache.org/) database.

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

There are two types of CassandraSink:
* CassandraSink works with Flink Tuples
* CassandraMapperSink works with datastax annotations

##### CassandraSink
The constructor accepts the following arguments:

1. query to create a new table to write into (optional) - mind attention: The creation works only with parallelism of one due to [CASSANDRA-8387](https://issues.apache.org/jira/browse/CASSANDRA-8387)
2. query to insert data the a table

You have to implement configureCluster to provide at the Sink the information to connect to Apache Cassandra.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
source.addSink(new CassandraSink<Tuple2<Long, String>>(CREATE_TABLE, INSERT_QUERY) {
			@Override
			public Builder configureCluster(Builder cluster) {
				return cluster.addContactPoint("127.0.0.1");
			}
		});
{% endhighlight %}
</div>

##### CassandraMapperSink
The constructor accepts the following arguments:

1. query to create a new table to write into (optional) - mind attention: The creation works only with parallelism of one due to [CASSANDRA-8387](https://issues.apache.org/jira/browse/CASSANDRA-8387)
2. Pojo's Class

You have to implement configureCluster to provide at the Sink the information to connect to Apache Cassandra.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// Pojo.java

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

// FlinkJob

source.addSink(new CassandraMapperSink<Pojo>(CREATE_TABLE_MAPPER,Pojo.class) {

           @Override
           public Builder configureCluster(Builder cluster) {
             return cluster.addContactPoint("127.0.0.1");
           }
       });
{% endhighlight %}
</div>
