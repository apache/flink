/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.cassandra.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.ArrayList;

/**
 * This is an example showing the to use the Tuple Cassandra Sink in the Streaming API.
 *
 * The example assumes that a table exists in a local cassandra database, according to the following query:
 * CREATE TABLE IF NOT EXISTS test.writetuple(element1 text PRIMARY KEY, element2 int)
 */
public class CassandraTupleSinkExample {
	private static final String INSERT = "INSERT INTO test.writetuple (element1, element2) VALUES (?, ?)";
	private static final ArrayList<Tuple2<String, Integer>> collection = new ArrayList<>(20);

	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<>("cassandra-" + i, i));
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(collection);

		CassandraSink.addSink(source)
			.setQuery(INSERT)
			.setClusterBuilder(new ClusterBuilder() {
				@Override
				protected Cluster buildCluster(Builder builder) {
					return builder.addContactPoint("127.0.0.1").build();
				}
			})
			.build();

		env.execute("WriteTupleIntoCassandra");
	}
}
