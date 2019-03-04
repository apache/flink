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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.mapping.Mapper;

import java.util.ArrayList;

/**
 * This is an example showing the to use the Pojo Cassandra Sink in the Streaming API.
 *
 * <p>Pojo's have to be annotated with datastax annotations to work with this sink.
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the following queries:
 * CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 * CREATE TABLE IF NOT EXISTS test.message(body txt PRIMARY KEY)
 */
public class CassandraPojoSinkExample {
	private static final ArrayList<Message> messages = new ArrayList<>(20);

	static {
		for (long i = 0; i < 20; i++) {
			messages.add(new Message("cassandra-" + i));
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Message> source = env.fromCollection(messages);

		CassandraSink.addSink(source)
			.setClusterBuilder(new ClusterBuilder() {
				@Override
				protected Cluster buildCluster(Builder builder) {
					return builder.addContactPoint("127.0.0.1").build();
				}
			})
			.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
			.build();

		env.execute("Cassandra Sink example");
	}
}
