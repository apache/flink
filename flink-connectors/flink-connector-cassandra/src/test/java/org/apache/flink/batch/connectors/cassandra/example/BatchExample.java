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

package org.apache.flink.batch.connectors.cassandra.example;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

import java.util.ArrayList;

/**
 * This is an example showing the to use the Cassandra Input-/OutputFormats in the Batch API.
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the following queries:
 * CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': ‘1’};
 * CREATE TABLE IF NOT EXISTS test.batches (number int, strings text, PRIMARY KEY(number, strings));
 */
public class BatchExample {
	private static final String INSERT_QUERY = "INSERT INTO test.batches (number, strings) VALUES (?,?);";
	private static final String SELECT_QUERY = "SELECT number, strings FROM test.batches;";

	/*
	 *	table script: "CREATE TABLE test.batches (number int, strings text, PRIMARY KEY(number, strings));"
	 */
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		ArrayList<Tuple2<Integer, String>> collection = new ArrayList<>(20);
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<>(i, "string " + i));
		}

		DataSet<Tuple2<Integer, String>> dataSet = env.fromCollection(collection);

		dataSet.output(new CassandraTupleOutputFormat<Tuple2<Integer, String>>(INSERT_QUERY, new ClusterBuilder() {
			@Override
			protected Cluster buildCluster(Builder builder) {
				return builder.addContactPoints("127.0.0.1").build();
			}
		}));

		env.execute("Write");

		DataSet<Tuple2<Integer, String>> inputDS = env
			.createInput(new CassandraInputFormat<Tuple2<Integer, String>>(SELECT_QUERY, new ClusterBuilder() {
				@Override
				protected Cluster buildCluster(Builder builder) {
					return builder.addContactPoints("127.0.0.1").build();
				}
			}), TupleTypeInfo.of(new TypeHint<Tuple2<Integer, String>>() {
			}));

		inputDS.print();
	}
}
