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

package org.apache.flink.connectors.cassandra.streaming.examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.cassandra.streaming.CassandraSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster.Builder;

import java.util.ArrayList;

public class CassandraSinkExample {
	
	private static final Logger LOG = LoggerFactory.getLogger(CassandraSinkExample.class);

	private static final String INSERT = "INSERT INTO test.writetuple (element1, element2) VALUES (?, ?)";
	private static final ArrayList<Tuple2<String,Integer>> collection = new ArrayList<>(20);
	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<>("cassandra-" + i,i));
		}
	}

	/*
	 * table script: "CREATE TABLE IF NOT EXISTS test.writetuple(element1 text PRIMARY KEY, element2 int)"
	 */
	public static void main(String[] args) throws Exception {
	
		LOG.debug("WriteTupleIntoCassandra");
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Tuple2<String,Integer>> source = env.fromCollection(collection);
		
		source.addSink(new CassandraSink<Tuple2<String,Integer>>(INSERT) {

			@Override
			public Builder configureCluster(Builder cluster) {
				return cluster.addContactPoint("127.0.0.1");
			}
		});
		

		env.execute("WriteTupleIntoCassandra");
	}
}
