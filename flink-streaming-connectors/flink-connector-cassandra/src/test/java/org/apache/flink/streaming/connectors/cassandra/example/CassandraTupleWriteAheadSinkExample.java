/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.cassandra.example;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.UUID;

/**
 * This is an example showing the to use the Cassandra Sink (with write-ahead log) in the Streaming API.
 *
 * The example assumes that a table exists in a local cassandra database, according to the following query:
 * CREATE TABLE example.values (id text, count int, PRIMARY KEY(id));
 * 
 * Important things to note are that checkpointing is enabled, a StateBackend is set and the enableWriteAheadLog() call
 * when creating the CassandraSink.
 */
public class CassandraTupleWriteAheadSinkExample {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000));
		env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("java.io.tmpdir") + "/flink/backend"));

		CassandraSink<Tuple2<String, Integer>> sink = CassandraSink.addSink(env.addSource(new MySource()))
			.setQuery("INSERT INTO example.values (id, counter) values (?, ?);")
			.enableWriteAheadLog()
			.setClusterBuilder(new ClusterBuilder() {
				@Override
				public Cluster buildCluster(Cluster.Builder builder) {
					return builder.addContactPoint("127.0.0.1").build();
				}
			})
			.build();

		sink.name("Cassandra Sink").disableChaining().setParallelism(1).uid("hello");

		env.execute();
	}

	public static class MySource implements SourceFunction<Tuple2<String, Integer>>, Checkpointed<Integer> {
		private int counter = 0;
		private boolean stop = false;

		@Override
		public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
			while (!stop) {
				Thread.sleep(50);
				ctx.collect(new Tuple2<>("" + UUID.randomUUID(), 1));
				counter++;
				if (counter == 100) {
					stop = true;
				}
			}
		}

		@Override
		public void cancel() {
			stop = true;
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return counter;
		}

		@Override
		public void restoreState(Integer state) throws Exception {
			this.counter = state;
		}
	}
}
