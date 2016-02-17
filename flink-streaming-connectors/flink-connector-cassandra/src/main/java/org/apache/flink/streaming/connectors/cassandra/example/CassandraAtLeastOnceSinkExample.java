/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.cassandra.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.cassandra.CassandraAtLeastOnceSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraCommitter;

import java.util.UUID;

public class CassandraAtLeastOnceSinkExample {
	public static void main(String[] args) throws Exception {

		class MySource implements SourceFunction<Tuple2<String, Integer>>, Checkpointed<Integer> {
			private int counter = 0;
			private boolean stop = false;

			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
				while (!stop) {
					Thread.sleep(50);
					ctx.collect(new Tuple2<>("" + UUID.randomUUID(), counter));
					counter++;
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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000);
		env.setNumberOfExecutionRetries(1);
		env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("java.io.tmpdir") + "/flink/backend"));

		DataStream<Tuple2<String, Integer>> input = env.addSource(new MySource());
		input.transform(
			"Cassandra Sink",
			null,
			new CassandraAtLeastOnceSink<>(
				"127.0.0.1",
				"CREATE TABLE example.values (id text PRIMARY KEY, counter int);",
				"INSERT INTO example.values (id, counter) VALUES (?, ?);",
				new CassandraCommitter("127.0.0.1", "example", "checkpoints"),
				input.getType().createSerializer(env.getConfig())));

		env.execute();
	}
}
