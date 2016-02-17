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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.connectors.cassandra.streaming.CassandraMapperSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster.Builder;

public class WriteCassandraMapperSink {

	private static final Logger LOG = LoggerFactory.getLogger(WriteCassandraMapperSink.class);

	private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS test.message(body txt PRIMARY KEY);";

	public static void main(String[] args) throws Exception {

		LOG.debug("WritePojoIntoCassandra");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Message> source = env
				.addSource(new SourceFunction<Message>() {
					private static final long serialVersionUID = 1L;

					private volatile boolean running = true;

					@Override
					public void run(SourceContext<Message> ctx)
							throws Exception {
						for (int i = 0; i < 20; i++) {
							Message msg = new Message("message #" + i);
							ctx.collect(msg);
						}
						cancel();
					}

					@Override
					public void cancel() {
						running = false;
					}
				});

		source.addSink(new CassandraMapperSink<Message>(CREATE_TABLE, Message.class){

			@Override
			public Builder configureCluster(Builder cluster) {
				return cluster.addContactPoint("127.0.0.1");
			}
		});

		env.execute("Cassandra Sink example");
	}
}