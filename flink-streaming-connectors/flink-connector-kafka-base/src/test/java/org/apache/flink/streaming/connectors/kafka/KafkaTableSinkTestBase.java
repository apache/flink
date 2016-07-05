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
package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;
import org.apache.flink.test.util.SuccessException;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;

import static org.apache.flink.test.util.TestUtils.tryExecute;

public abstract class KafkaTableSinkTestBase extends KafkaTestBase implements Serializable {

	protected final static String TOPIC = "customPartitioningTestTopic";
	protected final static int PARALLELISM = 1;
	protected final static String[] FIELD_NAMES = new String[] {"field1", "field2"};
	protected final static Class[] FIELD_TYPES = new Class[] {Integer.class, String.class};

	public void testKafkaTableSink() throws Exception {
		LOG.info("Starting KafkaTableSinkTestBase.testKafkaTableSink()");

		createTestTopic(TOPIC, PARALLELISM, 1);
		StreamExecutionEnvironment env = createEnvironment();

		createProducingTopology(env);
		createConsumingTopology(env);

		tryExecute(env, "custom partitioning test");
		deleteTestTopic(TOPIC);
		LOG.info("Finished KafkaTableSinkTestBase.testKafkaTableSink()");
	}

	private StreamExecutionEnvironment createEnvironment() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();
		return env;
	}

	private void createConsumingTopology(StreamExecutionEnvironment env) {
		JsonRowDeserializationSchema jsonDeserializationSchema = new JsonRowDeserializationSchema(
			FIELD_NAMES, FIELD_TYPES);

		FlinkKafkaConsumerBase<Row> source = kafkaServer.getConsumer(TOPIC, jsonDeserializationSchema, standardProps);

		env.addSource(source).setParallelism(PARALLELISM)
				.map(new RichMapFunction<Row, Integer>() {
					@Override
					public Integer map(Row value) {
						return (Integer) value.getField(0);
					}
				}).setParallelism(PARALLELISM)

				.addSink(new SinkFunction<Integer>() {
					HashSet<Integer> ids = new HashSet<>();
					@Override
					public void invoke(Integer value) throws Exception {
						ids.add(value);

						if (ids.size() == 100) {
							throw new SuccessException();
						}
					}
				}).setParallelism(1);
	}

	private void createProducingTopology(StreamExecutionEnvironment env) {
		DataStream<Row> stream = env.addSource(new SourceFunction<Row>() {

			private boolean running = true;

			@Override
			public void run(SourceContext<Row> ctx) throws Exception {
				long cnt = 0;
				while (running) {
					Row row = new Row(2);
					row.setField(0, cnt);
					row.setField(1, "kafka-" + cnt);
					ctx.collect(row);
					cnt++;
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		})
		.setParallelism(1);

		KafkaTableSink kafkaTableSinkBase = createTableSink();

		kafkaTableSinkBase.emitDataStream(stream);
	}

	protected KafkaPartitioner<Row> createPartitioner() {
		return new CustomPartitioner();
	}

	protected Properties createSinkProperties() {
		return FlinkKafkaProducerBase.getPropertiesFromBrokerList(KafkaTestBase.brokerConnectionStrings);
	}

	protected abstract KafkaTableSink createTableSink();

	public static class CustomPartitioner extends KafkaPartitioner<Row> implements Serializable {
		@Override
		public int partition(Row next, byte[] serializedKey, byte[] serializedValue, int numPartitions) {
			return 0;
		}
	}
}
