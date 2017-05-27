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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public abstract class KafkaProducerTestBase extends KafkaTestBase {

	/**
	 * This tests verifies that custom partitioning works correctly, with a default topic
	 * and dynamic topic. The number of partitions for each topic is deliberately different.
	 *
	 * Test topology:
	 *
	 * <pre>
	 *             +------> (sink) --+--> [DEFAULT_TOPIC-1] --> (source) -> (map) -----+
	 *            /                  |                             |          |        |
	 *           |                   |                             |          |  ------+--> (sink)
	 *             +------> (sink) --+--> [DEFAULT_TOPIC-2] --> (source) -> (map) -----+
	 *            /                  |
	 *           |                   |
	 * (source) ----------> (sink) --+--> [DYNAMIC_TOPIC-1] --> (source) -> (map) -----+
	 *           |                   |                             |          |        |
	 *            \                  |                             |          |        |
	 *             +------> (sink) --+--> [DYNAMIC_TOPIC-2] --> (source) -> (map) -----+--> (sink)
	 *           |                   |                             |          |        |
	 *            \                  |                             |          |        |
	 *             +------> (sink) --+--> [DYNAMIC_TOPIC-3] --> (source) -> (map) -----+
	 * </pre>
	 * 
	 * Each topic has an independent mapper that validates the values come consistently from
	 * the correct Kafka partition of the topic is is responsible of.
	 * 
	 * Each topic also has a final sink that validates that there are no duplicates and that all
	 * partitions are present.
	 */
	public void runCustomPartitioningTest() {
		try {
			LOG.info("Starting KafkaProducerITCase.testCustomPartitioning()");

			final String defaultTopic = "defaultTopic";
			final int defaultTopicPartitions = 2;

			final String dynamicTopic = "dynamicTopic";
			final int dynamicTopicPartitions = 3;

			createTestTopic(defaultTopic, defaultTopicPartitions, 1);
			createTestTopic(dynamicTopic, dynamicTopicPartitions, 1);

			Map<String, Integer> expectedTopicsToNumPartitions = new HashMap<>(2);
			expectedTopicsToNumPartitions.put(defaultTopic, defaultTopicPartitions);
			expectedTopicsToNumPartitions.put(dynamicTopic, dynamicTopicPartitions);

			TypeInformation<Tuple2<Long, String>> longStringInfo = TypeInfoParser.parse("Tuple2<Long, String>");

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setRestartStrategy(RestartStrategies.noRestart());
			env.getConfig().disableSysoutLogging();

			TypeInformationSerializationSchema<Tuple2<Long, String>> serSchema =
				new TypeInformationSerializationSchema<>(longStringInfo, env.getConfig());

			TypeInformationSerializationSchema<Tuple2<Long, String>> deserSchema =
				new TypeInformationSerializationSchema<>(longStringInfo, env.getConfig());

			// ------ producing topology ---------

			// source has DOP 1 to make sure it generates no duplicates
			DataStream<Tuple2<Long, String>> stream = env.addSource(new SourceFunction<Tuple2<Long, String>>() {

				private boolean running = true;

				@Override
				public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
					long cnt = 0;
					while (running) {
						ctx.collect(new Tuple2<Long, String>(cnt, "kafka-" + cnt));
						cnt++;
					}
				}

				@Override
				public void cancel() {
					running = false;
				}
			}).setParallelism(1);

			Properties props = new Properties();
			props.putAll(FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings));
			props.putAll(secureProps);

			// sink partitions into
			kafkaServer.produceIntoKafka(stream, defaultTopic,
					// this serialization schema will route between the default topic and dynamic topic
					new CustomKeyedSerializationSchemaWrapper(serSchema, defaultTopic, dynamicTopic),
					props,
					new CustomPartitioner(expectedTopicsToNumPartitions))
				.setParallelism(Math.max(defaultTopicPartitions, dynamicTopicPartitions));

			// ------ consuming topology ---------

			Properties consumerProps = new Properties();
			consumerProps.putAll(standardProps);
			consumerProps.putAll(secureProps);

			FlinkKafkaConsumerBase<Tuple2<Long, String>> defaultTopicSource =
					kafkaServer.getConsumer(defaultTopic, deserSchema, consumerProps);
			FlinkKafkaConsumerBase<Tuple2<Long, String>> dynamicTopicSource =
					kafkaServer.getConsumer(dynamicTopic, deserSchema, consumerProps);

			env.addSource(defaultTopicSource).setParallelism(defaultTopicPartitions)
				.map(new PartitionValidatingMapper(defaultTopicPartitions)).setParallelism(defaultTopicPartitions)
				.addSink(new PartitionValidatingSink(defaultTopicPartitions)).setParallelism(1);

			env.addSource(dynamicTopicSource).setParallelism(dynamicTopicPartitions)
				.map(new PartitionValidatingMapper(dynamicTopicPartitions)).setParallelism(dynamicTopicPartitions)
				.addSink(new PartitionValidatingSink(dynamicTopicPartitions)).setParallelism(1);

			tryExecute(env, "custom partitioning test");

			deleteTestTopic(defaultTopic);
			deleteTestTopic(dynamicTopic);

			LOG.info("Finished KafkaProducerITCase.testCustomPartitioning()");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------

	public static class CustomPartitioner extends FlinkKafkaPartitioner<Tuple2<Long, String>> implements Serializable {

		private final Map<String, Integer> expectedTopicsToNumPartitions;

		public CustomPartitioner(Map<String, Integer> expectedTopicsToNumPartitions) {
			this.expectedTopicsToNumPartitions = expectedTopicsToNumPartitions;
		}

		@Override
		public int partition(Tuple2<Long, String> next, byte[] serializedKey, byte[] serializedValue, String topic, int[] partitions) {
			assertEquals(expectedTopicsToNumPartitions.get(topic).intValue(), partitions.length);

			return (int) (next.f0 % partitions.length);
		}
	}

	/**
	 * A {@link KeyedSerializationSchemaWrapper} that supports routing serialized records to different target topics.
	 */
	public static class CustomKeyedSerializationSchemaWrapper extends KeyedSerializationSchemaWrapper<Tuple2<Long, String>> {

		private final String defaultTopic;
		private final String dynamicTopic;

		public CustomKeyedSerializationSchemaWrapper(
				SerializationSchema<Tuple2<Long, String>> serializationSchema,
				String defaultTopic,
				String dynamicTopic) {

			super(serializationSchema);

			this.defaultTopic = Preconditions.checkNotNull(defaultTopic);
			this.dynamicTopic = Preconditions.checkNotNull(dynamicTopic);
		}

		@Override
		public String getTargetTopic(Tuple2<Long, String> element) {
			return (element.f0 % 2 == 0) ? defaultTopic : dynamicTopic;
		}
	}

	/**
	 * Mapper that validates partitioning and maps to partition.
	 */
	public static class PartitionValidatingMapper extends RichMapFunction<Tuple2<Long, String>, Integer> {

		private final int numPartitions;

		private int ourPartition = -1;

		public PartitionValidatingMapper(int numPartitions) {
			this.numPartitions = numPartitions;
		}

		@Override
		public Integer map(Tuple2<Long, String> value) throws Exception {
			int partition = value.f0.intValue() % numPartitions;
			if (ourPartition != -1) {
				assertEquals("inconsistent partitioning", ourPartition, partition);
			} else {
				ourPartition = partition;
			}
			return partition;
		}
	}

	/**
	 * Sink that validates records received from each partition and checks that there are no duplicates.
	 */
	public static class PartitionValidatingSink implements SinkFunction<Integer> {
		private final int[] valuesPerPartition;

		public PartitionValidatingSink(int numPartitions) {
			this.valuesPerPartition = new int[numPartitions];
		}

		@Override
		public void invoke(Integer value) throws Exception {
			valuesPerPartition[value]++;

			boolean missing = false;
			for (int i : valuesPerPartition) {
				if (i < 100) {
					missing = true;
					break;
				}
			}
			if (!missing) {
				throw new SuccessException();
			}
		}
	}
}
