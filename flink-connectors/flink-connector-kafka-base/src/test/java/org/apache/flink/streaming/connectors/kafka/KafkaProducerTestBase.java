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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Preconditions;

import com.google.common.collect.ImmutableSet;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Abstract test base for all Kafka producer tests.
 */
@SuppressWarnings("serial")
public abstract class KafkaProducerTestBase extends KafkaTestBase {

	/**
	 * This tests verifies that custom partitioning works correctly, with a default topic
	 * and dynamic topic. The number of partitions for each topic is deliberately different.
	 *
	 * <p>Test topology:
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
	 * <p>Each topic has an independent mapper that validates the values come consistently from
	 * the correct Kafka partition of the topic is is responsible of.
	 *
	 * <p>Each topic also has a final sink that validates that there are no duplicates and that all
	 * partitions are present.
	 */
	@Test
	public void testCustomPartitioning() {
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
			kafkaServer.produceIntoKafka(
					stream,
					defaultTopic,
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

	/**
	 * Tests the at-least-once semantic for the simple writes into Kafka.
	 */
	@Test
	public void testOneToOneAtLeastOnceRegularSink() throws Exception {
		testOneToOneAtLeastOnce(true);
	}

	/**
	 * Tests the at-least-once semantic for the simple writes into Kafka.
	 */
	@Test
	public void testOneToOneAtLeastOnceCustomOperator() throws Exception {
		testOneToOneAtLeastOnce(false);
	}

	/**
	 * This test sets KafkaProducer so that it will not automatically flush the data and
	 * and fails the broker to check whether FlinkKafkaProducer flushed records manually on snapshotState.
	 */
	protected void testOneToOneAtLeastOnce(boolean regularSink) throws Exception {
		final String topic = regularSink ? "oneToOneTopicRegularSink" : "oneToOneTopicCustomOperator";
		final int partition = 0;
		final int numElements = 1000;
		final int failAfterElements = 333;

		createTestTopic(topic, 1, 1);

		TypeInformationSerializationSchema<Integer> schema = new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
		KeyedSerializationSchema<Integer> keyedSerializationSchema = new KeyedSerializationSchemaWrapper(schema);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		// decrease timeout and block time from 60s down to 10s - this is how long KafkaProducer will try send pending (not flushed) data on close()
		properties.setProperty("timeout.ms", "10000");
		properties.setProperty("max.block.ms", "10000");
		// increase batch.size and linger.ms - this tells KafkaProducer to batch produced events instead of flushing them immediately
		properties.setProperty("batch.size", "10240000");
		properties.setProperty("linger.ms", "10000");

		int leaderId = kafkaServer.getLeaderToShutDown(topic);
		BrokerRestartingMapper.resetState();

		// process exactly failAfterElements number of elements and then shutdown Kafka broker and fail application
		DataStream<Integer> inputStream = env
			.fromCollection(getIntegersSequence(numElements))
			.map(new BrokerRestartingMapper<Integer>(leaderId, failAfterElements));

		StreamSink<Integer> kafkaSink = kafkaServer.getProducerSink(topic, keyedSerializationSchema, properties, new FlinkKafkaPartitioner<Integer>() {
			@Override
			public int partition(Integer record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
				return partition;
			}
		});

		if (regularSink) {
			inputStream.addSink(kafkaSink.getUserFunction());
		}
		else {
			kafkaServer.produceIntoKafka(inputStream, topic, keyedSerializationSchema, properties, new FlinkKafkaPartitioner<Integer>() {
				@Override
				public int partition(Integer record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
					return partition;
				}
			});
		}

		FailingIdentityMapper.failedBefore = false;
		try {
			env.execute("One-to-one at least once test");
			fail("Job should fail!");
		}
		catch (JobExecutionException ex) {
			assertEquals("Broker was shutdown!", ex.getCause().getMessage());
		}

		kafkaServer.restartBroker(leaderId);

		// assert that before failure we successfully snapshot/flushed all expected elements
		assertAtLeastOnceForTopic(
				properties,
				topic,
				partition,
				ImmutableSet.copyOf(getIntegersSequence(BrokerRestartingMapper.numElementsBeforeSnapshot)),
				30000L);

		deleteTestTopic(topic);
	}

	/**
	 * We manually handle the timeout instead of using JUnit's timeout to return failure instead of timeout error.
	 * After timeout we assume that there are missing records and there is a bug, not that the test has run out of time.
	 */
	private void assertAtLeastOnceForTopic(
			Properties properties,
			String topic,
			int partition,
			Set<Integer> expectedElements,
			long timeoutMillis) throws Exception {

		long startMillis = System.currentTimeMillis();
		Set<Integer> actualElements = new HashSet<>();

		// until we timeout...
		while (System.currentTimeMillis() < startMillis + timeoutMillis) {
			properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
			properties.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

			// query kafka for new records ...
			Collection<ConsumerRecord<Integer, Integer>> records = kafkaServer.getAllRecordsFromTopic(properties, topic, partition, 100);

			for (ConsumerRecord<Integer, Integer> record : records) {
				actualElements.add(record.value());
			}

			// succeed if we got all expectedElements
			if (actualElements.containsAll(expectedElements)) {
				return;
			}
		}

		fail(String.format("Expected to contain all of: <%s>, but was: <%s>", expectedElements, actualElements));
	}

	private List<Integer> getIntegersSequence(int size) {
		List<Integer> result = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			result.add(i);
		}
		return result;
	}

	// ------------------------------------------------------------------------

	private static class CustomPartitioner extends FlinkKafkaPartitioner<Tuple2<Long, String>> implements Serializable {

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

	private static class BrokerRestartingMapper<T> extends RichMapFunction<T, T>
		implements CheckpointedFunction, CheckpointListener {

		private static final long serialVersionUID = 6334389850158707313L;

		public static volatile boolean restartedLeaderBefore;
		public static volatile boolean hasBeenCheckpointedBeforeFailure;
		public static volatile int numElementsBeforeSnapshot;

		private final int shutdownBrokerId;
		private final int failCount;
		private int numElementsTotal;

		private boolean failer;
		private boolean hasBeenCheckpointed;

		public static void resetState() {
			restartedLeaderBefore = false;
			hasBeenCheckpointedBeforeFailure = false;
			numElementsBeforeSnapshot = 0;
		}

		public BrokerRestartingMapper(int shutdownBrokerId, int failCount) {
			this.shutdownBrokerId = shutdownBrokerId;
			this.failCount = failCount;
		}

		@Override
		public void open(Configuration parameters) {
			failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
		}

		@Override
		public T map(T value) throws Exception {
			numElementsTotal++;

			if (!restartedLeaderBefore) {
				Thread.sleep(10);

				if (failer && numElementsTotal >= failCount) {
					// shut down a Kafka broker
					KafkaServer toShutDown = null;
					for (KafkaServer server : kafkaServer.getBrokers()) {

						if (kafkaServer.getBrokerId(server) == shutdownBrokerId) {
							toShutDown = server;
							break;
						}
					}

					if (toShutDown == null) {
						StringBuilder listOfBrokers = new StringBuilder();
						for (KafkaServer server : kafkaServer.getBrokers()) {
							listOfBrokers.append(kafkaServer.getBrokerId(server));
							listOfBrokers.append(" ; ");
						}

						throw new Exception("Cannot find broker to shut down: " + shutdownBrokerId
												+ " ; available brokers: " + listOfBrokers.toString());
					} else {
						hasBeenCheckpointedBeforeFailure = hasBeenCheckpointed;
						restartedLeaderBefore = true;
						toShutDown.shutdown();
						toShutDown.awaitShutdown();
						throw new Exception("Broker was shutdown!");
					}
				}
			}
			return value;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			hasBeenCheckpointed = true;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			numElementsBeforeSnapshot = numElementsTotal;
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
		}
	}
}
