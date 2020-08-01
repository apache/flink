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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.IntegerSource;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Abstract test base for all Kafka producer tests.
 */
@SuppressWarnings("serial")
public abstract class KafkaProducerTestBase extends KafkaTestBaseWithFlink {

	private static final long KAFKA_READ_TIMEOUT = 60_000L;

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

			TypeInformation<Tuple2<Long, String>> longStringInfo = TypeInformation.of(new TypeHint<Tuple2<Long, String>>(){});

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setRestartStrategy(RestartStrategies.noRestart());

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
						if (cnt % 100 == 0) {
							Thread.sleep(1);
						}
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
	 * simulate network failure between Flink and Kafka to check whether FlinkKafkaProducer
	 * flushed records manually on snapshotState.
	 *
	 * <p>Due to legacy reasons there are two different ways of instantiating a Kafka 0.10 sink. The
	 * parameter controls which method is used.
	 */
	protected void testOneToOneAtLeastOnce(boolean regularSink) throws Exception {
		final String topic = regularSink ? "oneToOneTopicRegularSink" : "oneToOneTopicCustomOperator";
		final int partition = 0;
		final int numElements = 1000;
		final int failAfterElements = 333;

		createTestTopic(topic, 1, 1);

		TypeInformationSerializationSchema<Integer> schema = new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());

		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		// decrease timeout and block time from 60s down to 10s - this is how long KafkaProducer will try send pending (not flushed) data on close()
		properties.setProperty("timeout.ms", "10000");
		// KafkaProducer prior to KIP-91 (release 2.1) uses request timeout to expire the unsent records.
		properties.setProperty("request.timeout.ms", "3000");
		// KafkaProducer in 2.1.0 and above uses delivery timeout to expire the the records.
		properties.setProperty("delivery.timeout.ms", "5000");
		properties.setProperty("max.block.ms", "10000");
		// increase batch.size and linger.ms - this tells KafkaProducer to batch produced events instead of flushing them immediately
		properties.setProperty("batch.size", "10240000");
		properties.setProperty("linger.ms", "10000");
		// kafka producer messages guarantee
		properties.setProperty("retries", "3");
		properties.setProperty("acks", "all");

		BrokerRestartingMapper.resetState(kafkaServer::blockProxyTraffic);

		// process exactly failAfterElements number of elements and then shutdown Kafka broker and fail application
		DataStream<Integer> inputStream = env
			.addSource(new InfiniteIntegerSource())
			.map(new BrokerRestartingMapper<>(failAfterElements));

		StreamSink<Integer> kafkaSink = kafkaServer.getProducerSink(topic, schema, properties, new FlinkKafkaPartitioner<Integer>() {
			@Override
			public int partition(Integer record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
				return partition;
			}
		});

		if (regularSink) {
			inputStream.addSink(kafkaSink.getUserFunction());
		}
		else {
			kafkaServer.produceIntoKafka(inputStream, topic, schema, properties, new FlinkKafkaPartitioner<Integer>() {
				@Override
				public int partition(Integer record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
					return partition;
				}
			});
		}

		try {
			env.execute("One-to-one at least once test");
			fail("Job should fail!");
		} catch (JobExecutionException ex) {
			// ignore error, it can be one of many errors so it would be hard to check the exception message/cause
		} finally {
			kafkaServer.unblockProxyTraffic();
		}

		// assert that before failure we successfully snapshot/flushed all expected elements
		assertAtLeastOnceForTopic(
				properties,
				topic,
				partition,
				Collections.unmodifiableSet(new HashSet<>(getIntegersSequence(BrokerRestartingMapper.lastSnapshotedElementBeforeShutdown))),
				KAFKA_READ_TIMEOUT);

		deleteTestTopic(topic);
	}

	/**
	 * Tests the exactly-once semantic for the simple writes into Kafka.
	 */
	@Test
	public void testExactlyOnceRegularSink() throws Exception {
		testExactlyOnce(true, 1);
	}

	/**
	 * Tests the exactly-once semantic for the simple writes into Kafka.
	 */
	@Test
	public void testExactlyOnceCustomOperator() throws Exception {
		testExactlyOnce(false, 1);
	}

	/**
	 * This test sets KafkaProducer so that it will  automatically flush the data and
	 * and fails the broker to check whether flushed records since last checkpoint were not duplicated.
	 */
	protected void testExactlyOnce(boolean regularSink, int sinksCount) throws Exception {
		final String topic = (regularSink ? "exactlyOnceTopicRegularSink" : "exactlyTopicCustomOperator") + sinksCount;
		final int partition = 0;
		final int numElements = 1000;
		final int failAfterElements = 333;

		for (int i = 0; i < sinksCount; i++) {
			createTestTopic(topic + i, 1, 1);
		}

		TypeInformationSerializationSchema<Integer> schema = new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);

		// process exactly failAfterElements number of elements and then shutdown Kafka broker and fail application
		List<Integer> expectedElements = getIntegersSequence(numElements);

		DataStream<Integer> inputStream = env
			.addSource(new IntegerSource(numElements))
			.map(new FailingIdentityMapper<Integer>(failAfterElements));

		for (int i = 0; i < sinksCount; i++) {
			FlinkKafkaPartitioner<Integer> partitioner = new FlinkKafkaPartitioner<Integer>() {
				@Override
				public int partition(Integer record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
					return partition;
				}
			};

			if (regularSink) {
				StreamSink<Integer> kafkaSink = kafkaServer.getProducerSink(topic + i, schema, properties, partitioner);
				inputStream.addSink(kafkaSink.getUserFunction());
			} else {
				kafkaServer.produceIntoKafka(inputStream, topic + i, schema, properties, partitioner);
			}
		}

		FailingIdentityMapper.failedBefore = false;
		TestUtils.tryExecute(env, "Exactly once test");

		for (int i = 0; i < sinksCount; i++) {
			// assert that before failure we successfully snapshot/flushed all expected elements
			assertExactlyOnceForTopic(
				properties,
				topic + i,
				partition,
				expectedElements,
				KAFKA_READ_TIMEOUT);
			deleteTestTopic(topic + i);
		}
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

		public static volatile boolean triggeredShutdown;
		public static volatile int lastSnapshotedElementBeforeShutdown;
		public static volatile Runnable shutdownAction;

		private final int failCount;
		private int numElementsTotal;

		private boolean failer;

		public static void resetState(Runnable shutdownAction) {
			triggeredShutdown = false;
			lastSnapshotedElementBeforeShutdown = 0;
			BrokerRestartingMapper.shutdownAction = shutdownAction;
		}

		public BrokerRestartingMapper(int failCount) {
			this.failCount = failCount;
		}

		@Override
		public void open(Configuration parameters) {
			failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
		}

		@Override
		public T map(T value) throws Exception {
			numElementsTotal++;
			Thread.sleep(10);

			if (!triggeredShutdown && failer && numElementsTotal >= failCount) {
				// shut down a Kafka broker
				triggeredShutdown = true;
				shutdownAction.run();
			}
			return value;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
		}

		@Override
		public void notifyCheckpointAborted(long checkpointId) {
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (!triggeredShutdown) {
				lastSnapshotedElementBeforeShutdown = numElementsTotal;
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
		}
	}

	private static final class InfiniteIntegerSource implements SourceFunction<Integer> {

		private volatile boolean running = true;
		private int counter = 0;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				ctx.collect(counter++);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
