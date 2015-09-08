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

import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.server.KafkaServer;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections.map.LinkedMap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.ZookeeperOffsetHandler;
import org.apache.flink.streaming.connectors.kafka.testutils.DataGenerators;
import org.apache.flink.streaming.connectors.kafka.testutils.DiscardingSink;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.JobManagerCommunicationUtils;
import org.apache.flink.streaming.connectors.kafka.testutils.MockRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.testutils.PartitionValidatingMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.SuccessException;
import org.apache.flink.streaming.connectors.kafka.testutils.ThrottledMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.Tuple2Partitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.ValidatingExactlyOnceSink;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assert;

import scala.collection.Seq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@SuppressWarnings("serial")
public abstract class KafkaConsumerTestBase extends KafkaTestBase {


	// ------------------------------------------------------------------------
	//  Required methods by the abstract test base
	// ------------------------------------------------------------------------

	protected abstract <T> FlinkKafkaConsumer<T> getConsumer(
			String topic, DeserializationSchema<T> deserializationSchema, Properties props);

	// ------------------------------------------------------------------------
	//  Suite of Tests
	//
	//  The tests here are all not activated (by an @Test tag), but need
	//  to be invoked from the extending classes. That way, the classes can
	//  select which tests to run.
	// ------------------------------------------------------------------------

	/**
	 * Test that validates that checkpointing and checkpoint notification works properly
	 */
	public void runCheckpointingTest() throws Exception {
		createTestTopic("testCheckpointing", 1, 1);

		FlinkKafkaConsumer<String> source = getConsumer("testCheckpointing", new JavaDefaultStringSchema(), standardProps);
		Field pendingCheckpointsField = FlinkKafkaConsumer.class.getDeclaredField("pendingCheckpoints");
		pendingCheckpointsField.setAccessible(true);
		LinkedMap pendingCheckpoints = (LinkedMap) pendingCheckpointsField.get(source);

		Assert.assertEquals(0, pendingCheckpoints.size());
		source.setRuntimeContext(new MockRuntimeContext(1, 0));

		final long[] initialOffsets = new long[] { 1337 };

		// first restore
		source.restoreState(initialOffsets);

		// then open
		source.open(new Configuration());
		long[] state1 = source.snapshotState(1, 15);

		assertArrayEquals(initialOffsets, state1);

		long[] state2 = source.snapshotState(2, 30);
		Assert.assertArrayEquals(initialOffsets, state2);
		Assert.assertEquals(2, pendingCheckpoints.size());

		source.notifyCheckpointComplete(1);
		Assert.assertEquals(1, pendingCheckpoints.size());

		source.notifyCheckpointComplete(2);
		Assert.assertEquals(0, pendingCheckpoints.size());

		source.notifyCheckpointComplete(666); // invalid checkpoint
		Assert.assertEquals(0, pendingCheckpoints.size());

		// create 500 snapshots
		for (int i = 100; i < 600; i++) {
			source.snapshotState(i, 15 * i);
		}
		Assert.assertEquals(FlinkKafkaConsumer.MAX_NUM_PENDING_CHECKPOINTS, pendingCheckpoints.size());

		// commit only the second last
		source.notifyCheckpointComplete(598);
		Assert.assertEquals(1, pendingCheckpoints.size());

		// access invalid checkpoint
		source.notifyCheckpointComplete(590);

		// and the last
		source.notifyCheckpointComplete(599);
		Assert.assertEquals(0, pendingCheckpoints.size());

		source.close();

		deleteTestTopic("testCheckpointing");
	}

	/**
	 * Tests that offsets are properly committed to ZooKeeper and initial offsets are read from ZooKeeper.
	 *
	 * This test is only applicable if Teh Flink Kafka Consumer uses the ZooKeeperOffsetHandler.
	 */
	public void runOffsetInZookeeperValidationTest() throws Exception {
		LOG.info("Starting testFlinkKafkaConsumerWithOffsetUpdates()");

		final String topicName = "testOffsetHacking";
		final int parallelism = 3;

		createTestTopic(topicName, parallelism, 1);

		StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env1.getConfig().disableSysoutLogging();
		env1.enableCheckpointing(50);
		env1.setNumberOfExecutionRetries(0);
		env1.setParallelism(parallelism);

		StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env2.getConfig().disableSysoutLogging();
		env2.enableCheckpointing(50);
		env2.setNumberOfExecutionRetries(0);
		env2.setParallelism(parallelism);

		StreamExecutionEnvironment env3 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env3.getConfig().disableSysoutLogging();
		env3.enableCheckpointing(50);
		env3.setNumberOfExecutionRetries(0);
		env3.setParallelism(parallelism);

		// write a sequence from 0 to 99 to each of the 3 partitions.
		writeSequence(env1, topicName, 100, parallelism);

		readSequence(env2, standardProps, parallelism, topicName, 100, 0);

		ZkClient zkClient = createZookeeperClient();

		long o1 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(zkClient, standardCC.groupId(), topicName, 0);
		long o2 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(zkClient, standardCC.groupId(), topicName, 1);
		long o3 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(zkClient, standardCC.groupId(), topicName, 2);

		LOG.info("Got final offsets from zookeeper o1={}, o2={}, o3={}", o1, o2, o3);

		assertTrue(o1 == FlinkKafkaConsumer.OFFSET_NOT_SET || (o1 >= 0 && o1 <= 100));
		assertTrue(o2 == FlinkKafkaConsumer.OFFSET_NOT_SET || (o1 >= 0 && o1 <= 100));
		assertTrue(o3 == FlinkKafkaConsumer.OFFSET_NOT_SET || (o1 >= 0 && o1 <= 100));

		LOG.info("Manipulating offsets");

		// set the offset to 50 for the three partitions
		ZookeeperOffsetHandler.setOffsetInZooKeeper(zkClient, standardCC.groupId(), topicName, 0, 49);
		ZookeeperOffsetHandler.setOffsetInZooKeeper(zkClient, standardCC.groupId(), topicName, 1, 49);
		ZookeeperOffsetHandler.setOffsetInZooKeeper(zkClient, standardCC.groupId(), topicName, 2, 49);

		zkClient.close();

		// create new env
		readSequence(env3, standardProps, parallelism, topicName, 50, 50);

		deleteTestTopic(topicName);

		LOG.info("Finished testFlinkKafkaConsumerWithOffsetUpdates()");
	}

	/**
	 * Ensure Kafka is working on both producer and consumer side.
	 * This executes a job that contains two Flink pipelines.
	 *
	 * <pre>
	 * (generator source) --> (kafka sink)-[KAFKA-TOPIC]-(kafka source) --> (validating sink)
	 * </pre>
	 */
	public void runSimpleConcurrentProducerConsumerTopology() throws Exception {
		LOG.info("Starting runSimpleConcurrentProducerConsumerTopology()");

		final String topic = "concurrentProducerConsumerTopic";
		final int parallelism = 3;
		final int elementsPerPartition = 100;
		final int totalElements = parallelism * elementsPerPartition;

		createTestTopic(topic, parallelism, 2);

		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(parallelism);
		env.setNumberOfExecutionRetries(0);
		env.getConfig().disableSysoutLogging();

		TypeInformation<Tuple2<Long, String>> longStringType = TypeInfoParser.parse("Tuple2<Long, String>");

		TypeInformationSerializationSchema<Tuple2<Long, String>> sourceSchema =
				new TypeInformationSerializationSchema<>(longStringType, env.getConfig());

		TypeInformationSerializationSchema<Tuple2<Long, String>> sinkSchema =
				new TypeInformationSerializationSchema<>(longStringType, env.getConfig());

		// ----------- add producer dataflow ----------

		DataStream<Tuple2<Long, String>> stream = env.addSource(new RichParallelSourceFunction<Tuple2<Long,String>>() {

			private boolean running = true;

			@Override
			public void run(SourceContext<Tuple2<Long, String>> ctx) {
				int cnt = getRuntimeContext().getIndexOfThisSubtask() * elementsPerPartition;
				int limit = cnt + elementsPerPartition;


				while (running && cnt < limit) {
					ctx.collect(new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt));
					cnt++;
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});
		stream.addSink(new FlinkKafkaProducer<>(brokerConnectionStrings, topic, sinkSchema));

		// ----------- add consumer dataflow ----------

		FlinkKafkaConsumer<Tuple2<Long, String>> source = getConsumer(topic, sourceSchema, standardProps);

		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(source).setParallelism(parallelism);

		consuming.addSink(new RichSinkFunction<Tuple2<Long, String>>() {

			private int elCnt = 0;
			private BitSet validator = new BitSet(totalElements);

			@Override
			public void invoke(Tuple2<Long, String> value) throws Exception {
				String[] sp = value.f1.split("-");
				int v = Integer.parseInt(sp[1]);

				assertEquals(value.f0 - 1000, (long) v);

				assertFalse("Received tuple twice", validator.get(v));
				validator.set(v);
				elCnt++;

				if (elCnt == totalElements) {
					// check if everything in the bitset is set to true
					int nc;
					if ((nc = validator.nextClearBit(0)) != totalElements) {
						fail("The bitset was not set to 1 on all elements. Next clear:"
								+ nc + " Set: " + validator);
					}
					throw new SuccessException();
				}
			}

			@Override
			public void close() throws Exception {
				super.close();
			}
		}).setParallelism(1);

		tryExecute(env, "runSimpleConcurrentProducerConsumerTopology");

		LOG.info("Finished runSimpleConcurrentProducerConsumerTopology()");

		deleteTestTopic(topic);
	}

	/**
	 * Tests the proper consumption when having a 1:1 correspondence between kafka partitions and
	 * Flink sources.
	 */
	public void runOneToOneExactlyOnceTest() throws Exception {
		LOG.info("Starting runOneToOneExactlyOnceTest()");

		final String topic = "oneToOneTopic";
		final int parallelism = 5;
		final int numElementsPerPartition = 1000;
		final int totalElements = parallelism * numElementsPerPartition;
		final int failAfterElements = numElementsPerPartition / 3;

		createTestTopic(topic, parallelism, 1);

		DataGenerators.generateRandomizedIntegerSequence(
				StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort),
				brokerConnectionStrings,
				topic, parallelism, numElementsPerPartition, true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setNumberOfExecutionRetries(3);
		env.getConfig().disableSysoutLogging();

		FlinkKafkaConsumer<Integer> kafkaSource = getConsumer(topic, schema, standardProps);

		env
				.addSource(kafkaSource)
				.map(new PartitionValidatingMapper(parallelism, 1))
				.map(new FailingIdentityMapper<Integer>(failAfterElements))
				.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		FailingIdentityMapper.failedBefore = false;
		tryExecute(env, "One-to-one exactly once test");

		// this cannot be reliably checked, as checkpoints come in time intervals, and
		// failures after a number of elements
//			assertTrue("Job did not do a checkpoint before the failure",
//					FailingIdentityMapper.hasBeenCheckpointedBeforeFailure);

		deleteTestTopic(topic);
	}

	/**
	 * Tests the proper consumption when having fewer Flink sources than Kafka partitions, so
	 * one Flink source will read multiple Kafka partitions.
	 */
	public void runOneSourceMultiplePartitionsExactlyOnceTest() throws Exception {
		LOG.info("Starting runOneSourceMultiplePartitionsExactlyOnceTest()");

		final String topic = "oneToManyTopic";
		final int numPartitions = 5;
		final int numElementsPerPartition = 1000;
		final int totalElements = numPartitions * numElementsPerPartition;
		final int failAfterElements = numElementsPerPartition / 3;

		final int parallelism = 2;

		createTestTopic(topic, numPartitions, 1);

		DataGenerators.generateRandomizedIntegerSequence(
				StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort),
				brokerConnectionStrings,
				topic, numPartitions, numElementsPerPartition, true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setNumberOfExecutionRetries(3);
		env.getConfig().disableSysoutLogging();

		FlinkKafkaConsumer<Integer> kafkaSource = getConsumer(topic, schema, standardProps);

		env
				.addSource(kafkaSource)
				.map(new PartitionValidatingMapper(numPartitions, 3))
				.map(new FailingIdentityMapper<Integer>(failAfterElements))
				.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		FailingIdentityMapper.failedBefore = false;
		tryExecute(env, "One-source-multi-partitions exactly once test");

		// this cannot be reliably checked, as checkpoints come in time intervals, and
		// failures after a number of elements
//			assertTrue("Job did not do a checkpoint before the failure",
//					FailingIdentityMapper.hasBeenCheckpointedBeforeFailure);

		deleteTestTopic(topic);
	}

	/**
	 * Tests the proper consumption when having more Flink sources than Kafka partitions, which means
	 * that some Flink sources will read no partitions.
	 */
	public void runMultipleSourcesOnePartitionExactlyOnceTest() throws Exception {
		LOG.info("Starting runMultipleSourcesOnePartitionExactlyOnceTest()");

		final String topic = "manyToOneTopic";
		final int numPartitions = 5;
		final int numElementsPerPartition = 1000;
		final int totalElements = numPartitions * numElementsPerPartition;
		final int failAfterElements = numElementsPerPartition / 3;

		final int parallelism = 8;

		createTestTopic(topic, numPartitions, 1);

		DataGenerators.generateRandomizedIntegerSequence(
				StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort),
				brokerConnectionStrings,
				topic, numPartitions, numElementsPerPartition, true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setNumberOfExecutionRetries(3);
		env.getConfig().disableSysoutLogging();
		env.setBufferTimeout(0);

		FlinkKafkaConsumer<Integer> kafkaSource = getConsumer(topic, schema, standardProps);

		env
			.addSource(kafkaSource)
			.map(new PartitionValidatingMapper(numPartitions, 1))
			.map(new FailingIdentityMapper<Integer>(failAfterElements))
			.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		FailingIdentityMapper.failedBefore = false;
		tryExecute(env, "multi-source-one-partitions exactly once test");

		// this cannot be reliably checked, as checkpoints come in time intervals, and
		// failures after a number of elements
//			assertTrue("Job did not do a checkpoint before the failure",
//					FailingIdentityMapper.hasBeenCheckpointedBeforeFailure);

		deleteTestTopic(topic);
	}
	
	
	/**
	 * Tests that the source can be properly canceled when reading full partitions. 
	 */
	public void runCancelingOnFullInputTest() throws Exception {
		final String topic = "cancelingOnFullTopic";

		final int parallelism = 3;
		createTestTopic(topic, parallelism, 1);

		// launch a producer thread
		DataGenerators.InfiniteStringsGenerator generator =
				new DataGenerators.InfiniteStringsGenerator(brokerConnectionStrings, topic);
		generator.start();

		// launch a consumer asynchronously

		final AtomicReference<Throwable> jobError = new AtomicReference<>();

		final Runnable jobRunner = new Runnable() {
			@Override
			public void run() {
				try {
					final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
					env.setParallelism(parallelism);
					env.enableCheckpointing(100);
					env.getConfig().disableSysoutLogging();

					FlinkKafkaConsumer<String> source = getConsumer(topic, new JavaDefaultStringSchema(), standardProps);

					env.addSource(source).addSink(new DiscardingSink<String>());

					env.execute();
				}
				catch (Throwable t) {
					jobError.set(t);
				}
			}
		};

		Thread runnerThread = new Thread(jobRunner, "program runner thread");
		runnerThread.start();

		// wait a bit before canceling
		Thread.sleep(2000);

		// cancel
		JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));

		// wait for the program to be done and validate that we failed with the right exception
		runnerThread.join();

		Throwable failueCause = jobError.get();
		assertNotNull("program did not fail properly due to canceling", failueCause);
		assertTrue(failueCause.getMessage().contains("Job was cancelled"));

		if (generator.isAlive()) {
			generator.shutdown();
			generator.join();
		}
		else {
			Throwable t = generator.getError();
			if (t != null) {
				t.printStackTrace();
				fail("Generator failed: " + t.getMessage());
			} else {
				fail("Generator failed with no exception");
			}
		}

		deleteTestTopic(topic);
	}

	/**
	 * Tests that the source can be properly canceled when reading empty partitions. 
	 */
	public void runCancelingOnEmptyInputTest() throws Exception {
		final String topic = "cancelingOnEmptyInputTopic";

		final int parallelism = 3;
		createTestTopic(topic, parallelism, 1);

		final AtomicReference<Throwable> error = new AtomicReference<>();

		final Runnable jobRunner = new Runnable() {
			@Override
			public void run() {
				try {
					final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
					env.setParallelism(parallelism);
					env.enableCheckpointing(100);
					env.getConfig().disableSysoutLogging();

					FlinkKafkaConsumer<String> source = getConsumer(topic, new JavaDefaultStringSchema(), standardProps);

					env.addSource(source).addSink(new DiscardingSink<String>());

					env.execute();
				}
				catch (Throwable t) {
					error.set(t);
				}
			}
		};

		Thread runnerThread = new Thread(jobRunner, "program runner thread");
		runnerThread.start();

		// wait a bit before canceling
		Thread.sleep(2000);

		// cancel
		JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));

		// wait for the program to be done and validate that we failed with the right exception
		runnerThread.join();

		Throwable failueCause = error.get();
		assertNotNull("program did not fail properly due to canceling", failueCause);
		assertTrue(failueCause.getMessage().contains("Job was cancelled"));

		deleteTestTopic(topic);
	}

	/**
	 * Tests that the source can be properly canceled when reading full partitions. 
	 */
	public void runFailOnDeployTest() throws Exception {
		final String topic = "failOnDeployTopic";

		createTestTopic(topic, 2, 1);

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(12); // needs to be more that the mini cluster has slots
		env.getConfig().disableSysoutLogging();

		FlinkKafkaConsumer<Integer> kafkaSource = getConsumer(topic, schema, standardProps);

		env
				.addSource(kafkaSource)
				.addSink(new DiscardingSink<Integer>());

		try {
			env.execute();
			fail("this test should fail with an exception");
		}
		catch (ProgramInvocationException e) {

			// validate that we failed due to a NoResourceAvailableException
			Throwable cause = e.getCause();
			int depth = 0;
			boolean foundResourceException = false;

			while (cause != null && depth++ < 20) {
				if (cause instanceof NoResourceAvailableException) {
					foundResourceException = true;
					break;
				}
				cause = cause.getCause();
			}

			assertTrue("Wrong exception", foundResourceException);
		}

		deleteTestTopic(topic);
	}

	/**
	 * Test Flink's Kafka integration also with very big records (30MB)
	 * see http://stackoverflow.com/questions/21020347/kafka-sending-a-15mb-message
	 */
	public void runBigRecordTestTopology() throws Exception {
		LOG.info("Starting runBigRecordTestTopology()");

		final String topic = "bigRecordTestTopic";
		final int parallelism = 1; // otherwise, the kafka mini clusters may run out of heap space

		createTestTopic(topic, parallelism, 1);

		final TypeInformation<Tuple2<Long, byte[]>> longBytesInfo = TypeInfoParser.parse("Tuple2<Long, byte[]>");

		final TypeInformationSerializationSchema<Tuple2<Long, byte[]>> serSchema =
				new TypeInformationSerializationSchema<>(longBytesInfo, new ExecutionConfig());

		final TypeInformationSerializationSchema<Tuple2<Long, byte[]>> deserSchema =
				new TypeInformationSerializationSchema<>(longBytesInfo, new ExecutionConfig());

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setNumberOfExecutionRetries(0);
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(100);
		env.setParallelism(parallelism);

		// add consuming topology:
		Properties consumerProps = new Properties();
		consumerProps.putAll(standardProps);
		consumerProps.setProperty("fetch.message.max.bytes", Integer.toString(1024 * 1024 * 40));
		consumerProps.setProperty("max.partition.fetch.bytes", Integer.toString(1024 * 1024 * 40)); // for the new fetcher
		consumerProps.setProperty("queued.max.message.chunks", "1");

		FlinkKafkaConsumer<Tuple2<Long, byte[]>> source = getConsumer(topic, serSchema, consumerProps);
		DataStreamSource<Tuple2<Long, byte[]>> consuming = env.addSource(source);

		consuming.addSink(new SinkFunction<Tuple2<Long, byte[]>>() {

			private int elCnt = 0;

			@Override
			public void invoke(Tuple2<Long, byte[]> value) throws Exception {
				elCnt++;
				if (value.f0 == -1) {
					// we should have seen 11 elements now.
					if(elCnt == 11) {
						throw new SuccessException();
					} else {
						throw new RuntimeException("There have been "+elCnt+" elements");
					}
				}
				if(elCnt > 10) {
					throw new RuntimeException("More than 10 elements seen: "+elCnt);
				}
			}
		});

		// add producing topology
		Properties producerProps = new Properties();
		producerProps.setProperty("max.request.size", Integer.toString(1024 * 1024 * 30));
		producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectionStrings);

		DataStream<Tuple2<Long, byte[]>> stream = env.addSource(new RichSourceFunction<Tuple2<Long, byte[]>>() {

			private boolean running;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				running = true;
			}

			@Override
			public void run(SourceContext<Tuple2<Long, byte[]>> ctx) throws Exception {
				Random rnd = new Random();
				long cnt = 0;
				int fifteenMb = 1024 * 1024 * 15;

				while (running) {
					byte[] wl = new byte[fifteenMb + rnd.nextInt(fifteenMb)];
					ctx.collect(new Tuple2<Long, byte[]>(cnt++, wl));

					Thread.sleep(100);

					if (cnt == 10) {
						// signal end
						ctx.collect(new Tuple2<Long, byte[]>(-1L, new byte[]{1}));
						break;
					}
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		stream.addSink(new FlinkKafkaProducer<>(topic, deserSchema, producerProps));

		tryExecute(env, "big topology test");

		deleteTestTopic(topic);

		LOG.info("Finished runBigRecordTestTopology()");
	}

	
	public void runBrokerFailureTest() throws Exception {
		LOG.info("starting runBrokerFailureTest()");

		final String topic = "brokerFailureTestTopic";

		final int parallelism = 2;
		final int numElementsPerPartition = 1000;
		final int totalElements = parallelism * numElementsPerPartition;
		final int failAfterElements = numElementsPerPartition / 3;


		createTestTopic(topic, parallelism, 2);

		DataGenerators.generateRandomizedIntegerSequence(
				StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort),
				brokerConnectionStrings,
				topic, parallelism, numElementsPerPartition, true);

		// find leader to shut down
		ZkClient zkClient = createZookeeperClient();
		PartitionMetadata firstPart = null;
		do {
			if (firstPart != null) {
				LOG.info("Unable to find leader. error code {}", firstPart.errorCode());
				// not the first try. Sleep a bit
				Thread.sleep(150);
			}

			Seq<PartitionMetadata> partitionMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient).partitionsMetadata();
			firstPart = partitionMetadata.head();
		}
		while (firstPart.errorCode() != 0);
		zkClient.close();

		final String leaderToShutDown = firstPart.leader().get().connectionString();
		final int leaderIdToShutDown = firstPart.leader().get().id();
		LOG.info("Leader to shutdown {}", leaderToShutDown);


		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(parallelism);
		env.enableCheckpointing(500);
		env.setNumberOfExecutionRetries(3);
		env.getConfig().disableSysoutLogging();


		FlinkKafkaConsumer<Integer> kafkaSource = getConsumer(topic, schema, standardProps);

		env
				.addSource(kafkaSource)
				.map(new PartitionValidatingMapper(parallelism, 1))
				.map(new BrokerKillingMapper<Integer>(leaderToShutDown, failAfterElements))
				.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		BrokerKillingMapper.killedLeaderBefore = false;
		tryExecute(env, "One-to-one exactly once test");

		// start a new broker:
		brokers.set(leaderIdToShutDown, getKafkaServer(leaderIdToShutDown, tmpKafkaDirs.get(leaderIdToShutDown), kafkaHost, zookeeperConnectionString));

		LOG.info("finished runBrokerFailureTest()");
	}

	// ------------------------------------------------------------------------
	//  Reading writing test data sets
	// ------------------------------------------------------------------------

	private void readSequence(StreamExecutionEnvironment env, Properties cc,
								final int sourceParallelism,
								final String topicName,
								final int valuesCount, final int startFrom) throws Exception {

		final int finalCount = valuesCount * sourceParallelism;

		final TypeInformation<Tuple2<Integer, Integer>> intIntTupleType = TypeInfoParser.parse("Tuple2<Integer, Integer>");

		final TypeInformationSerializationSchema<Tuple2<Integer, Integer>> deser =
				new TypeInformationSerializationSchema<>(intIntTupleType, env.getConfig());

		// create the consumer
		FlinkKafkaConsumer<Tuple2<Integer, Integer>> consumer = getConsumer(topicName, deser, cc);

		DataStream<Tuple2<Integer, Integer>> source = env
				.addSource(consumer).setParallelism(sourceParallelism)
				.map(new ThrottledMapper<Tuple2<Integer, Integer>>(20)).setParallelism(sourceParallelism);

		// verify data
		source.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Integer>() {

			private int[] values = new int[valuesCount];
			private int count = 0;

			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Integer> out) throws Exception {
				values[value.f1 - startFrom]++;
				count++;

				// verify if we've seen everything
				if (count == finalCount) {
					for (int i = 0; i < values.length; i++) {
						int v = values[i];
						if (v != sourceParallelism) {
							printTopic(topicName, valuesCount, deser);
							throw new RuntimeException("Expected v to be 3, but was " + v + " on element " + i + " array=" + Arrays.toString(values));
						}
					}
					// test has passed
					throw new SuccessException();
				}
			}

		}).setParallelism(1);

		tryExecute(env, "Read data from Kafka");

		LOG.info("Successfully read sequence for verification");
	}

	private static void writeSequence(StreamExecutionEnvironment env, String topicName, final int numElements, int parallelism) throws Exception {

		TypeInformation<Tuple2<Integer, Integer>> resultType = TypeInfoParser.parse("Tuple2<Integer, Integer>");

		DataStream<Tuple2<Integer, Integer>> stream = env.addSource(new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {

			private boolean running = true;

			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
				int cnt = 0;
				int partition = getRuntimeContext().getIndexOfThisSubtask();

				while (running && cnt < numElements) {
					ctx.collect(new Tuple2<Integer, Integer>(partition, cnt));
					cnt++;
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		}).setParallelism(parallelism);
		
		stream.addSink(new FlinkKafkaProducer<>(topicName,
				new TypeInformationSerializationSchema<>(resultType, env.getConfig()),
				FlinkKafkaProducer.getPropertiesFromBrokerList(brokerConnectionStrings),
				new Tuple2Partitioner(parallelism)
		)).setParallelism(parallelism);

		env.execute("Write sequence");

		LOG.info("Finished writing sequence");
	}

	// ------------------------------------------------------------------------
	//  Debugging utilities
	// ------------------------------------------------------------------------

	/**
	 * Read topic to list, only using Kafka code.
	 */
	private static List<MessageAndMetadata<byte[], byte[]>> readTopicToList(String topicName, ConsumerConfig config, final int stopAfter) {
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(config);
		// we request only one stream per consumer instance. Kafka will make sure that each consumer group
		// will see each message only once.
		Map<String,Integer> topicCountMap = Collections.singletonMap(topicName, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumerConnector.createMessageStreams(topicCountMap);
		if(streams.size() != 1) {
			throw new RuntimeException("Expected only one message stream but got "+streams.size());
		}
		List<KafkaStream<byte[], byte[]>> kafkaStreams = streams.get(topicName);
		if(kafkaStreams == null) {
			throw new RuntimeException("Requested stream not available. Available streams: "+streams.toString());
		}
		if(kafkaStreams.size() != 1) {
			throw new RuntimeException("Requested 1 stream from Kafka, bot got "+kafkaStreams.size()+" streams");
		}
		LOG.info("Opening Consumer instance for topic '{}' on group '{}'", topicName, config.groupId());
		ConsumerIterator<byte[], byte[]> iteratorToRead = kafkaStreams.get(0).iterator();

		List<MessageAndMetadata<byte[], byte[]>> result = new ArrayList<MessageAndMetadata<byte[], byte[]>>();
		int read = 0;
		while(iteratorToRead.hasNext()) {
			read++;
			result.add(iteratorToRead.next());
			if(read == stopAfter) {
				LOG.info("Read "+read+" elements");
				return result;
			}
		}
		return result;
	}

	private static void printTopic(String topicName, ConsumerConfig config,
								DeserializationSchema<?> deserializationSchema,
								int stopAfter) {

		List<MessageAndMetadata<byte[], byte[]>> contents = readTopicToList(topicName, config, stopAfter);
		LOG.info("Printing contents of topic {} in consumer grouo {}", topicName, config.groupId());

		for (MessageAndMetadata<byte[], byte[]> message: contents) {
			Object out = deserializationSchema.deserialize(message.message());
			LOG.info("Message: partition: {} offset: {} msg: {}", message.partition(), message.offset(), out.toString());
		}
	}

	private static void printTopic(String topicName, int elements,DeserializationSchema<?> deserializer) {
		// write the sequence to log for debugging purposes
		Properties stdProps = standardCC.props().props();
		Properties newProps = new Properties(stdProps);
		newProps.setProperty("group.id", "topic-printer"+ UUID.randomUUID().toString());
		newProps.setProperty("auto.offset.reset", "smallest");
		newProps.setProperty("zookeeper.connect", standardCC.zkConnect());

		ConsumerConfig printerConfig = new ConsumerConfig(newProps);
		printTopic(topicName, printerConfig, deserializer, elements);
	}


	public static class BrokerKillingMapper<T> extends RichMapFunction<T,T>
			implements Checkpointed<Integer>, CheckpointNotifier {

		private static final long serialVersionUID = 6334389850158707313L;

		public static volatile boolean killedLeaderBefore;
		public static volatile boolean hasBeenCheckpointedBeforeFailure;
		
		private final String leaderToShutDown;
		private final int failCount;
		private int numElementsTotal;

		private boolean failer;
		private boolean hasBeenCheckpointed;


		public BrokerKillingMapper(String leaderToShutDown, int failCount) {
			this.leaderToShutDown = leaderToShutDown;
			this.failCount = failCount;
		}

		@Override
		public void open(Configuration parameters) {
			failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
		}

		@Override
		public T map(T value) throws Exception {
			numElementsTotal++;
			
			if (!killedLeaderBefore) {
				Thread.sleep(10);
				
				if (failer && numElementsTotal >= failCount) {
					// shut down a Kafka broker
					KafkaServer toShutDown = null;
					for (KafkaServer kafkaServer : brokers) {
						if (leaderToShutDown.equals(kafkaServer.config().advertisedHostName()+ ":"+ kafkaServer.config().advertisedPort())) {
							toShutDown = kafkaServer;
							break;
						}
					}
	
					if (toShutDown == null) {
						throw new Exception("Cannot find broker to shut down");
					}
					else {
						hasBeenCheckpointedBeforeFailure = hasBeenCheckpointed;
						killedLeaderBefore = true;
						toShutDown.shutdown();
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
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) {
			return numElementsTotal;
		}

		@Override
		public void restoreState(Integer state) {
			this.numElementsTotal = state;
		}
	}
}
