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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.typeutils.runtime.ByteArrayInputView;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionLeader;
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
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.testutils.junit.RetryOnException;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.Collector;

import org.apache.flink.util.NetUtils;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assert;

import org.junit.Rule;
import scala.collection.Seq;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@SuppressWarnings("serial")
public abstract class KafkaConsumerTestBase extends KafkaTestBase {
	
	@Rule
	public RetryRule retryRule = new RetryRule();
	
	// ------------------------------------------------------------------------
	//  Required methods by the abstract test base
	// ------------------------------------------------------------------------

	protected abstract <T> FlinkKafkaConsumer<T> getConsumer(
			List<String> topics, DeserializationSchema<T> deserializationSchema, Properties props);

	protected <T> FlinkKafkaConsumer<T> getConsumer(
			String topic, DeserializationSchema<T> deserializationSchema, Properties props) {
		return getConsumer(Collections.singletonList(topic), deserializationSchema, props);
	}

	// ------------------------------------------------------------------------
	//  Suite of Tests
	//
	//  The tests here are all not activated (by an @Test tag), but need
	//  to be invoked from the extending classes. That way, the classes can
	//  select which tests to run.
	// ------------------------------------------------------------------------


	/**
	 * Test that ensures the KafkaConsumer is properly failing if the topic doesnt exist
	 * and a wrong broker was specified
	 *
	 * @throws Exception
	 */
	public void runFailOnNoBrokerTest() throws Exception {
		try {
			Properties properties = new Properties();

			StreamExecutionEnvironment see = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
			see.getConfig().disableSysoutLogging();
			see.setNumberOfExecutionRetries(0);
			see.setParallelism(1);

			// use wrong ports for the consumers
			properties.setProperty("bootstrap.servers", "localhost:80");
			properties.setProperty("zookeeper.connect", "localhost:80");
			properties.setProperty("group.id", "test");
			FlinkKafkaConsumer<String> source = getConsumer("doesntexist", new SimpleStringSchema(), properties);
			DataStream<String> stream = see.addSource(source);
			stream.print();
			see.execute("No broker test");
		} catch(RuntimeException re){
			Assert.assertTrue("Wrong RuntimeException thrown: " + StringUtils.stringifyException(re),
					re.getMessage().contains("Unable to retrieve any partitions for the requested topics [doesntexist]"));
		}
	}
	/**
	 * Test that validates that checkpointing and checkpoint notification works properly
	 */
	public void runCheckpointingTest() throws Exception {
		createTestTopic("testCheckpointing", 1, 1);

		FlinkKafkaConsumer<String> source = getConsumer("testCheckpointing", new SimpleStringSchema(), standardProps);
		Field pendingCheckpointsField = FlinkKafkaConsumer.class.getDeclaredField("pendingCheckpoints");
		pendingCheckpointsField.setAccessible(true);
		LinkedMap pendingCheckpoints = (LinkedMap) pendingCheckpointsField.get(source);

		Assert.assertEquals(0, pendingCheckpoints.size());
		source.setRuntimeContext(new MockRuntimeContext(1, 0));

		final HashMap<KafkaTopicPartition, Long> initialOffsets = new HashMap<>();
		initialOffsets.put(new KafkaTopicPartition("testCheckpointing", 0), 1337L);

		// first restore
		source.restoreState(initialOffsets);

		// then open
		source.open(new Configuration());
		HashMap<KafkaTopicPartition, Long> state1 = source.snapshotState(1, 15);

		assertEquals(initialOffsets, state1);

		HashMap<KafkaTopicPartition, Long> state2 = source.snapshotState(2, 30);
		Assert.assertEquals(initialOffsets, state2);

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
	 * This test is only applicable if the Flink Kafka Consumer uses the ZooKeeperOffsetHandler.
	 */
	public void runOffsetInZookeeperValidationTest() throws Exception {
		final String topicName = "testOffsetInZK";
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
		assertTrue(o2 == FlinkKafkaConsumer.OFFSET_NOT_SET || (o2 >= 0 && o2 <= 100));
		assertTrue(o3 == FlinkKafkaConsumer.OFFSET_NOT_SET || (o3 >= 0 && o3 <= 100));

		LOG.info("Manipulating offsets");

		// set the offset to 50 for the three partitions
		ZookeeperOffsetHandler.setOffsetInZooKeeper(zkClient, standardCC.groupId(), topicName, 0, 49);
		ZookeeperOffsetHandler.setOffsetInZooKeeper(zkClient, standardCC.groupId(), topicName, 1, 49);
		ZookeeperOffsetHandler.setOffsetInZooKeeper(zkClient, standardCC.groupId(), topicName, 2, 49);

		zkClient.close();

		// create new env
		readSequence(env3, standardProps, parallelism, topicName, 50, 50);

		deleteTestTopic(topicName);
	}

	public void runOffsetAutocommitTest() throws Exception {
		final String topicName = "testOffsetAutocommit";
		final int parallelism = 3;

		createTestTopic(topicName, parallelism, 1);

		StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env1.getConfig().disableSysoutLogging();
		env1.setNumberOfExecutionRetries(0);
		env1.setParallelism(parallelism);

		StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		// NOTE: We are not enabling the checkpointing!
		env2.getConfig().disableSysoutLogging();
		env2.setNumberOfExecutionRetries(0);
		env2.setParallelism(parallelism);


		// write a sequence from 0 to 99 to each of the 3 partitions.
		writeSequence(env1, topicName, 100, parallelism);


		// the readSequence operation sleeps for 20 ms between each record.
		// setting a delay of 25*20 = 500 for the commit interval makes
		// sure that we commit roughly 3-4 times while reading, however
		// at least once.
		Properties readProps = new Properties();
		readProps.putAll(standardProps);
		readProps.setProperty("auto.commit.interval.ms", "500");

		// read so that the offset can be committed to ZK
		readSequence(env2, readProps, parallelism, topicName, 100, 0);

		// get the offset
		ZkClient zkClient = createZookeeperClient();

		long o1 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(zkClient, standardCC.groupId(), topicName, 0);
		long o2 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(zkClient, standardCC.groupId(), topicName, 1);
		long o3 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(zkClient, standardCC.groupId(), topicName, 2);

		LOG.info("Got final offsets from zookeeper o1={}, o2={}, o3={}", o1, o2, o3);

		// ensure that the offset has been committed
		assertTrue("Offset of o1=" + o1 + " was not in range", o1 > 0 && o1 <= 100);
		assertTrue("Offset of o2=" + o2 + " was not in range", o2 > 0 && o2 <= 100);
		assertTrue("Offset of o3=" + o3 + " was not in range", o3 > 0 && o3 <= 100);

		deleteTestTopic(topicName);
	}

	/**
	 * Ensure Kafka is working on both producer and consumer side.
	 * This executes a job that contains two Flink pipelines.
	 *
	 * <pre>
	 * (generator source) --> (kafka sink)-[KAFKA-TOPIC]-(kafka source) --> (validating sink)
	 * </pre>
	 * 
	 * We need to externally retry this test. We cannot let Flink's retry mechanism do it, because the Kafka producer
	 * does not guarantee exactly-once output. Hence a recovery would introduce duplicates that
	 * cause the test to fail.
	 *
	 * This test also ensures that FLINK-3156 doesn't happen again:
	 *
	 * The following situation caused a NPE in the FlinkKafkaConsumer
	 *
	 * topic-1 <-- elements are only produced into topic1.
	 * topic-2
	 *
	 * Therefore, this test is consuming as well from an empty topic.
	 *
	 */
	@RetryOnException(times=2, exception=kafka.common.NotLeaderForPartitionException.class)
	public void runSimpleConcurrentProducerConsumerTopology() throws Exception {
		final String topic = "concurrentProducerConsumerTopic_" + UUID.randomUUID().toString();
		final String additionalEmptyTopic = "additionalEmptyTopic_" + UUID.randomUUID().toString();

		final int parallelism = 3;
		final int elementsPerPartition = 100;
		final int totalElements = parallelism * elementsPerPartition;

		createTestTopic(topic, parallelism, 2);
		createTestTopic(additionalEmptyTopic, parallelism, 1); // create an empty topic which will remain empty all the time

		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(parallelism);
		env.enableCheckpointing(500);
		env.setNumberOfExecutionRetries(0); // fail immediately
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
			public void run(SourceContext<Tuple2<Long, String>> ctx) throws InterruptedException {
				int cnt = getRuntimeContext().getIndexOfThisSubtask() * elementsPerPartition;
				int limit = cnt + elementsPerPartition;


				while (running && cnt < limit) {
					ctx.collect(new Tuple2<>(1000L + cnt, "kafka-" + cnt));
					cnt++;
					// we delay data generation a bit so that we are sure that some checkpoints are
					// triggered (for FLINK-3156)
					Thread.sleep(50);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});
		stream.addSink(new FlinkKafkaProducer<>(brokerConnectionStrings, topic, sinkSchema));

		// ----------- add consumer dataflow ----------

		List<String> topics = new ArrayList<>();
		topics.add(topic);
		topics.add(additionalEmptyTopic);
		FlinkKafkaConsumer<Tuple2<Long, String>> source = getConsumer(topics, sourceSchema, standardProps);

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

		try {
			tryExecutePropagateExceptions(env, "runSimpleConcurrentProducerConsumerTopology");
		}
		catch (ProgramInvocationException | JobExecutionException e) {
			// look for NotLeaderForPartitionException
			Throwable cause = e.getCause();

			// search for nested SuccessExceptions
			int depth = 0;
			while (cause != null && depth++ < 20) {
				if (cause instanceof kafka.common.NotLeaderForPartitionException) {
					throw (Exception) cause;
				}
				cause = cause.getCause();
			}
			throw e;
		}

		deleteTestTopic(topic);
	}

	/**
	 * Tests the proper consumption when having a 1:1 correspondence between kafka partitions and
	 * Flink sources.
	 */
	public void runOneToOneExactlyOnceTest() throws Exception {

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

		deleteTestTopic(topic);
	}

	/**
	 * Tests the proper consumption when having fewer Flink sources than Kafka partitions, so
	 * one Flink source will read multiple Kafka partitions.
	 */
	public void runOneSourceMultiplePartitionsExactlyOnceTest() throws Exception {
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

		deleteTestTopic(topic);
	}

	/**
	 * Tests the proper consumption when having more Flink sources than Kafka partitions, which means
	 * that some Flink sources will read no partitions.
	 */
	public void runMultipleSourcesOnePartitionExactlyOnceTest() throws Exception {
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

					FlinkKafkaConsumer<String> source = getConsumer(topic, new SimpleStringSchema(), standardProps);

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

					FlinkKafkaConsumer<String> source = getConsumer(topic, new SimpleStringSchema(), standardProps);

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

	public void runInvalidOffsetTest() throws Exception {
		final String topic = "invalidOffsetTopic";
		final int parallelism = 1;

		// create topic
		createTestTopic(topic, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);

		// write 20 messages into topic:
		writeSequence(env, topic, 20, parallelism);

		// set invalid offset:
		ZkClient zkClient = createZookeeperClient();
		ZookeeperOffsetHandler.setOffsetInZooKeeper(zkClient, standardCC.groupId(), topic, 0, 1234);

		// read from topic
		final int valuesCount = 20;
		final int startFrom = 0;
		readSequence(env, standardCC.props().props(), parallelism, topic, valuesCount, startFrom);

		deleteTestTopic(topic);
	}

	public void runConsumeMultipleTopics() throws java.lang.Exception {
		final int NUM_TOPICS = 5;
		final int NUM_ELEMENTS = 20;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);

		// create topics with content
		final List<String> topics = new ArrayList<>();
		for (int i = 0; i < NUM_TOPICS; i++) {
			final String topic = "topic-" + i;
			topics.add(topic);
			// create topic
			createTestTopic(topic, i + 1 /*partitions*/, 1);

			// write something
			writeSequence(env, topic, NUM_ELEMENTS, i + 1);
		}

		// validate getPartitionsForTopic method
		List<KafkaTopicPartitionLeader> topicPartitions = FlinkKafkaConsumer082.getPartitionsForTopic(topics, standardProps);
		Assert.assertEquals((NUM_TOPICS * (NUM_TOPICS + 1))/2, topicPartitions.size());

		KeyedDeserializationSchema<Tuple3<Integer, Integer, String>> readSchema = new Tuple2WithTopicDeserializationSchema(env.getConfig());
		DataStreamSource<Tuple3<Integer, Integer, String>> stream = env.addSource(new FlinkKafkaConsumer082<>(topics, readSchema, standardProps));

		stream.flatMap(new FlatMapFunction<Tuple3<Integer, Integer, String>, Integer>() {
			Map<String, Integer> countPerTopic = new HashMap<>(NUM_TOPICS);
			@Override
			public void flatMap(Tuple3<Integer, Integer, String> value, Collector<Integer> out) throws Exception {
				Integer count = countPerTopic.get(value.f2);
				if (count == null) {
					count = 1;
				} else {
					count++;
				}
				countPerTopic.put(value.f2, count);

				// check map:
				for (Map.Entry<String, Integer> el: countPerTopic.entrySet()) {
					if (el.getValue() < NUM_ELEMENTS) {
						break; // not enough yet
					}
					if (el.getValue() > NUM_ELEMENTS) {
						throw new RuntimeException("There is a failure in the test. I've read " +
								el.getValue() + " from topic " + el.getKey());
					}
				}
				// we've seen messages from all topics
				throw new SuccessException();
			}
		}).setParallelism(1);

		tryExecute(env, "Count elements from the topics");


		// delete all topics again
		for (int i = 0; i < NUM_TOPICS; i++) {
			final String topic = "topic-" + i;
			deleteTestTopic(topic);
		}
	}

	private static class Tuple2WithTopicDeserializationSchema implements KeyedDeserializationSchema<Tuple3<Integer, Integer, String>> {

		TypeSerializer ts;
		public Tuple2WithTopicDeserializationSchema(ExecutionConfig ec) {
			ts = TypeInfoParser.parse("Tuple2<Integer, Integer>").createSerializer(ec);
		}

		@Override
		public Tuple3<Integer, Integer, String> deserialize(byte[] messageKey, byte[] message, String topic, long offset) throws IOException {
			Tuple2<Integer, Integer> t2 = (Tuple2<Integer, Integer>) ts.deserialize(new ByteArrayInputView(message));
			return new Tuple3<>(t2.f0, t2.f1, topic);
		}

		@Override
		public boolean isEndOfStream(Tuple3<Integer, Integer, String> nextElement) {
			return false;
		}

		@Override
		public TypeInformation<Tuple3<Integer, Integer, String>> getProducedType() {
			return TypeInfoParser.parse("Tuple3<Integer, Integer, String>");
		}
	}

	/**
	 * Test Flink's Kafka integration also with very big records (30MB)
	 * see http://stackoverflow.com/questions/21020347/kafka-sending-a-15mb-message
	 */
	public void runBigRecordTestTopology() throws Exception {

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
					if (elCnt == 11) {
						throw new SuccessException();
					} else {
						throw new RuntimeException("There have been "+elCnt+" elements");
					}
				}
				if (elCnt > 10) {
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
					ctx.collect(new Tuple2<>(cnt++, wl));

					Thread.sleep(100);

					if (cnt == 10) {
						// signal end
						ctx.collect(new Tuple2<>(-1L, new byte[]{1}));
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

	}

	
	public void runBrokerFailureTest() throws Exception {
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

		final kafka.cluster.Broker leaderToShutDown = firstPart.leader().get();
		final String leaderToShutDownConnection = 
				NetUtils.hostAndPortToUrlString(leaderToShutDown.host(), leaderToShutDown.port());
		
		
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
				.map(new BrokerKillingMapper<Integer>(leaderToShutDownConnection, failAfterElements))
				.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		BrokerKillingMapper.killedLeaderBefore = false;
		tryExecute(env, "One-to-one exactly once test");

		// start a new broker:
		brokers.set(leaderIdToShutDown, getKafkaServer(leaderIdToShutDown, tmpKafkaDirs.get(leaderIdToShutDown), kafkaHost, zookeeperConnectionString));

	}

	public void runKeyValueTest() throws Exception {
		final String topic = "keyvaluetest";
		createTestTopic(topic, 1, 1);
		final int ELEMENT_COUNT = 5000;

		// ----------- Write some data into Kafka -------------------

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.setNumberOfExecutionRetries(3);
		env.getConfig().disableSysoutLogging();

		DataStream<Tuple2<Long, PojoValue>> kvStream = env.addSource(new SourceFunction<Tuple2<Long, PojoValue>>() {
			@Override
			public void run(SourceContext<Tuple2<Long, PojoValue>> ctx) throws Exception {
				Random rnd = new Random(1337);
				for (long i = 0; i < ELEMENT_COUNT; i++) {
					PojoValue pojo = new PojoValue();
					pojo.when = new Date(rnd.nextLong());
					pojo.lon = rnd.nextLong();
					pojo.lat = i;
					// make every second key null to ensure proper "null" serialization
					Long key = (i % 2 == 0) ? null : i;
					ctx.collect(new Tuple2<>(key, pojo));
				}
			}
			@Override
			public void cancel() {
			}
		});

		KeyedSerializationSchema<Tuple2<Long, PojoValue>> schema = new TypeInformationKeyValueSerializationSchema<>(Long.class, PojoValue.class, env.getConfig());
		kvStream.addSink(new FlinkKafkaProducer<>(topic, schema,
				FlinkKafkaProducer.getPropertiesFromBrokerList(brokerConnectionStrings)));
		env.execute("Write KV to Kafka");

		// ----------- Read the data again -------------------

		env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.setNumberOfExecutionRetries(3);
		env.getConfig().disableSysoutLogging();


		KeyedDeserializationSchema<Tuple2<Long, PojoValue>> readSchema = new TypeInformationKeyValueSerializationSchema<>(Long.class, PojoValue.class, env.getConfig());

		DataStream<Tuple2<Long, PojoValue>> fromKafka = env.addSource(new FlinkKafkaConsumer082<>(topic, readSchema, standardProps));
		fromKafka.flatMap(new RichFlatMapFunction<Tuple2<Long,PojoValue>, Object>() {
			long counter = 0;
			@Override
			public void flatMap(Tuple2<Long, PojoValue> value, Collector<Object> out) throws Exception {
				// the elements should be in order.
				Assert.assertTrue("Wrong value " + value.f1.lat, value.f1.lat == counter );
				if (value.f1.lat % 2 == 0) {
					Assert.assertNull("key was not null", value.f0);
				} else {
					Assert.assertTrue("Wrong value " + value.f0, value.f0 == counter);
				}
				counter++;
				if (counter == ELEMENT_COUNT) {
					// we got the right number of elements
					throw new SuccessException();
				}
			}
		});

		tryExecute(env, "Read KV from Kafka");

		deleteTestTopic(topic);
	}

	public static class PojoValue {
		public Date when;
		public long lon;
		public long lat;
		public PojoValue() {}
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

		LOG.info("\n===================================\n== Writing sequence of "+numElements+" into "+topicName+" with p="+parallelism+"\n===================================");
		TypeInformation<Tuple2<Integer, Integer>> resultType = TypeInfoParser.parse("Tuple2<Integer, Integer>");

		DataStream<Tuple2<Integer, Integer>> stream = env.addSource(new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {

			private boolean running = true;

			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
				int cnt = 0;
				int partition = getRuntimeContext().getIndexOfThisSubtask();

				while (running && cnt < numElements) {
					ctx.collect(new Tuple2<>(partition, cnt));
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
		if (streams.size() != 1) {
			throw new RuntimeException("Expected only one message stream but got "+streams.size());
		}
		List<KafkaStream<byte[], byte[]>> kafkaStreams = streams.get(topicName);
		if (kafkaStreams == null) {
			throw new RuntimeException("Requested stream not available. Available streams: "+streams.toString());
		}
		if (kafkaStreams.size() != 1) {
			throw new RuntimeException("Requested 1 stream from Kafka, bot got "+kafkaStreams.size()+" streams");
		}
		LOG.info("Opening Consumer instance for topic '{}' on group '{}'", topicName, config.groupId());
		ConsumerIterator<byte[], byte[]> iteratorToRead = kafkaStreams.get(0).iterator();

		List<MessageAndMetadata<byte[], byte[]>> result = new ArrayList<>();
		int read = 0;
		while(iteratorToRead.hasNext()) {
			read++;
			result.add(iteratorToRead.next());
			if (read == stopAfter) {
				LOG.info("Read "+read+" elements");
				return result;
			}
		}
		return result;
	}

	private static void printTopic(String topicName, ConsumerConfig config,
								DeserializationSchema<?> deserializationSchema,
								int stopAfter) throws IOException {

		List<MessageAndMetadata<byte[], byte[]>> contents = readTopicToList(topicName, config, stopAfter);
		LOG.info("Printing contents of topic {} in consumer grouo {}", topicName, config.groupId());

		for (MessageAndMetadata<byte[], byte[]> message: contents) {
			Object out = deserializationSchema.deserialize(message.message());
			LOG.info("Message: partition: {} offset: {} msg: {}", message.partition(), message.offset(), out.toString());
		}
	}

	private static void printTopic(String topicName, int elements,DeserializationSchema<?> deserializer) 
			throws IOException
	{
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
						String connectionUrl = 
								NetUtils.hostAndPortToUrlString(
										kafkaServer.config().advertisedHostName(),
										kafkaServer.config().advertisedPort());
						if (leaderToShutDown.equals(connectionUrl)) {
							toShutDown = kafkaServer;
							break;
						}
					}
	
					if (toShutDown == null) {
						StringBuilder listOfBrokers = new StringBuilder();
						for (KafkaServer kafkaServer : brokers) {
							listOfBrokers.append(
									NetUtils.hostAndPortToUrlString(
											kafkaServer.config().advertisedHostName(),
											kafkaServer.config().advertisedPort()));
							listOfBrokers.append(" ; ");
						}
						
						throw new Exception("Cannot find broker to shut down: " + leaderToShutDown
								+ " ; available brokers: " + listOfBrokers.toString());
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
