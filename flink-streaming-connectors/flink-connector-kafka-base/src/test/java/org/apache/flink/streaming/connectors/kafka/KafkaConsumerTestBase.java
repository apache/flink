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

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.server.KafkaServer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.testutils.DataGenerators;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.JobManagerCommunicationUtils;
import org.apache.flink.streaming.connectors.kafka.testutils.PartitionValidatingMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.ThrottledMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.Tuple2Partitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.ValidatingExactlyOnceSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.testutils.junit.RetryOnException;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.Collector;

import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Assert;

import org.junit.Before;
import org.junit.Rule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@SuppressWarnings("serial")
public abstract class KafkaConsumerTestBase extends KafkaTestBase {
	
	@Rule
	public RetryRule retryRule = new RetryRule();


	// ------------------------------------------------------------------------
	//  Common Test Preparation
	// ------------------------------------------------------------------------

	/**
	 * Makes sure that no job is on the JobManager any more from any previous tests that use
	 * the same mini cluster. Otherwise, missing slots may happen.
	 */
	@Before
	public void ensureNoJobIsLingering() throws Exception {
		JobManagerCommunicationUtils.waitUntilNoJobIsRunning(flink.getLeaderGateway(timeout));
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
			see.setRestartStrategy(RestartStrategies.noRestart());
			see.setParallelism(1);

			// use wrong ports for the consumers
			properties.setProperty("bootstrap.servers", "localhost:80");
			properties.setProperty("zookeeper.connect", "localhost:80");
			properties.setProperty("group.id", "test");
			properties.setProperty("request.timeout.ms", "3000"); // let the test fail fast
			properties.setProperty("socket.timeout.ms", "3000");
			properties.setProperty("session.timeout.ms", "2000");
			properties.setProperty("fetch.max.wait.ms", "2000");
			properties.setProperty("heartbeat.interval.ms", "1000");
			FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer("doesntexist", new SimpleStringSchema(), properties);
			DataStream<String> stream = see.addSource(source);
			stream.print();
			see.execute("No broker test");
		} catch(RuntimeException re) {
			if(kafkaServer.getVersion().equals("0.9")) {
				Assert.assertTrue("Wrong RuntimeException thrown: " + StringUtils.stringifyException(re),
						re.getClass().equals(TimeoutException.class) &&
								re.getMessage().contains("Timeout expired while fetching topic metadata"));
			} else {
				Assert.assertTrue("Wrong RuntimeException thrown: " + StringUtils.stringifyException(re),
						re.getClass().equals(RuntimeException.class) &&
								re.getMessage().contains("Unable to retrieve any partitions for the requested topics [doesntexist]"));
			}
		}
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
		env.setRestartStrategy(RestartStrategies.noRestart()); // fail immediately
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
		Properties producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings);
		producerProperties.setProperty("retries", "3");
		stream.addSink(kafkaServer.getProducer(topic, new KeyedSerializationSchemaWrapper<>(sinkSchema), producerProperties, null));

		// ----------- add consumer dataflow ----------

		List<String> topics = new ArrayList<>();
		topics.add(topic);
		topics.add(additionalEmptyTopic);
		FlinkKafkaConsumerBase<Tuple2<Long, String>> source = kafkaServer.getConsumer(topics, sourceSchema, standardProps);

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
				kafkaServer,
				topic, parallelism, numElementsPerPartition, true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
		env.getConfig().disableSysoutLogging();

		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, standardProps);

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
				kafkaServer,
				topic, numPartitions, numElementsPerPartition, true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
		env.getConfig().disableSysoutLogging();

		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, standardProps);

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
				kafkaServer,
				topic, numPartitions, numElementsPerPartition, true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
		env.getConfig().disableSysoutLogging();
		env.setBufferTimeout(0);

		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, standardProps);

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
				new DataGenerators.InfiniteStringsGenerator(kafkaServer, topic);
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

					FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, new SimpleStringSchema(), standardProps);

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

		Throwable failueCause = jobError.get();
		if(failueCause != null) {
			failueCause.printStackTrace();
			Assert.fail("Test failed prematurely with: " + failueCause.getMessage());
		}

		// cancel
		JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));

		// wait for the program to be done and validate that we failed with the right exception
		runnerThread.join();

		failueCause = jobError.get();
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

					FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, new SimpleStringSchema(), standardProps);

					env.addSource(source).addSink(new DiscardingSink<String>());

					env.execute();
				}
				catch (Throwable t) {
					LOG.error("Job Runner failed with exception", t);
					error.set(t);
				}
			}
		};

		Thread runnerThread = new Thread(jobRunner, "program runner thread");
		runnerThread.start();

		// wait a bit before canceling
		Thread.sleep(2000);

		Throwable failueCause = error.get();
		if (failueCause != null) {
			failueCause.printStackTrace();
			Assert.fail("Test failed prematurely with: " + failueCause.getMessage());
		}
		// cancel
		JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));

		// wait for the program to be done and validate that we failed with the right exception
		runnerThread.join();

		failueCause = error.get();
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

		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, standardProps);

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
	 * Test producing and consuming into multiple topics
	 * @throws java.lang.Exception
	 */
	public void runProduceConsumeMultipleTopics() throws java.lang.Exception {
		final int NUM_TOPICS = 5;
		final int NUM_ELEMENTS = 20;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.getConfig().disableSysoutLogging();
		
		// create topics with content
		final List<String> topics = new ArrayList<>();
		for (int i = 0; i < NUM_TOPICS; i++) {
			final String topic = "topic-" + i;
			topics.add(topic);
			// create topic
			createTestTopic(topic, i + 1 /*partitions*/, 1);
		}
		// run first job, producing into all topics
		DataStream<Tuple3<Integer, Integer, String>> stream = env.addSource(new RichParallelSourceFunction<Tuple3<Integer, Integer, String>>() {

			@Override
			public void run(SourceContext<Tuple3<Integer, Integer, String>> ctx) throws Exception {
				int partition = getRuntimeContext().getIndexOfThisSubtask();

				for (int topicId = 0; topicId < NUM_TOPICS; topicId++) {
					for (int i = 0; i < NUM_ELEMENTS; i++) {
						ctx.collect(new Tuple3<>(partition, i, "topic-" + topicId));
					}
				}
			}

			@Override
			public void cancel() {
			}
		});

		Tuple2WithTopicSchema schema = new Tuple2WithTopicSchema(env.getConfig());

		stream.addSink(kafkaServer.getProducer("dummy", schema, standardProps, null));

		env.execute("Write to topics");

		// run second job consuming from multiple topics
		env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.getConfig().disableSysoutLogging();
		
		stream = env.addSource(kafkaServer.getConsumer(topics, schema, standardProps));

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

	private static class Tuple2WithTopicSchema implements KeyedDeserializationSchema<Tuple3<Integer, Integer, String>>,
			KeyedSerializationSchema<Tuple3<Integer, Integer, String>> {

		private final TypeSerializer<Tuple2<Integer, Integer>> ts;
		
		public Tuple2WithTopicSchema(ExecutionConfig ec) {
			ts = TypeInfoParser.<Tuple2<Integer, Integer>>parse("Tuple2<Integer, Integer>").createSerializer(ec);
		}

		@Override
		public Tuple3<Integer, Integer, String> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(message));
			Tuple2<Integer, Integer> t2 = ts.deserialize(in);
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

		@Override
		public byte[] serializeKey(Tuple3<Integer, Integer, String> element) {
			return null;
		}

		@Override
		public byte[] serializeValue(Tuple3<Integer, Integer, String> element) {
			ByteArrayOutputStream by = new ByteArrayOutputStream();
			DataOutputView out = new DataOutputViewStreamWrapper(by);
			try {
				ts.serialize(new Tuple2<>(element.f0, element.f1), out);
			} catch (IOException e) {
				throw new RuntimeException("Error" ,e);
			}
			return by.toByteArray();
		}

		@Override
		public String getTargetTopic(Tuple3<Integer, Integer, String> element) {
			return element.f2;
		}
	}

	/**
	 * Test Flink's Kafka integration also with very big records (30MB)
	 * see http://stackoverflow.com/questions/21020347/kafka-sending-a-15mb-message
	 *
	 */
	public void runBigRecordTestTopology() throws Exception {

		final String topic = "bigRecordTestTopic";
		final int parallelism = 1; // otherwise, the kafka mini clusters may run out of heap space

		createTestTopic(topic, parallelism, 1);

		final TypeInformation<Tuple2<Long, byte[]>> longBytesInfo = TypeInfoParser.parse("Tuple2<Long, byte[]>");

		final TypeInformationSerializationSchema<Tuple2<Long, byte[]>> serSchema =
				new TypeInformationSerializationSchema<>(longBytesInfo, new ExecutionConfig());

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(100);
		env.setParallelism(parallelism);

		// add consuming topology:
		Properties consumerProps = new Properties();
		consumerProps.putAll(standardProps);
		consumerProps.setProperty("fetch.message.max.bytes", Integer.toString(1024 * 1024 * 14));
		consumerProps.setProperty("max.partition.fetch.bytes", Integer.toString(1024 * 1024 * 14)); // for the new fetcher
		consumerProps.setProperty("queued.max.message.chunks", "1");

		FlinkKafkaConsumerBase<Tuple2<Long, byte[]>> source = kafkaServer.getConsumer(topic, serSchema, consumerProps);
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
		producerProps.setProperty("max.request.size", Integer.toString(1024 * 1024 * 14));
		producerProps.setProperty("retries", "3");
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
				int sevenMb = 1024 * 1024 * 7;

				while (running) {
					byte[] wl = new byte[sevenMb + rnd.nextInt(sevenMb)];
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

		stream.addSink(kafkaServer.getProducer(topic, new KeyedSerializationSchemaWrapper<>(serSchema), producerProps, null));

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
				kafkaServer,
				topic, parallelism, numElementsPerPartition, true);

		// find leader to shut down
		int leaderId = kafkaServer.getLeaderToShutDown(topic);

		LOG.info("Leader to shutdown {}", leaderId);


		// run the topology (the consumers must handle the failures)

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(parallelism);
		env.enableCheckpointing(500);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();


		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, standardProps);

		env
				.addSource(kafkaSource)
				.map(new PartitionValidatingMapper(parallelism, 1))
				.map(new BrokerKillingMapper<Integer>(leaderId, failAfterElements))
				.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		BrokerKillingMapper.killedLeaderBefore = false;
		tryExecute(env, "Broker failure once test");

		// start a new broker:
		kafkaServer.restartBroker(leaderId);
	}

	public void runKeyValueTest() throws Exception {
		final String topic = "keyvaluetest";
		createTestTopic(topic, 1, 1);
		final int ELEMENT_COUNT = 5000;

		// ----------- Write some data into Kafka -------------------

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());
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
		Properties producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings);
		producerProperties.setProperty("retries", "3");
		kvStream.addSink(kafkaServer.getProducer(topic, schema, producerProperties, null));
		env.execute("Write KV to Kafka");

		// ----------- Read the data again -------------------

		env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();


		KeyedDeserializationSchema<Tuple2<Long, PojoValue>> readSchema = new TypeInformationKeyValueSerializationSchema<>(Long.class, PojoValue.class, env.getConfig());

		DataStream<Tuple2<Long, PojoValue>> fromKafka = env.addSource(kafkaServer.getConsumer(topic, readSchema, standardProps));
		fromKafka.flatMap(new RichFlatMapFunction<Tuple2<Long,PojoValue>, Object>() {
			long counter = 0;
			@Override
			public void flatMap(Tuple2<Long, PojoValue> value, Collector<Object> out) throws Exception {
				// the elements should be in order.
				Assert.assertTrue("Wrong value " + value.f1.lat, value.f1.lat == counter );
				if (value.f1.lat % 2 == 0) {
					assertNull("key was not null", value.f0);
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


	/**
	 * Test delete behavior and metrics for producer
	 * @throws Exception
	 */
	public void runAllDeletesTest() throws Exception {
		final String topic = "alldeletestest";
		createTestTopic(topic, 1, 1);
		final int ELEMENT_COUNT = 300;

		// ----------- Write some data into Kafka -------------------

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		DataStream<Tuple2<byte[], PojoValue>> kvStream = env.addSource(new SourceFunction<Tuple2<byte[], PojoValue>>() {
			@Override
			public void run(SourceContext<Tuple2<byte[], PojoValue>> ctx) throws Exception {
				Random rnd = new Random(1337);
				for (long i = 0; i < ELEMENT_COUNT; i++) {
					final byte[] key = new byte[200];
					rnd.nextBytes(key);
					ctx.collect(new Tuple2<>(key, (PojoValue) null));
				}
			}
			@Override
			public void cancel() {
			}
		});

		TypeInformationKeyValueSerializationSchema<byte[], PojoValue> schema = new TypeInformationKeyValueSerializationSchema<>(byte[].class, PojoValue.class, env.getConfig());

		Properties producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings);
		producerProperties.setProperty("retries", "3");
		kvStream.addSink(kafkaServer.getProducer(topic, schema, producerProperties, null));

		JobExecutionResult result = env.execute("Write deletes to Kafka");

		Map<String, Object> accuResults = result.getAllAccumulatorResults();
		// there are 37 accumulator results in Kafka 0.9
		// and 34 in Kafka 0.8
		Assert.assertTrue("Not enough accumulators from Kafka Producer: " + accuResults.size(), accuResults.size() > 33);

		// ----------- Read the data again -------------------

		env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		DataStream<Tuple2<byte[], PojoValue>> fromKafka = env.addSource(kafkaServer.getConsumer(topic, schema, standardProps));

		fromKafka.flatMap(new RichFlatMapFunction<Tuple2<byte[], PojoValue>, Object>() {
			long counter = 0;
			@Override
			public void flatMap(Tuple2<byte[], PojoValue> value, Collector<Object> out) throws Exception {
				// ensure that deleted messages are passed as nulls
				assertNull(value.f1);
				counter++;
				if (counter == ELEMENT_COUNT) {
					// we got the right number of elements
					throw new SuccessException();
				}
			}
		});

		tryExecute(env, "Read deletes from Kafka");

		deleteTestTopic(topic);
	}

	/**
	 * Test that ensures that DeserializationSchema.isEndOfStream() is properly evaluated
	 * and that the metrics for the consumer are properly reported.
	 *
	 * @throws Exception
	 */
	public void runMetricsAndEndOfStreamTest() throws Exception {

		final int ELEMENT_COUNT = 300;
		final String topic = writeSequence("testEndOfStream", ELEMENT_COUNT, 1, 1);

		// read using custom schema
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env1.setParallelism(1);
		env1.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env1.getConfig().disableSysoutLogging();

		DataStream<Tuple2<Integer, Integer>> fromKafka = env1.addSource(kafkaServer.getConsumer(topic, new FixedNumberDeserializationSchema(ELEMENT_COUNT), standardProps));
		fromKafka.flatMap(new FlatMapFunction<Tuple2<Integer,Integer>, Void>() {
			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Void> out) throws Exception {
				// noop ;)
			}
		});

		JobExecutionResult result = tryExecute(env1, "Consume " + ELEMENT_COUNT + " elements from Kafka");

		Map<String, Object> accuResults = result.getAllAccumulatorResults();
		// kafka 0.9 consumer: 39 results
		if (kafkaServer.getVersion().equals("0.9")) {
			assertTrue("Not enough accumulators from Kafka Consumer: " + accuResults.size(), accuResults.size() > 38);
		}

		deleteTestTopic(topic);
	}

	public static class FixedNumberDeserializationSchema implements DeserializationSchema<Tuple2<Integer, Integer>> {
		
		final int finalCount;
		int count = 0;
		
		TypeInformation<Tuple2<Integer, Integer>> ti = TypeInfoParser.parse("Tuple2<Integer, Integer>");
		TypeSerializer<Tuple2<Integer, Integer>> ser = ti.createSerializer(new ExecutionConfig());

		public FixedNumberDeserializationSchema(int finalCount) {
			this.finalCount = finalCount;
		}

		@Override
		public Tuple2<Integer, Integer> deserialize(byte[] message) throws IOException {
			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(message));
			return ser.deserialize(in);
		}

		@Override
		public boolean isEndOfStream(Tuple2<Integer, Integer> nextElement) {
			return ++count >= finalCount;
		}

		@Override
		public TypeInformation<Tuple2<Integer, Integer>> getProducedType() {
			return ti;
		}
	}


	// ------------------------------------------------------------------------
	//  Reading writing test data sets
	// ------------------------------------------------------------------------

	protected void readSequence(StreamExecutionEnvironment env, Properties cc,
								final int sourceParallelism,
								final String topicName,
								final int valuesCount, final int startFrom) throws Exception {

		final int finalCount = valuesCount * sourceParallelism;

		final TypeInformation<Tuple2<Integer, Integer>> intIntTupleType = TypeInfoParser.parse("Tuple2<Integer, Integer>");

		final TypeInformationSerializationSchema<Tuple2<Integer, Integer>> deser =
				new TypeInformationSerializationSchema<>(intIntTupleType, env.getConfig());

		// create the consumer
		FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> consumer = kafkaServer.getConsumer(topicName, deser, cc);

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
				LOG.info("Received message {}, total {} messages", value, count);

				// verify if we've seen everything
				if (count == finalCount) {
					for (int i = 0; i < values.length; i++) {
						int v = values[i];
						if (v != sourceParallelism) {
							printTopic(topicName, valuesCount, deser);
							throw new RuntimeException("Expected v to be " + sourceParallelism + 
									", but was " + v + " on element " + i + " array=" + Arrays.toString(values));
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

	protected String writeSequence(
			String baseTopicName,
			final int numElements,
			final int parallelism,
			final int replicationFactor) throws Exception
	{
		LOG.info("\n===================================\n" +
				"== Writing sequence of " + numElements + " into " + baseTopicName + " with p=" + parallelism + "\n" +
				"===================================");

		final TypeInformation<Tuple2<Integer, Integer>> resultType = 
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {});

		final KeyedSerializationSchema<Tuple2<Integer, Integer>> serSchema =
				new KeyedSerializationSchemaWrapper<>(
						new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));

		final KeyedDeserializationSchema<Tuple2<Integer, Integer>> deserSchema =
				new KeyedDeserializationSchemaWrapper<>(
						new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));
		
		final int maxNumAttempts = 10;

		for (int attempt = 1; attempt <= maxNumAttempts; attempt++) {
			
			final String topicName = baseTopicName + '-' + attempt;
			
			LOG.info("Writing attempt #1");
			
			// -------- Write the Sequence --------
			
			createTestTopic(topicName, parallelism, replicationFactor);

			StreamExecutionEnvironment writeEnv = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
			writeEnv.getConfig().setRestartStrategy(RestartStrategies.noRestart());
			writeEnv.getConfig().disableSysoutLogging();
			
			DataStream<Tuple2<Integer, Integer>> stream = writeEnv.addSource(new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {
	
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
	
			// the producer must not produce duplicates
			Properties producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings);
			producerProperties.setProperty("retries", "0");
			
			stream.addSink(kafkaServer.getProducer(
							topicName, serSchema, producerProperties,
							new Tuple2Partitioner(parallelism)))
					.setParallelism(parallelism);

			try {
				writeEnv.execute("Write sequence");
			}
			catch (Exception e) {
				LOG.error("Write attempt failed, trying again", e);
				deleteTestTopic(topicName);
				JobManagerCommunicationUtils.waitUntilNoJobIsRunning(flink.getLeaderGateway(timeout));
				continue;
			}
			
			LOG.info("Finished writing sequence");

			// -------- Validate the Sequence --------
			
			// we need to validate the sequence, because kafka's producers are not exactly once
			LOG.info("Validating sequence");

			JobManagerCommunicationUtils.waitUntilNoJobIsRunning(flink.getLeaderGateway(timeout));
			
			final StreamExecutionEnvironment readEnv = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
			readEnv.getConfig().setRestartStrategy(RestartStrategies.noRestart());
			readEnv.getConfig().disableSysoutLogging();
			readEnv.setParallelism(parallelism);
			
			Properties readProps = (Properties) standardProps.clone();
			readProps.setProperty("group.id", "flink-tests-validator");
			
			FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> consumer = kafkaServer.getConsumer(topicName, deserSchema, readProps);

			readEnv
					.addSource(consumer)
					.map(new RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
						
						private final int totalCount = parallelism * numElements;
						private int count = 0;
						
						@Override
						public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
							if (++count == totalCount) {
								throw new SuccessException();
							} else {
								return value;
							}
						}
					}).setParallelism(1)
					.addSink(new DiscardingSink<Tuple2<Integer, Integer>>()).setParallelism(1);
			
			final AtomicReference<Throwable> errorRef = new AtomicReference<>();
			
			Thread runner = new Thread() {
				@Override
				public void run() {
					try {
						tryExecute(readEnv, "sequence validation");
					} catch (Throwable t) {
						errorRef.set(t);
					}
				}
			};
			runner.start();
			
			final long deadline = System.currentTimeMillis() + 10000;
			long delay;
			while (runner.isAlive() && (delay = deadline - System.currentTimeMillis()) > 0) {
				runner.join(delay);
			}
			
			boolean success;
			
			if (runner.isAlive()) {
				// did not finish in time, maybe the producer dropped one or more records and
				// the validation did not reach the exit point
				success = false;
				JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));
			}
			else {
				Throwable error = errorRef.get();
				if (error != null) {
					success = false;
					LOG.info("Attempt " + attempt + " failed with exception", error);
				}
				else {
					success = true;
				}
			}

			JobManagerCommunicationUtils.waitUntilNoJobIsRunning(flink.getLeaderGateway(timeout));
			
			if (success) {
				// everything is good!
				return topicName;
			}
			else {
				deleteTestTopic(topicName);
				// fall through the loop
			}
		}
		
		throw new Exception("Could not write a valid sequence to Kafka after " + maxNumAttempts + " attempts");
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
		Properties newProps = new Properties(standardProps);
		newProps.setProperty("group.id", "topic-printer"+ UUID.randomUUID().toString());
		newProps.setProperty("auto.offset.reset", "smallest");
		newProps.setProperty("zookeeper.connect", standardProps.getProperty("zookeeper.connect"));

		ConsumerConfig printerConfig = new ConsumerConfig(newProps);
		printTopic(topicName, printerConfig, deserializer, elements);
	}


	public static class BrokerKillingMapper<T> extends RichMapFunction<T,T>
			implements Checkpointed<Integer>, CheckpointListener {

		private static final long serialVersionUID = 6334389850158707313L;

		public static volatile boolean killedLeaderBefore;
		public static volatile boolean hasBeenCheckpointedBeforeFailure;
		
		private final int shutdownBrokerId;
		private final int failCount;
		private int numElementsTotal;

		private boolean failer;
		private boolean hasBeenCheckpointed;


		public BrokerKillingMapper(int shutdownBrokerId, int failCount) {
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
			
			if (!killedLeaderBefore) {
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

	/////////////			Testing the Kafka consumer with embeded watermark generation functionality			///////////////

//	@RetryOnException(times=0, exception=kafka.common.NotLeaderForPartitionException.class)
//	public void runExplicitPunctuatedWMgeneratingConsumerTest(boolean emptyPartition) throws Exception {
//
//		final String topic1 = "wmExtractorTopic1_" + UUID.randomUUID().toString();
//		final String topic2 = "wmExtractorTopic2_" + UUID.randomUUID().toString();
//
//		final Map<String, Boolean> topics = new HashMap<>();
//		topics.put(topic1, false);
//		topics.put(topic2, emptyPartition);
//
//		final int noOfTopcis = topics.size();
//		final int partitionsPerTopic = 1;
//		final int elementsPerPartition = 100 + 1;
//
//		final int totalElements = emptyPartition ?
//			partitionsPerTopic * elementsPerPartition :
//			noOfTopcis * partitionsPerTopic * elementsPerPartition;
//
//		createTestTopic(topic1, partitionsPerTopic, 1);
//		createTestTopic(topic2, partitionsPerTopic, 1);
//
//		final StreamExecutionEnvironment env =
//			StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		env.setParallelism(partitionsPerTopic);
//		env.setRestartStrategy(RestartStrategies.noRestart()); // fail immediately
//		env.getConfig().disableSysoutLogging();
//
//		TypeInformation<Tuple2<Long, Integer>> longIntType = TypeInfoParser.parse("Tuple2<Long, Integer>");
//
//		Properties producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings);
//		producerProperties.setProperty("retries", "0");
//
//		putDataInTopics(env, producerProperties, elementsPerPartition, topics, longIntType);
//
//		List<String> topicTitles = new ArrayList<>(topics.keySet());
//		runPunctuatedComsumer(env, topicTitles, totalElements, longIntType);
//
//		executeAndCatchException(env, "runComsumerWithPunctuatedExplicitWMTest");
//
//		for(String topic: topicTitles) {
//			deleteTestTopic(topic);
//		}
//	}

	private void executeAndCatchException(StreamExecutionEnvironment env, String execName) throws Exception {
		try {
			tryExecutePropagateExceptions(env, execName);
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
	}

	private void putDataInTopics(StreamExecutionEnvironment env,
								Properties producerProperties,
								final int elementsPerPartition,
								Map<String, Boolean> topics,
								TypeInformation<Tuple2<Long, Integer>> outputTypeInfo) {
		if(topics.size() != 2) {
			throw new RuntimeException("This method accepts two topics as arguments.");
		}

		TypeInformationSerializationSchema<Tuple2<Long, Integer>> sinkSchema =
			new TypeInformationSerializationSchema<>(outputTypeInfo, env.getConfig());

		DataStream<Tuple2<Long, Integer>> stream = env
			.addSource(new RichParallelSourceFunction<Tuple2<Long, Integer>>() {
				private boolean running = true;

				@Override
				public void run(SourceContext<Tuple2<Long, Integer>> ctx) throws InterruptedException {
					int topic = 0;
					int currentTs = 1;

					while (running && currentTs < elementsPerPartition) {
						long timestamp = (currentTs % 10 == 0) ? -1L : currentTs;
						ctx.collect(new Tuple2<Long, Integer>(timestamp, topic));
						currentTs++;
					}

					Tuple2<Long, Integer> toWrite2 = new Tuple2<Long, Integer>(-1L, topic);
					ctx.collect(toWrite2);
				}

				@Override
				public void cancel() {
				running = false;
			}
			}).setParallelism(1);

		List<Map.Entry<String, Boolean>> topicsL = new ArrayList<>(topics.entrySet());
		stream.map(new MapFunction<Tuple2<Long,Integer>, Tuple2<Long,Integer>>() {

			@Override
			public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
				return value;
			}
		}).setParallelism(1).addSink(kafkaServer.getProducer(topicsL.get(0).getKey(),
			new KeyedSerializationSchemaWrapper<>(sinkSchema), producerProperties, null)).setParallelism(1);

		if(!topicsL.get(1).getValue()) {
			stream.map(new MapFunction<Tuple2<Long,Integer>, Tuple2<Long,Integer>>() {

				@Override
				public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
					long timestamp = (value.f0 == -1) ? -1L : 1000 + value.f0;
					return new Tuple2<Long, Integer>(timestamp, 1);
				}
			}).setParallelism(1).addSink(kafkaServer.getProducer(topicsL.get(1).getKey(),
				new KeyedSerializationSchemaWrapper<>(sinkSchema), producerProperties, null)).setParallelism(1);
		}
	}

	private DataStreamSink<Tuple2<Long, Integer>> runPunctuatedComsumer(StreamExecutionEnvironment env,
																		List<String> topics,
																		final int totalElementsToExpect,
																		TypeInformation<Tuple2<Long, Integer>> inputTypeInfo) {

		TypeInformationSerializationSchema<Tuple2<Long, Integer>> sourceSchema =
			new TypeInformationSerializationSchema<>(inputTypeInfo, env.getConfig());

		FlinkKafkaConsumerBase<Tuple2<Long, Integer>> source = kafkaServer
			.getConsumer(topics, sourceSchema, standardProps)
			.setPunctuatedWatermarkEmitter(new TestPunctuatedTSExtractor());

		DataStreamSource<Tuple2<Long, Integer>> consuming = env.setParallelism(1).addSource(source);

		return consuming
			.transform("testingWatermarkOperator", inputTypeInfo, new WMTestingOperator())
			.addSink(new RichSinkFunction<Tuple2<Long, Integer>>() {

				private int elementCount = 0;

				@Override
				public void invoke(Tuple2<Long, Integer> value) throws Exception {
					elementCount++;
					if (elementCount == totalElementsToExpect) {
						throw new SuccessException();
					}
				}

				@Override
				public void close() throws Exception {
					super.close();
				}
			});
	}

	/** An extractor that emits a Watermark whenever the timestamp <b>in the record</b> is equal to {@code -1}. */
	private static class TestPunctuatedTSExtractor implements AssignerWithPunctuatedWatermarks<Tuple2<Long, Integer>> {

		@Override
		public Watermark checkAndGetNextWatermark(Tuple2<Long, Integer> lastElement, long extractedTimestamp) {
			return (lastElement.f0 == -1) ? new Watermark(extractedTimestamp) : null;
		}

		@Override
		public long extractTimestamp(Tuple2<Long, Integer> element, long previousElementTimestamp) {
			return element.f0;
		}
	}

	private static class WMTestingOperator extends AbstractStreamOperator<Tuple2<Long, Integer>> implements OneInputStreamOperator<Tuple2<Long, Integer>, Tuple2<Long, Integer>> {

		private long lastReceivedWatermark = Long.MIN_VALUE;

		private Map<Integer, Boolean> isEligible = new HashMap<>();
		private Map<Integer, Long> perPartitionMaxTs = new HashMap<>();

		WMTestingOperator() {
			isEligible = new HashMap<>();
			perPartitionMaxTs = new HashMap<>();
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Long, Integer>> element) throws Exception {
			int partition = element.getValue().f1;
			Long maxTs = perPartitionMaxTs.get(partition);
			if(maxTs == null || maxTs < element.getValue().f0) {
				perPartitionMaxTs.put(partition, element.getValue().f0);
				isEligible.put(partition, element.getValue().f0 > lastReceivedWatermark);
			}
			output.collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			int partition = -1;
			long minTS = Long.MAX_VALUE;
			for (Integer part : perPartitionMaxTs.keySet()) {
				Long ts = perPartitionMaxTs.get(part);
				if (ts < minTS && isEligible.get(part)) {
					partition = part;
					minTS = ts;
					lastReceivedWatermark = ts;
				}
			}
			isEligible.put(partition, false);

			assertEquals(minTS, mark.getTimestamp());
			output.emitWatermark(mark);
		}

		@Override
		public void close() throws Exception {
			super.close();
			perPartitionMaxTs.clear();
			isEligible.clear();
		}
	}
}
