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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.testutils.DataGenerators;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.JobManagerCommunicationUtils;
import org.apache.flink.streaming.connectors.kafka.testutils.PartitionValidatingMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.ThrottledMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.Tuple2Partitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.ValidatingExactlyOnceSink;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.testutils.junit.RetryOnException;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
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
			properties.putAll(secureProps);
			FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer("doesntexist", new SimpleStringSchema(), properties);
			DataStream<String> stream = see.addSource(source);
			stream.print();
			see.execute("No broker test");
		} catch(ProgramInvocationException pie) {
			if(kafkaServer.getVersion().equals("0.9") || kafkaServer.getVersion().equals("0.10")) {
				assertTrue(pie.getCause() instanceof JobExecutionException);

				JobExecutionException jee = (JobExecutionException) pie.getCause();

				assertTrue(jee.getCause() instanceof TimeoutException);

				TimeoutException te = (TimeoutException) jee.getCause();

				assertEquals("Timeout expired while fetching topic metadata", te.getMessage());
			} else {
				assertTrue(pie.getCause() instanceof JobExecutionException);

				JobExecutionException jee = (JobExecutionException) pie.getCause();

				assertTrue(jee.getCause() instanceof RuntimeException);

				RuntimeException re = (RuntimeException) jee.getCause();

				assertTrue(re.getMessage().contains("Unable to retrieve any partitions for the requested topics [doesntexist]"));
			}
		}
	}

	/**
	 * Ensures that the committed offsets to Kafka are the offsets of "the next record to process"
	 */
	public void runCommitOffsetsToKafka() throws Exception {
		// 3 partitions with 50 records each (0-49, so the expected commit offset of each partition should be 50)
		final int parallelism = 3;
		final int recordsInEachPartition = 50;

		final String topicName = writeSequence("testCommitOffsetsToKafkaTopic", recordsInEachPartition, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.setParallelism(parallelism);
		env.enableCheckpointing(200);

		DataStream<String> stream = env.addSource(kafkaServer.getConsumer(topicName, new SimpleStringSchema(), standardProps));
		stream.addSink(new DiscardingSink<String>());

		final AtomicReference<Throwable> errorRef = new AtomicReference<>();
		final Thread runner = new Thread("runner") {
			@Override
			public void run() {
				try {
					env.execute();
				}
				catch (Throwable t) {
					if (!(t.getCause() instanceof JobCancellationException)) {
						errorRef.set(t);
					}
				}
			}
		};
		runner.start();

		final Long l50 = 50L; // the final committed offset in Kafka should be 50
		final long deadline = 30000 + System.currentTimeMillis();

		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler(standardProps);

		do {
			Long o1 = kafkaOffsetHandler.getCommittedOffset(topicName, 0);
			Long o2 = kafkaOffsetHandler.getCommittedOffset(topicName, 1);
			Long o3 = kafkaOffsetHandler.getCommittedOffset(topicName, 2);

			if (l50.equals(o1) && l50.equals(o2) && l50.equals(o3)) {
				break;
			}

			Thread.sleep(100);
		}
		while (System.currentTimeMillis() < deadline);

		// cancel the job
		JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));

		final Throwable t = errorRef.get();
		if (t != null) {
			throw new RuntimeException("Job failed with an exception", t);
		}

		// final check to see if offsets are correctly in Kafka
		Long o1 = kafkaOffsetHandler.getCommittedOffset(topicName, 0);
		Long o2 = kafkaOffsetHandler.getCommittedOffset(topicName, 1);
		Long o3 = kafkaOffsetHandler.getCommittedOffset(topicName, 2);
		Assert.assertEquals(Long.valueOf(50L), o1);
		Assert.assertEquals(Long.valueOf(50L), o2);
		Assert.assertEquals(Long.valueOf(50L), o3);

		kafkaOffsetHandler.close();
		deleteTestTopic(topicName);
	}

	/**
	 * This test first writes a total of 300 records to a test topic, reads the first 150 so that some offsets are
	 * committed to Kafka, and then startup the consumer again to read the remaining records starting from the committed offsets.
	 * The test ensures that whatever offsets were committed to Kafka, the consumer correctly picks them up
	 * and starts at the correct position.
	 */
	public void runStartFromKafkaCommitOffsets() throws Exception {
		final int parallelism = 3;
		final int recordsInEachPartition = 300;

		final String topicName = writeSequence("testStartFromKafkaCommitOffsetsTopic", recordsInEachPartition, parallelism, 1);

		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler(standardProps);

		Long o1;
		Long o2;
		Long o3;
		int attempt = 0;
		// make sure that o1, o2, o3 are not all null before proceeding
		do {
			attempt++;
			LOG.info("Attempt " + attempt + " to read records and commit some offsets to Kafka");

			final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
			env.getConfig().disableSysoutLogging();
			env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
			env.setParallelism(parallelism);
			env.enableCheckpointing(20); // fast checkpoints to make sure we commit some offsets

			env
				.addSource(kafkaServer.getConsumer(topicName, new SimpleStringSchema(), standardProps))
				.map(new ThrottledMapper<String>(50))
				.map(new MapFunction<String, Object>() {
					int count = 0;
					@Override
					public Object map(String value) throws Exception {
						count++;
						if (count == 150) {
							throw new SuccessException();
						}
						return null;
					}
				})
				.addSink(new DiscardingSink<>());

			tryExecute(env, "Read some records to commit offsets to Kafka");

			o1 = kafkaOffsetHandler.getCommittedOffset(topicName, 0);
			o2 = kafkaOffsetHandler.getCommittedOffset(topicName, 1);
			o3 = kafkaOffsetHandler.getCommittedOffset(topicName, 2);
		} while (o1 == null && o2 == null && o3 == null && attempt < 3);

		if (o1 == null && o2 == null && o3 == null) {
			throw new RuntimeException("No offsets have been committed after 3 attempts");
		}

		LOG.info("Got final committed offsets from Kafka o1={}, o2={}, o3={}", o1, o2, o3);

		final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env2.getConfig().disableSysoutLogging();
		env2.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env2.setParallelism(parallelism);

		// whatever offsets were committed for each partition, the consumer should pick
		// them up and start from the correct position so that the remaining records are all read
		HashMap<Integer, Tuple2<Integer, Integer>> partitionsToValuesCountAndStartOffset = new HashMap<>();
		partitionsToValuesCountAndStartOffset.put(0, new Tuple2<>(
			(o1 != null) ? (int) (recordsInEachPartition - o1) : recordsInEachPartition,
			(o1 != null) ? o1.intValue() : 0
		));
		partitionsToValuesCountAndStartOffset.put(1, new Tuple2<>(
			(o2 != null) ? (int) (recordsInEachPartition - o2) : recordsInEachPartition,
			(o2 != null) ? o2.intValue() : 0
		));
		partitionsToValuesCountAndStartOffset.put(2, new Tuple2<>(
			(o3 != null) ? (int) (recordsInEachPartition - o3) : recordsInEachPartition,
			(o3 != null) ? o3.intValue() : 0
		));

		readSequence(env2, standardProps, topicName, partitionsToValuesCountAndStartOffset);

		kafkaOffsetHandler.close();
		deleteTestTopic(topicName);
	}

	/**
	 * This test ensures that when the consumers retrieve some start offset from kafka (earliest, latest), that this offset
	 * is committed to Kafka, even if some partitions are not read.
	 *
	 * Test:
	 * - Create 3 partitions
	 * - write 50 messages into each.
	 * - Start three consumers with auto.offset.reset='latest' and wait until they committed into Kafka.
	 * - Check if the offsets in Kafka are set to 50 for the three partitions
	 *
	 * See FLINK-3440 as well
	 */
	public void runAutoOffsetRetrievalAndCommitToKafka() throws Exception {
		// 3 partitions with 50 records each (0-49, so the expected commit offset of each partition should be 50)
		final int parallelism = 3;
		final int recordsInEachPartition = 50;

		final String topicName = writeSequence("testAutoOffsetRetrievalAndCommitToKafkaTopic", recordsInEachPartition, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.setParallelism(parallelism);
		env.enableCheckpointing(200);

		Properties readProps = new Properties();
		readProps.putAll(standardProps);
		readProps.setProperty("auto.offset.reset", "latest"); // set to reset to latest, so that partitions are initially not read

		DataStream<String> stream = env.addSource(kafkaServer.getConsumer(topicName, new SimpleStringSchema(), readProps));
		stream.addSink(new DiscardingSink<String>());

		final AtomicReference<Throwable> errorRef = new AtomicReference<>();
		final Thread runner = new Thread("runner") {
			@Override
			public void run() {
				try {
					env.execute();
				}
				catch (Throwable t) {
					if (!(t.getCause() instanceof JobCancellationException)) {
						errorRef.set(t);
					}
				}
			}
		};
		runner.start();

		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler(standardProps);

		final Long l50 = 50L; // the final committed offset in Kafka should be 50
		final long deadline = 30000 + System.currentTimeMillis();
		do {
			Long o1 = kafkaOffsetHandler.getCommittedOffset(topicName, 0);
			Long o2 = kafkaOffsetHandler.getCommittedOffset(topicName, 1);
			Long o3 = kafkaOffsetHandler.getCommittedOffset(topicName, 2);

			if (l50.equals(o1) && l50.equals(o2) && l50.equals(o3)) {
				break;
			}

			Thread.sleep(100);
		}
		while (System.currentTimeMillis() < deadline);

		// cancel the job
		JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));

		final Throwable t = errorRef.get();
		if (t != null) {
			throw new RuntimeException("Job failed with an exception", t);
		}

		// final check to see if offsets are correctly in Kafka
		Long o1 = kafkaOffsetHandler.getCommittedOffset(topicName, 0);
		Long o2 = kafkaOffsetHandler.getCommittedOffset(topicName, 1);
		Long o3 = kafkaOffsetHandler.getCommittedOffset(topicName, 2);
		Assert.assertEquals(Long.valueOf(50L), o1);
		Assert.assertEquals(Long.valueOf(50L), o2);
		Assert.assertEquals(Long.valueOf(50L), o3);

		kafkaOffsetHandler.close();
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
		producerProperties.putAll(secureProps);
		kafkaServer.produceIntoKafka(stream, topic, new KeyedSerializationSchemaWrapper<>(sinkSchema), producerProperties, null);

		// ----------- add consumer dataflow ----------

		List<String> topics = new ArrayList<>();
		topics.add(topic);
		topics.add(additionalEmptyTopic);

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		FlinkKafkaConsumerBase<Tuple2<Long, String>> source = kafkaServer.getConsumer(topics, sourceSchema, props);

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
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.getConfig().disableSysoutLogging();

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);

		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, props);

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
				topic, numPartitions, numElementsPerPartition, false);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.getConfig().disableSysoutLogging();

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, props);

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
		// set the number of restarts to one. The failing mapper will fail once, then it's only success exceptions.
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.getConfig().disableSysoutLogging();
		env.setBufferTimeout(0);

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, props);

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

					Properties props = new Properties();
					props.putAll(standardProps);
					props.putAll(secureProps);
					FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, new SimpleStringSchema(), props);

					env.addSource(source).addSink(new DiscardingSink<String>());

					env.execute("Runner for CancelingOnFullInputTest");
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
		JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout), "Runner for CancelingOnFullInputTest");

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

					Properties props = new Properties();
					props.putAll(standardProps);
					props.putAll(secureProps);
					FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, new SimpleStringSchema(), props);

					env.addSource(source).addSink(new DiscardingSink<String>());

					env.execute("CancelingOnEmptyInputTest");
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

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, props);

		env
				.addSource(kafkaSource)
				.addSink(new DiscardingSink<Integer>());

		try {
			env.execute("test fail on deploy");
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

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		kafkaServer.produceIntoKafka(stream, "dummy", schema, props, null);

		env.execute("Write to topics");

		// run second job consuming from multiple topics
		env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.getConfig().disableSysoutLogging();

		stream = env.addSource(kafkaServer.getConsumer(topics, schema, props));

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

	/**
	 * Serialization scheme forwarding byte[] records.
	 */
	private static class ByteArraySerializationSchema implements KeyedSerializationSchema<byte[]> {

		@Override
		public byte[] serializeKey(byte[] element) {
			return null;
		}

		@Override
		public byte[] serializeValue(byte[] element) {
			return element;
		}

		@Override
		public String getTargetTopic(byte[] element) {
			return null;
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
		consumerProps.putAll(secureProps);

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
		producerProps.setProperty("max.request.size", Integer.toString(1024 * 1024 * 15));
		producerProps.setProperty("retries", "3");
		producerProps.putAll(secureProps);
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

		kafkaServer.produceIntoKafka(stream, topic, new KeyedSerializationSchemaWrapper<>(serSchema), producerProps, null);

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

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		FlinkKafkaConsumerBase<Integer> kafkaSource = kafkaServer.getConsumer(topic, schema, props);

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
		kafkaServer.produceIntoKafka(kvStream, topic, schema, producerProperties, null);
		env.execute("Write KV to Kafka");

		// ----------- Read the data again -------------------

		env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();


		KeyedDeserializationSchema<Tuple2<Long, PojoValue>> readSchema = new TypeInformationKeyValueSerializationSchema<>(Long.class, PojoValue.class, env.getConfig());

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		DataStream<Tuple2<Long, PojoValue>> fromKafka = env.addSource(kafkaServer.getConsumer(topic, readSchema, props));
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
		producerProperties.putAll(secureProps);
		kafkaServer.produceIntoKafka(kvStream, topic, schema, producerProperties, null);

		env.execute("Write deletes to Kafka");

		// ----------- Read the data again -------------------

		env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		DataStream<Tuple2<byte[], PojoValue>> fromKafka = env.addSource(kafkaServer.getConsumer(topic, schema, props));

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
	 * Test that ensures that DeserializationSchema.isEndOfStream() is properly evaluated.
	 *
	 * @throws Exception
	 */
	public void runEndOfStreamTest() throws Exception {

		final int ELEMENT_COUNT = 300;
		final String topic = writeSequence("testEndOfStream", ELEMENT_COUNT, 1, 1);

		// read using custom schema
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env1.setParallelism(1);
		env1.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env1.getConfig().disableSysoutLogging();

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);

		DataStream<Tuple2<Integer, Integer>> fromKafka = env1.addSource(kafkaServer.getConsumer(topic, new FixedNumberDeserializationSchema(ELEMENT_COUNT), props));
		fromKafka.flatMap(new FlatMapFunction<Tuple2<Integer,Integer>, Void>() {
			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Void> out) throws Exception {
				// noop ;)
			}
		});

		JobExecutionResult result = tryExecute(env1, "Consume " + ELEMENT_COUNT + " elements from Kafka");

		deleteTestTopic(topic);
	}

	/**
	 * Test metrics reporting for consumer
	 *
	 * @throws Exception
	 */
	public void runMetricsTest() throws Throwable {

		// create a stream with 5 topics
		final String topic = "metricsStream";
		createTestTopic(topic, 5, 1);

		final Tuple1<Throwable> error = new Tuple1<>(null);
		Runnable job = new Runnable() {
			@Override
			public void run() {
				try {
					// start job writing & reading data.
					final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
					env1.setParallelism(1);
					env1.getConfig().setRestartStrategy(RestartStrategies.noRestart());
					env1.getConfig().disableSysoutLogging();
					env1.disableOperatorChaining(); // let the source read everything into the network buffers

					Properties props = new Properties();
					props.putAll(standardProps);
					props.putAll(secureProps);

					TypeInformationSerializationSchema<Tuple2<Integer, Integer>> schema = new TypeInformationSerializationSchema<>(TypeInfoParser.<Tuple2<Integer, Integer>>parse("Tuple2<Integer, Integer>"), env1.getConfig());
					DataStream<Tuple2<Integer, Integer>> fromKafka = env1.addSource(kafkaServer.getConsumer(topic, schema, standardProps));
					fromKafka.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Void>() {
						@Override
						public void flatMap(Tuple2<Integer, Integer> value, Collector<Void> out) throws Exception {// no op
						}
					});

					DataStream<Tuple2<Integer, Integer>> fromGen = env1.addSource(new RichSourceFunction<Tuple2<Integer, Integer>>() {
						boolean running = true;

						@Override
						public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
							int i = 0;
							while (running) {
								ctx.collect(Tuple2.of(i++, getRuntimeContext().getIndexOfThisSubtask()));
								Thread.sleep(1);
							}
						}

						@Override
						public void cancel() {
							running = false;
						}
					});

					kafkaServer.produceIntoKafka(fromGen, topic, new KeyedSerializationSchemaWrapper<>(schema), standardProps, null);

					env1.execute("Metrics test job");
				} catch(Throwable t) {
					LOG.warn("Got exception during execution", t);
					if(!(t.getCause() instanceof JobCancellationException)) { // we'll cancel the job
						error.f0 = t;
					}
				}
			}
		};
		Thread jobThread = new Thread(job);
		jobThread.start();

		try {
			// connect to JMX
			MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
			// wait until we've found all 5 offset metrics
			Set<ObjectName> offsetMetrics = mBeanServer.queryNames(new ObjectName("*current-offsets*:*"), null);
			while (offsetMetrics.size() < 5) { // test will time out if metrics are not properly working
				if (error.f0 != null) {
					// fail test early
					throw error.f0;
				}
				offsetMetrics = mBeanServer.queryNames(new ObjectName("*current-offsets*:*"), null);
				Thread.sleep(50);
			}
			Assert.assertEquals(5, offsetMetrics.size());
			// we can't rely on the consumer to have touched all the partitions already
			// that's why we'll wait until all five partitions have a positive offset.
			// The test will fail if we never meet the condition
			while (true) {
				int numPosOffsets = 0;
				// check that offsets are correctly reported
				for (ObjectName object : offsetMetrics) {
					Object offset = mBeanServer.getAttribute(object, "Value");
					if((long) offset >= 0) {
						numPosOffsets++;
					}
				}
				if (numPosOffsets == 5) {
					break;
				}
				// wait for the consumer to consume on all partitions
				Thread.sleep(50);
			}

			// check if producer metrics are also available.
			Set<ObjectName> producerMetrics = mBeanServer.queryNames(new ObjectName("*KafkaProducer*:*"), null);
			Assert.assertTrue("No producer metrics found", producerMetrics.size() > 30);


			LOG.info("Found all JMX metrics. Cancelling job.");
		} finally {
			// cancel
			JobManagerCommunicationUtils.cancelCurrentJob(flink.getLeaderGateway(timeout));
		}

		while (jobThread.isAlive()) {
			Thread.sleep(50);
		}
		if (error.f0 != null) {
			throw error.f0;
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

	/**
	 * Runs a job using the provided environment to read a sequence of records from a single Kafka topic.
	 * The method allows to individually specify the expected starting offset and total read value count of each partition.
	 * The job will be considered successful only if all partition read results match the start offset and value count criteria.
	 */
	protected void readSequence(StreamExecutionEnvironment env, Properties cc,
								final String topicName,
								final Map<Integer, Tuple2<Integer, Integer>> partitionsToValuesCountAndStartOffset) throws Exception {
		final int sourceParallelism = partitionsToValuesCountAndStartOffset.keySet().size();

		int finalCountTmp = 0;
		for (Map.Entry<Integer, Tuple2<Integer, Integer>> valuesCountAndStartOffset : partitionsToValuesCountAndStartOffset.entrySet()) {
			finalCountTmp += valuesCountAndStartOffset.getValue().f0;
		}
		final int finalCount = finalCountTmp;

		final TypeInformation<Tuple2<Integer, Integer>> intIntTupleType = TypeInfoParser.parse("Tuple2<Integer, Integer>");

		final TypeInformationSerializationSchema<Tuple2<Integer, Integer>> deser =
			new TypeInformationSerializationSchema<>(intIntTupleType, env.getConfig());

		// create the consumer
		cc.putAll(secureProps);
		FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> consumer = kafkaServer.getConsumer(topicName, deser, cc);

		DataStream<Tuple2<Integer, Integer>> source = env
			.addSource(consumer).setParallelism(sourceParallelism)
			.map(new ThrottledMapper<Tuple2<Integer, Integer>>(20)).setParallelism(sourceParallelism);

		// verify data
		source.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Integer>() {

			private HashMap<Integer, BitSet> partitionsToValueCheck;
			private int count = 0;

			@Override
			public void open(Configuration parameters) throws Exception {
				partitionsToValueCheck = new HashMap<>();
				for (Integer partition : partitionsToValuesCountAndStartOffset.keySet()) {
					partitionsToValueCheck.put(partition, new BitSet());
				}
			}

			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Integer> out) throws Exception {
				int partition = value.f0;
				int val = value.f1;

				BitSet bitSet = partitionsToValueCheck.get(partition);
				if (bitSet == null) {
					throw new RuntimeException("Got a record from an unknown partition");
				} else {
					bitSet.set(val - partitionsToValuesCountAndStartOffset.get(partition).f1);
				}

				count++;

				LOG.info("Received message {}, total {} messages", value, count);

				// verify if we've seen everything
				if (count == finalCount) {
					for (Map.Entry<Integer, BitSet> partitionsToValueCheck : this.partitionsToValueCheck.entrySet()) {
						BitSet check = partitionsToValueCheck.getValue();
						int expectedValueCount = partitionsToValuesCountAndStartOffset.get(partitionsToValueCheck.getKey()).f0;

						if (check.cardinality() != expectedValueCount) {
							throw new RuntimeException("Expected cardinality to be " + expectedValueCount +
								", but was " + check.cardinality());
						} else if (check.nextClearBit(0) != expectedValueCount) {
							throw new RuntimeException("Expected next clear bit to be " + expectedValueCount +
								", but was " + check.cardinality());
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

	/**
	 * Variant of {@link KafkaConsumerTestBase#readSequence(StreamExecutionEnvironment, Properties, String, Map)} to
	 * expect reading from the same start offset and the same value count for all partitions of a single Kafka topic.
	 */
	protected void readSequence(StreamExecutionEnvironment env, Properties cc,
								final int sourceParallelism,
								final String topicName,
								final int valuesCount, final int startFrom) throws Exception {
		HashMap<Integer, Tuple2<Integer, Integer>> partitionsToValuesCountAndStartOffset = new HashMap<>();
		for (int i = 0; i < sourceParallelism; i++) {
			partitionsToValuesCountAndStartOffset.put(i, new Tuple2<>(valuesCount, startFrom));
		}
		readSequence(env, cc, topicName, partitionsToValuesCountAndStartOffset);
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
			producerProperties.putAll(secureProps);
			
			kafkaServer.produceIntoKafka(stream, topicName, serSchema, producerProperties, new Tuple2Partitioner(parallelism))
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
			readProps.putAll(secureProps);
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
		newProps.putAll(secureProps);

		ConsumerConfig printerConfig = new ConsumerConfig(newProps);
		printTopic(topicName, printerConfig, deserializer, elements);
	}


	public static class BrokerKillingMapper<T> extends RichMapFunction<T,T>
			implements ListCheckpointed<Integer>, CheckpointListener {

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
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.numElementsTotal);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.numElementsTotal = state.get(0);
		}
	}
}
