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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.testutils.DataGenerators;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.PartitionValidatingMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.ThrottledMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.Tuple2FlinkPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.ValidatingExactlyOnceSink;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.testutils.junit.RetryOnException;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import kafka.server.KafkaServer;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import javax.annotation.Nullable;
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
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.connectors.kafka.testutils.ClusterCommunicationUtils.getRunningJobs;
import static org.apache.flink.streaming.connectors.kafka.testutils.ClusterCommunicationUtils.waitUntilJobIsRunning;
import static org.apache.flink.streaming.connectors.kafka.testutils.ClusterCommunicationUtils.waitUntilNoJobIsRunning;
import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract test base for all Kafka consumer tests.
 */
@SuppressWarnings("serial")
public abstract class KafkaConsumerTestBase extends KafkaTestBaseWithFlink {

	@Rule
	public RetryRule retryRule = new RetryRule();

	private ClusterClient<?> client;

	// ------------------------------------------------------------------------
	//  Common Test Preparation
	// ------------------------------------------------------------------------

	/**
	 * Makes sure that no job is on the JobManager any more from any previous tests that use
	 * the same mini cluster. Otherwise, missing slots may happen.
	 */
	@Before
	public void setClientAndEnsureNoJobIsLingering() throws Exception {
		client = flink.getClusterClient();
		waitUntilNoJobIsRunning(client);
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
	 * and a wrong broker was specified.
	 *
	 * @throws Exception
	 */
	public void runFailOnNoBrokerTest() throws Exception {
		try {
			Properties properties = new Properties();

			StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
			see.getConfig().disableSysoutLogging();
			see.setRestartStrategy(RestartStrategies.noRestart());
			see.setParallelism(1);

			// use wrong ports for the consumers
			properties.setProperty("bootstrap.servers", "localhost:80");
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
		} catch (JobExecutionException jee) {
			if (kafkaServer.getVersion().equals("0.9") ||
				kafkaServer.getVersion().equals("0.10") ||
				kafkaServer.getVersion().equals("0.11") ||
				kafkaServer.getVersion().equals("2.0")) {
				final Optional<TimeoutException> optionalTimeoutException = ExceptionUtils.findThrowable(jee, TimeoutException.class);
				assertTrue(optionalTimeoutException.isPresent());

				final TimeoutException timeoutException = optionalTimeoutException.get();
				assertEquals("Timeout expired while fetching topic metadata", timeoutException.getMessage());
			} else {
				final Optional<Throwable> optionalThrowable = ExceptionUtils.findThrowableWithMessage(jee, "Unable to retrieve any partitions");
				assertTrue(optionalThrowable.isPresent());
				assertTrue(optionalThrowable.get() instanceof RuntimeException);
			}
		}
	}

	/**
	 * Ensures that the committed offsets to Kafka are the offsets of "the next record to process".
	 */
	public void runCommitOffsetsToKafka() throws Exception {
		// 3 partitions with 50 records each (0-49, so the expected commit offset of each partition should be 50)
		final int parallelism = 3;
		final int recordsInEachPartition = 50;

		final String topicName = writeSequence("testCommitOffsetsToKafkaTopic", recordsInEachPartition, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
					if (!(t instanceof JobCancellationException)) {
						errorRef.set(t);
					}
				}
			}
		};
		runner.start();

		final Long l50 = 50L; // the final committed offset in Kafka should be 50
		final long deadline = 30_000_000_000L + System.nanoTime();

		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler();

		do {
			Long o1 = kafkaOffsetHandler.getCommittedOffset(topicName, 0);
			Long o2 = kafkaOffsetHandler.getCommittedOffset(topicName, 1);
			Long o3 = kafkaOffsetHandler.getCommittedOffset(topicName, 2);

			if (l50.equals(o1) && l50.equals(o2) && l50.equals(o3)) {
				break;
			}

			Thread.sleep(100);
		}
		while (System.nanoTime() < deadline);

		// cancel the job & wait for the job to finish
		client.cancel(Iterables.getOnlyElement(getRunningJobs(client))).get();
		runner.join();

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
	 * This test ensures that when the consumers retrieve some start offset from kafka (earliest, latest), that this offset
	 * is committed to Kafka, even if some partitions are not read.
	 *
	 * <p>Test:
	 * - Create 3 partitions
	 * - write 50 messages into each.
	 * - Start three consumers with auto.offset.reset='latest' and wait until they committed into Kafka.
	 * - Check if the offsets in Kafka are set to 50 for the three partitions
	 *
	 * <p>See FLINK-3440 as well
	 */
	public void runAutoOffsetRetrievalAndCommitToKafka() throws Exception {
		// 3 partitions with 50 records each (0-49, so the expected commit offset of each partition should be 50)
		final int parallelism = 3;
		final int recordsInEachPartition = 50;

		final String topicName = writeSequence("testAutoOffsetRetrievalAndCommitToKafkaTopic", recordsInEachPartition, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
					if (!(t instanceof JobCancellationException)) {
						errorRef.set(t);
					}
				}
			}
		};
		runner.start();

		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler();

		final Long l50 = 50L; // the final committed offset in Kafka should be 50
		final long deadline = 30_000_000_000L + System.nanoTime();
		do {
			Long o1 = kafkaOffsetHandler.getCommittedOffset(topicName, 0);
			Long o2 = kafkaOffsetHandler.getCommittedOffset(topicName, 1);
			Long o3 = kafkaOffsetHandler.getCommittedOffset(topicName, 2);

			if (l50.equals(o1) && l50.equals(o2) && l50.equals(o3)) {
				break;
			}

			Thread.sleep(100);
		}
		while (System.nanoTime() < deadline);

		// cancel the job & wait for the job to finish
		client.cancel(Iterables.getOnlyElement(getRunningJobs(client))).get();
		runner.join();

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
	 * This test ensures that when explicitly set to start from earliest record, the consumer
	 * ignores the "auto.offset.reset" behaviour as well as any committed group offsets in Kafka.
	 */
	public void runStartFromEarliestOffsets() throws Exception {
		// 3 partitions with 50 records each (0-49, so the expected commit offset of each partition should be 50)
		final int parallelism = 3;
		final int recordsInEachPartition = 50;

		final String topicName = writeSequence("testStartFromEarliestOffsetsTopic", recordsInEachPartition, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(parallelism);

		Properties readProps = new Properties();
		readProps.putAll(standardProps);
		readProps.setProperty("auto.offset.reset", "latest"); // this should be ignored

		// the committed offsets should be ignored
		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler();
		kafkaOffsetHandler.setCommittedOffset(topicName, 0, 23);
		kafkaOffsetHandler.setCommittedOffset(topicName, 1, 31);
		kafkaOffsetHandler.setCommittedOffset(topicName, 2, 43);

		readSequence(env, StartupMode.EARLIEST, null, null, readProps, parallelism, topicName, recordsInEachPartition, 0);

		kafkaOffsetHandler.close();
		deleteTestTopic(topicName);
	}

	/**
	 * This test ensures that when explicitly set to start from latest record, the consumer
	 * ignores the "auto.offset.reset" behaviour as well as any committed group offsets in Kafka.
	 */
	public void runStartFromLatestOffsets() throws Exception {
		// 50 records written to each of 3 partitions before launching a latest-starting consuming job
		final int parallelism = 3;
		final int recordsInEachPartition = 50;

		// each partition will be written an extra 200 records
		final int extraRecordsInEachPartition = 200;

		// all already existing data in the topic, before the consuming topology has started, should be ignored
		final String topicName = writeSequence("testStartFromLatestOffsetsTopic", recordsInEachPartition, parallelism, 1);

		// the committed offsets should be ignored
		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler();
		kafkaOffsetHandler.setCommittedOffset(topicName, 0, 23);
		kafkaOffsetHandler.setCommittedOffset(topicName, 1, 31);
		kafkaOffsetHandler.setCommittedOffset(topicName, 2, 43);

		// job names for the topologies for writing and consuming the extra records
		final String consumeExtraRecordsJobName = "Consume Extra Records Job";
		final String writeExtraRecordsJobName = "Write Extra Records Job";

		// serialization / deserialization schemas for writing and consuming the extra records
		final TypeInformation<Tuple2<Integer, Integer>> resultType =
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {});

		final KeyedSerializationSchema<Tuple2<Integer, Integer>> serSchema =
			new KeyedSerializationSchemaWrapper<>(
				new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));

		final KafkaDeserializationSchema<Tuple2<Integer, Integer>> deserSchema =
			new KafkaDeserializationSchemaWrapper<>(
				new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));

		// setup and run the latest-consuming job
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(parallelism);

		final Properties readProps = new Properties();
		readProps.putAll(standardProps);
		readProps.setProperty("auto.offset.reset", "earliest"); // this should be ignored

		FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> latestReadingConsumer =
			kafkaServer.getConsumer(topicName, deserSchema, readProps);
		latestReadingConsumer.setStartFromLatest();

		env
			.addSource(latestReadingConsumer).setParallelism(parallelism)
			.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Object>() {
				@Override
				public void flatMap(Tuple2<Integer, Integer> value, Collector<Object> out) throws Exception {
					if (value.f1 - recordsInEachPartition < 0) {
						throw new RuntimeException("test failed; consumed a record that was previously written: " + value);
					}
				}
			}).setParallelism(1)
			.addSink(new DiscardingSink<>());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
		final JobID consumeJobId = jobGraph.getJobID();

		final AtomicReference<Throwable> error = new AtomicReference<>();
		Thread consumeThread = new Thread(() -> {
			try {
				ClientUtils.submitJobAndWaitForResult(client, jobGraph, KafkaConsumerTestBase.class.getClassLoader());
			} catch (Throwable t) {
				if (!ExceptionUtils.findThrowable(t, JobCancellationException.class).isPresent()) {
					error.set(t);
				}
			}
		});
		consumeThread.start();

		// wait until the consuming job has started, to be extra safe
		waitUntilJobIsRunning(client);

		// setup the extra records writing job
		final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();

		env2.setParallelism(parallelism);

		DataStream<Tuple2<Integer, Integer>> extraRecordsStream = env2
			.addSource(new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {

				private boolean running = true;

				@Override
				public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
					int count = recordsInEachPartition; // the extra records should start from the last written value
					int partition = getRuntimeContext().getIndexOfThisSubtask();

					while (running && count < recordsInEachPartition + extraRecordsInEachPartition) {
						ctx.collect(new Tuple2<>(partition, count));
						count++;
					}
				}

				@Override
				public void cancel() {
					running = false;
				}
			});

		kafkaServer.produceIntoKafka(extraRecordsStream, topicName, serSchema, readProps, null);

		try {
			env2.execute(writeExtraRecordsJobName);
		}
		catch (Exception e) {
			throw new RuntimeException("Writing extra records failed", e);
		}

		// cancel the consume job after all extra records are written
		client.cancel(consumeJobId).get();
		consumeThread.join();

		kafkaOffsetHandler.close();
		deleteTestTopic(topicName);

		// check whether the consuming thread threw any test errors;
		// test will fail here if the consume job had incorrectly read any records other than the extra records
		final Throwable consumerError = error.get();
		if (consumerError != null) {
			throw new Exception("Exception in the consuming thread", consumerError);
		}
	}

	/**
	 * This test ensures that the consumer correctly uses group offsets in Kafka, and defaults to "auto.offset.reset"
	 * behaviour when necessary, when explicitly configured to start from group offsets.
	 *
	 * <p>The partitions and their committed group offsets are setup as:
	 * 	partition 0 --> committed offset 23
	 * 	partition 1 --> no commit offset
	 * 	partition 2 --> committed offset 43
	 *
	 * <p>When configured to start from group offsets, each partition should read:
	 * 	partition 0 --> start from offset 23, read to offset 49 (27 records)
	 * 	partition 1 --> default to "auto.offset.reset" (set to earliest), so start from offset 0, read to offset 49 (50 records)
	 * 	partition 2 --> start from offset 43, read to offset 49 (7 records)
	 */
	public void runStartFromGroupOffsets() throws Exception {
		// 3 partitions with 50 records each (offsets 0-49)
		final int parallelism = 3;
		final int recordsInEachPartition = 50;

		final String topicName = writeSequence("testStartFromGroupOffsetsTopic", recordsInEachPartition, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(parallelism);

		Properties readProps = new Properties();
		readProps.putAll(standardProps);
		readProps.setProperty("auto.offset.reset", "earliest");

		// the committed group offsets should be used as starting points
		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler();

		// only partitions 0 and 2 have group offsets committed
		kafkaOffsetHandler.setCommittedOffset(topicName, 0, 23);
		kafkaOffsetHandler.setCommittedOffset(topicName, 2, 43);

		Map<Integer, Tuple2<Integer, Integer>> partitionsToValueCountAndStartOffsets = new HashMap<>();
		partitionsToValueCountAndStartOffsets.put(0, new Tuple2<>(27, 23)); // partition 0 should read offset 23-49
		partitionsToValueCountAndStartOffsets.put(1, new Tuple2<>(50, 0)); // partition 1 should read offset 0-49
		partitionsToValueCountAndStartOffsets.put(2, new Tuple2<>(7, 43));	// partition 2 should read offset 43-49

		readSequence(env, StartupMode.GROUP_OFFSETS, null, null, readProps, topicName, partitionsToValueCountAndStartOffsets);

		kafkaOffsetHandler.close();
		deleteTestTopic(topicName);
	}

	/**
	 * This test ensures that the consumer correctly uses user-supplied specific offsets when explicitly configured to
	 * start from specific offsets. For partitions which a specific offset can not be found for, the starting position
	 * for them should fallback to the group offsets behaviour.
	 *
	 * <p>4 partitions will have 50 records with offsets 0 to 49. The supplied specific offsets map is:
	 * 	partition 0 --> start from offset 19
	 * 	partition 1 --> not set
	 * 	partition 2 --> start from offset 22
	 * 	partition 3 --> not set
	 * 	partition 4 --> start from offset 26 (this should be ignored because the partition does not exist)
	 *
	 * <p>The partitions and their committed group offsets are setup as:
	 * 	partition 0 --> committed offset 23
	 * 	partition 1 --> committed offset 31
	 * 	partition 2 --> committed offset 43
	 * 	partition 3 --> no commit offset
	 *
	 * <p>When configured to start from these specific offsets, each partition should read:
	 * 	partition 0 --> start from offset 19, read to offset 49 (31 records)
	 * 	partition 1 --> fallback to group offsets, so start from offset 31, read to offset 49 (19 records)
	 * 	partition 2 --> start from offset 22, read to offset 49 (28 records)
	 * 	partition 3 --> fallback to group offsets, but since there is no group offset for this partition,
	 * 	                will default to "auto.offset.reset" (set to "earliest"),
	 * 	                so start from offset 0, read to offset 49 (50 records)
	 */
	public void runStartFromSpecificOffsets() throws Exception {
		// 4 partitions with 50 records each (offsets 0-49)
		final int parallelism = 4;
		final int recordsInEachPartition = 50;

		final String topicName = writeSequence("testStartFromSpecificOffsetsTopic", recordsInEachPartition, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(parallelism);

		Properties readProps = new Properties();
		readProps.putAll(standardProps);
		readProps.setProperty("auto.offset.reset", "earliest"); // partition 3 should default back to this behaviour

		Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
		specificStartupOffsets.put(new KafkaTopicPartition(topicName, 0), 19L);
		specificStartupOffsets.put(new KafkaTopicPartition(topicName, 2), 22L);
		specificStartupOffsets.put(new KafkaTopicPartition(topicName, 4), 26L); // non-existing partition, should be ignored

		// only the committed offset for partition 1 should be used, because partition 1 has no entry in specific offset map
		KafkaTestEnvironment.KafkaOffsetHandler kafkaOffsetHandler = kafkaServer.createOffsetHandler();
		kafkaOffsetHandler.setCommittedOffset(topicName, 0, 23);
		kafkaOffsetHandler.setCommittedOffset(topicName, 1, 31);
		kafkaOffsetHandler.setCommittedOffset(topicName, 2, 43);

		Map<Integer, Tuple2<Integer, Integer>> partitionsToValueCountAndStartOffsets = new HashMap<>();
		partitionsToValueCountAndStartOffsets.put(0, new Tuple2<>(31, 19)); // partition 0 should read offset 19-49
		partitionsToValueCountAndStartOffsets.put(1, new Tuple2<>(19, 31)); // partition 1 should read offset 31-49
		partitionsToValueCountAndStartOffsets.put(2, new Tuple2<>(28, 22));	// partition 2 should read offset 22-49
		partitionsToValueCountAndStartOffsets.put(3, new Tuple2<>(50, 0));	// partition 3 should read offset 0-49

		readSequence(env, StartupMode.SPECIFIC_OFFSETS, specificStartupOffsets, null, readProps, topicName, partitionsToValueCountAndStartOffsets);

		kafkaOffsetHandler.close();
		deleteTestTopic(topicName);
	}

	/**
	 * This test ensures that the consumer correctly uses user-supplied timestamp when explicitly configured to
	 * start from timestamp.
	 *
	 * <p>The validated Kafka data is written in 2 steps: first, an initial 50 records is written to each partition.
	 * After that, another 30 records is appended to each partition. Before each step, a timestamp is recorded.
	 * For the validation, when the read job is configured to start from the first timestamp, each partition should start
	 * from offset 0 and read a total of 80 records. When configured to start from the second timestamp,
	 * each partition should start from offset 50 and read on the remaining 30 appended records.
	 */
	public void runStartFromTimestamp() throws Exception {
		// 4 partitions with 50 records each
		final int parallelism = 4;
		final int initialRecordsInEachPartition = 50;
		final int appendRecordsInEachPartition = 30;

		// attempt to create an appended test sequence, where the timestamp of writing the appended sequence
		// is assured to be larger than the timestamp of the original sequence.
		long firstTimestamp = System.currentTimeMillis();
		String topic = writeSequence("runStartFromTimestamp", initialRecordsInEachPartition, parallelism, 1);

		long secondTimestamp = 0;
		while (secondTimestamp <= firstTimestamp) {
			Thread.sleep(1000);
			secondTimestamp = System.currentTimeMillis();
		}
		writeAppendSequence(topic, initialRecordsInEachPartition, appendRecordsInEachPartition, parallelism);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(parallelism);

		Properties readProps = new Properties();
		readProps.putAll(standardProps);

		readSequence(env, StartupMode.TIMESTAMP, null, firstTimestamp,
			readProps, parallelism, topic, initialRecordsInEachPartition + appendRecordsInEachPartition, 0);
		readSequence(env, StartupMode.TIMESTAMP, null, secondTimestamp,
			readProps, parallelism, topic, appendRecordsInEachPartition, initialRecordsInEachPartition);

		deleteTestTopic(topic);
	}

	/**
	 * Ensure Kafka is working on both producer and consumer side.
	 * This executes a job that contains two Flink pipelines.
	 *
	 * <pre>
	 * (generator source) --> (kafka sink)-[KAFKA-TOPIC]-(kafka source) --> (validating sink)
	 * </pre>
	 *
	 * <p>We need to externally retry this test. We cannot let Flink's retry mechanism do it, because the Kafka producer
	 * does not guarantee exactly-once output. Hence a recovery would introduce duplicates that
	 * cause the test to fail.
	 *
	 * <p>This test also ensures that FLINK-3156 doesn't happen again:
	 *
	 * <p>The following situation caused a NPE in the FlinkKafkaConsumer
	 *
	 * <p>topic-1 <-- elements are only produced into topic1.
	 * topic-2
	 *
	 * <p>Therefore, this test is consuming as well from an empty topic.
	 *
	 */
	@RetryOnException(times = 2, exception = kafka.common.NotLeaderForPartitionException.class)
	public void runSimpleConcurrentProducerConsumerTopology() throws Exception {
		final String topic = "concurrentProducerConsumerTopic_" + UUID.randomUUID().toString();
		final String additionalEmptyTopic = "additionalEmptyTopic_" + UUID.randomUUID().toString();

		final int parallelism = 3;
		final int elementsPerPartition = 100;
		final int totalElements = parallelism * elementsPerPartition;

		createTestTopic(topic, parallelism, 2);
		createTestTopic(additionalEmptyTopic, parallelism, 1); // create an empty topic which will remain empty all the time

		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(500);
		env.setRestartStrategy(RestartStrategies.noRestart()); // fail immediately

		TypeInformation<Tuple2<Long, String>> longStringType =
				TypeInformation.of(new TypeHint<Tuple2<Long, String>>(){});

		TypeInformationSerializationSchema<Tuple2<Long, String>> sourceSchema =
				new TypeInformationSerializationSchema<>(longStringType, env.getConfig());

		TypeInformationSerializationSchema<Tuple2<Long, String>> sinkSchema =
				new TypeInformationSerializationSchema<>(longStringType, env.getConfig());

		// ----------- add producer dataflow ----------

		DataStream<Tuple2<Long, String>> stream = env.addSource(new RichParallelSourceFunction<Tuple2<Long, String>>() {

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
				StreamExecutionEnvironment.getExecutionEnvironment(),
				kafkaServer,
				topic,
				parallelism,
				numElementsPerPartition,
				true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

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
				StreamExecutionEnvironment.getExecutionEnvironment(),
				kafkaServer,
				topic,
				numPartitions,
				numElementsPerPartition,
				true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

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
				StreamExecutionEnvironment.getExecutionEnvironment(),
				kafkaServer,
				topic,
				numPartitions,
				numElementsPerPartition,
				true);

		// run the topology that fails and recovers

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		// set the number of restarts to one. The failing mapper will fail once, then it's only success exceptions.
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
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

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(100);

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, new SimpleStringSchema(), props);

		env.addSource(source).addSink(new DiscardingSink<String>());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
		final JobID jobId = jobGraph.getJobID();

		final Runnable jobRunner = () -> {
			try {
				ClientUtils.submitJobAndWaitForResult(client, jobGraph, KafkaConsumerTestBase.class.getClassLoader());
			} catch (Throwable t) {
				jobError.set(t);
			}
		};

		Thread runnerThread = new Thread(jobRunner, "program runner thread");
		runnerThread.start();

		// wait a bit before canceling
		Thread.sleep(2000);

		Throwable failueCause = jobError.get();
		if (failueCause != null) {
			failueCause.printStackTrace();
			Assert.fail("Test failed prematurely with: " + failueCause.getMessage());
		}

		// cancel
		client.cancel(jobId).get();

		// wait for the program to be done and validate that we failed with the right exception
		runnerThread.join();

		assertEquals(JobStatus.CANCELED, client.getJobStatus(jobId).get());

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

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(100);

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, new SimpleStringSchema(), props);

		env.addSource(source).addSink(new DiscardingSink<String>());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
		final JobID jobId = jobGraph.getJobID();

		final Runnable jobRunner = () -> {
			try {
				ClientUtils.submitJobAndWaitForResult(client, jobGraph, KafkaConsumerTestBase.class.getClassLoader());
			} catch (Throwable t) {
				LOG.error("Job Runner failed with exception", t);
				error.set(t);
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
		client.cancel(jobId).get();

		// wait for the program to be done and validate that we failed with the right exception
		runnerThread.join();

		assertEquals(JobStatus.CANCELED, client.getJobStatus(jobId).get());

		deleteTestTopic(topic);
	}

	/**
	 * Test producing and consuming into multiple topics.
	 * @throws Exception
	 */
	public void runProduceConsumeMultipleTopics(boolean useLegacySchema) throws Exception {
		final int numTopics = 5;
		final int numElements = 20;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// create topics with content
		final List<String> topics = new ArrayList<>();
		for (int i = 0; i < numTopics; i++) {
			final String topic = "topic-" + i;
			topics.add(topic);
			// create topic
			createTestTopic(topic, i + 1 /*partitions*/, 1);
		}

		// before FLINK-6078 the RemoteExecutionEnvironment set the parallelism to 1 as well
		env.setParallelism(1);

		// run first job, producing into all topics
		DataStream<Tuple3<Integer, Integer, String>> stream = env.addSource(new RichParallelSourceFunction<Tuple3<Integer, Integer, String>>() {

			@Override
			public void run(SourceContext<Tuple3<Integer, Integer, String>> ctx) throws Exception {
				int partition = getRuntimeContext().getIndexOfThisSubtask();

				for (int topicId = 0; topicId < numTopics; topicId++) {
					for (int i = 0; i < numElements; i++) {
						ctx.collect(new Tuple3<>(partition, i, "topic-" + topicId));
					}
				}
			}

			@Override
			public void cancel() {
			}
		});

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);

		if (useLegacySchema) {
			Tuple2WithTopicSchema schema = new Tuple2WithTopicSchema(env.getConfig());
			kafkaServer.produceIntoKafka(stream, "dummy", schema, props, null);
		} else {
			TestDeserializer schema = new TestDeserializer(env.getConfig());
			kafkaServer.produceIntoKafka(stream, "dummy", schema, props);
		}

		env.execute("Write to topics");

		// run second job consuming from multiple topics
		env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (useLegacySchema) {
			Tuple2WithTopicSchema schema = new Tuple2WithTopicSchema(env.getConfig());
			stream = env.addSource(kafkaServer.getConsumer(topics, schema, props));
		} else {
			TestDeserializer schema = new TestDeserializer(env.getConfig());
			stream = env.addSource(kafkaServer.getConsumer(topics, schema, props));
		}

		stream.flatMap(new FlatMapFunction<Tuple3<Integer, Integer, String>, Integer>() {
			Map<String, Integer> countPerTopic = new HashMap<>(numTopics);
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
					if (el.getValue() < numElements) {
						break; // not enough yet
					}
					if (el.getValue() > numElements) {
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
		for (int i = 0; i < numTopics; i++) {
			final String topic = "topic-" + i;
			deleteTestTopic(topic);
		}
	}

	/**
	 * Test Flink's Kafka integration also with very big records (30MB).
	 *
	 * <p>see http://stackoverflow.com/questions/21020347/kafka-sending-a-15mb-message
	 *
	 */
	public void runBigRecordTestTopology() throws Exception {

		final String topic = "bigRecordTestTopic";
		final int parallelism = 1; // otherwise, the kafka mini clusters may run out of heap space

		createTestTopic(topic, parallelism, 1);

		final TypeInformation<Tuple2<Long, byte[]>> longBytesInfo =
				TypeInformation.of(new TypeHint<Tuple2<Long, byte[]>>(){});

		final TypeInformationSerializationSchema<Tuple2<Long, byte[]>> serSchema =
				new TypeInformationSerializationSchema<>(longBytesInfo, new ExecutionConfig());

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.noRestart());
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
						throw new RuntimeException("There have been " + elCnt + " elements");
					}
				}
				if (elCnt > 10) {
					throw new RuntimeException("More than 10 elements seen: " + elCnt);
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
				StreamExecutionEnvironment.getExecutionEnvironment(),
				kafkaServer,
				topic, parallelism, numElementsPerPartition, true);

		// find leader to shut down
		int leaderId = kafkaServer.getLeaderToShutDown(topic);

		LOG.info("Leader to shutdown {}", leaderId);

		// run the topology (the consumers must handle the failures)

		DeserializationSchema<Integer> schema =
				new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(500);
		env.setRestartStrategy(RestartStrategies.noRestart());

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
		final int elementCount = 5000;

		// ----------- Write some data into Kafka -------------------

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());

		DataStream<Tuple2<Long, PojoValue>> kvStream = env.addSource(new SourceFunction<Tuple2<Long, PojoValue>>() {
			@Override
			public void run(SourceContext<Tuple2<Long, PojoValue>> ctx) throws Exception {
				Random rnd = new Random(1337);
				for (long i = 0; i < elementCount; i++) {
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

		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());

		KafkaDeserializationSchema<Tuple2<Long, PojoValue>> readSchema = new TypeInformationKeyValueSerializationSchema<>(Long.class, PojoValue.class, env.getConfig());

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		DataStream<Tuple2<Long, PojoValue>> fromKafka = env.addSource(kafkaServer.getConsumer(topic, readSchema, props));
		fromKafka.flatMap(new RichFlatMapFunction<Tuple2<Long, PojoValue>, Object>() {
			long counter = 0;
			@Override
			public void flatMap(Tuple2<Long, PojoValue> value, Collector<Object> out) throws Exception {
				// the elements should be in order.
				Assert.assertTrue("Wrong value " + value.f1.lat, value.f1.lat == counter);
				if (value.f1.lat % 2 == 0) {
					assertNull("key was not null", value.f0);
				} else {
					Assert.assertTrue("Wrong value " + value.f0, value.f0 == counter);
				}
				counter++;
				if (counter == elementCount) {
					// we got the right number of elements
					throw new SuccessException();
				}
			}
		});

		tryExecute(env, "Read KV from Kafka");

		deleteTestTopic(topic);
	}

	private static class PojoValue {
		public Date when;
		public long lon;
		public long lat;
		public PojoValue() {}
	}

	/**
	 * Test delete behavior and metrics for producer.
	 * @throws Exception
	 */
	public void runAllDeletesTest() throws Exception {
		final String topic = "alldeletestest";
		createTestTopic(topic, 1, 1);
		final int elementCount = 300;

		// ----------- Write some data into Kafka -------------------

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

		DataStream<Tuple2<byte[], PojoValue>> kvStream = env.addSource(new SourceFunction<Tuple2<byte[], PojoValue>>() {
			@Override
			public void run(SourceContext<Tuple2<byte[], PojoValue>> ctx) throws Exception {
				Random rnd = new Random(1337);
				for (long i = 0; i < elementCount; i++) {
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

		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

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
				if (counter == elementCount) {
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

		final int elementCount = 300;
		final String topic = writeSequence("testEndOfStream", elementCount, 1, 1);

		// read using custom schema
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
		env1.setParallelism(1);
		env1.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env1.getConfig().disableSysoutLogging();

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);

		DataStream<Tuple2<Integer, Integer>> fromKafka = env1.addSource(kafkaServer.getConsumer(topic, new FixedNumberDeserializationSchema(elementCount), props));
		fromKafka.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Void>() {
			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Void> out) throws Exception {
				// noop ;)
			}
		});

		tryExecute(env1, "Consume " + elementCount + " elements from Kafka");

		deleteTestTopic(topic);
	}

	/**
	 * Test that ensures that DeserializationSchema can emit multiple records via a Collector.
	 *
	 * @throws Exception
	 */
	public void runCollectingSchemaTest() throws Exception {

		final int elementCount = 20;
		final String topic = writeSequence("testCollectingSchema", elementCount, 1, 1);

		// read using custom schema
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
		env1.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env1.setParallelism(1);
		env1.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env1.getConfig().disableSysoutLogging();

		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);

		DataStream<Tuple2<Integer, String>> fromKafka = env1.addSource(
			kafkaServer.getConsumer(
				topic,
				new CollectingDeserializationSchema(elementCount),
				props)
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> element) {
						String string = element.f1;
						return Long.parseLong(string.substring(0, string.length() - 1));
					}
				})
		);
		fromKafka
			.keyBy(t -> t.f0)
			.process(new KeyedProcessFunction<Integer, Tuple2<Integer, String>, Void>() {
				private boolean registered = false;

				@Override
				public void processElement(
						Tuple2<Integer, String> value,
						Context ctx,
						Collector<Void> out) throws Exception {
					if (!registered) {
						ctx.timerService().registerEventTimeTimer(elementCount - 2);
						registered = true;
					}
				}

				@Override
				public void onTimer(
						long timestamp,
						OnTimerContext ctx,
						Collector<Void> out) throws Exception {
					throw new SuccessException();
				}
			});

		tryExecute(env1, "Consume " + elementCount + " elements from Kafka");

		deleteTestTopic(topic);
	}

	/**
	 * Test metrics reporting for consumer.
	 *
	 * @throws Exception
	 */
	public void runMetricsTest() throws Throwable {

		// create a stream with 5 topics
		final String topic = "metricsStream";
		createTestTopic(topic, 5, 1);

		final Tuple1<Throwable> error = new Tuple1<>(null);

		// start job writing & reading data.
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
		env1.setParallelism(1);
		env1.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env1.getConfig().disableSysoutLogging();
		env1.disableOperatorChaining(); // let the source read everything into the network buffers

		TypeInformationSerializationSchema<Tuple2<Integer, Integer>> schema = new TypeInformationSerializationSchema<>(
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}), env1.getConfig());

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

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env1.getStreamGraph());
		final JobID jobId = jobGraph.getJobID();

		Thread jobThread = new Thread(() -> {
			try {
				ClientUtils.submitJobAndWaitForResult(client, jobGraph, KafkaConsumerTestBase.class.getClassLoader());
			} catch (Throwable t) {
				if (!ExceptionUtils.findThrowable(t, JobCancellationException.class).isPresent()) {
					LOG.warn("Got exception during execution", t);
					error.f0 = t;
				}
			}
		});
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
					if ((long) offset >= 0) {
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
			client.cancel(jobId).get();
			// wait for the job to finish (it should due to the cancel command above)
			jobThread.join();
		}

		if (error.f0 != null) {
			throw error.f0;
		}

		deleteTestTopic(topic);
	}

	private static class CollectingDeserializationSchema implements KafkaDeserializationSchema<Tuple2<Integer, String>> {

		final int finalCount;

		TypeInformation<Tuple2<Integer, String>> ti = TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {});
		TypeSerializer<Tuple2<Integer, Integer>> ser =
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})
				.createSerializer(new ExecutionConfig());

		public CollectingDeserializationSchema(int finalCount) {
			this.finalCount = finalCount;
		}

		@Override
		public boolean isEndOfStream(Tuple2<Integer, String> nextElement) {
			return false;
		}

		@Override
		public Tuple2<Integer, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
			throw new UnsupportedOperationException("Should not be called");
		}

		@Override
		public void deserialize(
				ConsumerRecord<byte[], byte[]> message,
				Collector<Tuple2<Integer, String>> out) throws Exception {
			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(message.value()));
			Tuple2<Integer, Integer> tuple = ser.deserialize(in);
			out.collect(Tuple2.of(tuple.f0, tuple.f1 + "a"));
			out.collect(Tuple2.of(tuple.f0, tuple.f1 + "b"));
		}

		@Override
		public TypeInformation<Tuple2<Integer, String>> getProducedType() {
			return ti;
		}
	}

	private static class FixedNumberDeserializationSchema implements DeserializationSchema<Tuple2<Integer, Integer>> {

		final int finalCount;
		int count = 0;

		TypeInformation<Tuple2<Integer, Integer>> ti = TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){});
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
	protected void readSequence(final StreamExecutionEnvironment env,
								final StartupMode startupMode,
								final Map<KafkaTopicPartition, Long> specificStartupOffsets,
								final Long startupTimestamp,
								final Properties cc,
								final String topicName,
								final Map<Integer, Tuple2<Integer, Integer>> partitionsToValuesCountAndStartOffset) throws Exception {
		final int sourceParallelism = partitionsToValuesCountAndStartOffset.keySet().size();

		int finalCountTmp = 0;
		for (Map.Entry<Integer, Tuple2<Integer, Integer>> valuesCountAndStartOffset : partitionsToValuesCountAndStartOffset.entrySet()) {
			finalCountTmp += valuesCountAndStartOffset.getValue().f0;
		}
		final int finalCount = finalCountTmp;

		final TypeInformation<Tuple2<Integer, Integer>> intIntTupleType =
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){});

		final TypeInformationSerializationSchema<Tuple2<Integer, Integer>> deser =
			new TypeInformationSerializationSchema<>(intIntTupleType, env.getConfig());

		// create the consumer
		cc.putAll(secureProps);
		FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> consumer = kafkaServer.getConsumer(topicName, deser, cc);
		setKafkaConsumerOffset(startupMode, consumer, specificStartupOffsets, startupTimestamp);

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
	 * Variant of {@link KafkaConsumerTestBase#readSequence(StreamExecutionEnvironment, StartupMode, Map, Long, Properties, String, Map)} to
	 * expect reading from the same start offset and the same value count for all partitions of a single Kafka topic.
	 */
	protected void readSequence(final StreamExecutionEnvironment env,
								final StartupMode startupMode,
								final Map<KafkaTopicPartition, Long> specificStartupOffsets,
								final Long startupTimestamp,
								final Properties cc,
								final int sourceParallelism,
								final String topicName,
								final int valuesCount,
								final int startFrom) throws Exception {
		HashMap<Integer, Tuple2<Integer, Integer>> partitionsToValuesCountAndStartOffset = new HashMap<>();
		for (int i = 0; i < sourceParallelism; i++) {
			partitionsToValuesCountAndStartOffset.put(i, new Tuple2<>(valuesCount, startFrom));
		}
		readSequence(env, startupMode, specificStartupOffsets, startupTimestamp, cc, topicName, partitionsToValuesCountAndStartOffset);
	}

	protected void setKafkaConsumerOffset(final StartupMode startupMode,
										final FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> consumer,
										final Map<KafkaTopicPartition, Long> specificStartupOffsets,
										final Long startupTimestamp) {
		switch (startupMode) {
			case EARLIEST:
				consumer.setStartFromEarliest();
				break;
			case LATEST:
				consumer.setStartFromLatest();
				break;
			case SPECIFIC_OFFSETS:
				consumer.setStartFromSpecificOffsets(specificStartupOffsets);
				break;
			case GROUP_OFFSETS:
				consumer.setStartFromGroupOffsets();
				break;
			case TIMESTAMP:
				consumer.setStartFromTimestamp(startupTimestamp);
				break;
		}
	}

	protected String writeSequence(
			String baseTopicName,
			final int numElements,
			final int parallelism,
			final int replicationFactor) throws Exception {
		LOG.info("\n===================================\n" +
				"== Writing sequence of " + numElements + " into " + baseTopicName + " with p=" + parallelism + "\n" +
				"===================================");

		final TypeInformation<Tuple2<Integer, Integer>> resultType =
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {});

		final KeyedSerializationSchema<Tuple2<Integer, Integer>> serSchema =
				new KeyedSerializationSchemaWrapper<>(
						new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));

		final KafkaDeserializationSchema<Tuple2<Integer, Integer>> deserSchema =
				new KafkaDeserializationSchemaWrapper<>(
						new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));

		final int maxNumAttempts = 10;

		for (int attempt = 1; attempt <= maxNumAttempts; attempt++) {

			final String topicName = baseTopicName + '-' + attempt;

			LOG.info("Writing attempt #" + attempt);

			// -------- Write the Sequence --------

			createTestTopic(topicName, parallelism, replicationFactor);

			StreamExecutionEnvironment writeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
			writeEnv.getConfig().setRestartStrategy(RestartStrategies.noRestart());
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

			kafkaServer.produceIntoKafka(stream, topicName, serSchema, producerProperties, new Tuple2FlinkPartitioner(parallelism))
					.setParallelism(parallelism);

			try {
				writeEnv.execute("Write sequence");
			}
			catch (Exception e) {
				LOG.error("Write attempt failed, trying again", e);
				deleteTestTopic(topicName);
				waitUntilNoJobIsRunning(client);
				continue;
			}

			LOG.info("Finished writing sequence");

			// -------- Validate the Sequence --------

			// we need to validate the sequence, because kafka's producers are not exactly once
			LOG.info("Validating sequence");

			waitUntilNoJobIsRunning(client);

			if (validateSequence(topicName, parallelism, deserSchema, numElements)) {
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

	protected void writeAppendSequence(
			String topicName,
			final int originalNumElements,
			final int numElementsToAppend,
			final int parallelism) throws Exception {

		LOG.info("\n===================================\n" +
			"== Appending sequence of " + numElementsToAppend + " into " + topicName +
			"===================================");

		final TypeInformation<Tuple2<Integer, Integer>> resultType =
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {});

		final KeyedSerializationSchema<Tuple2<Integer, Integer>> serSchema =
			new KeyedSerializationSchemaWrapper<>(
				new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));

		final KafkaDeserializationSchema<Tuple2<Integer, Integer>> deserSchema =
			new KafkaDeserializationSchemaWrapper<>(
				new TypeInformationSerializationSchema<>(resultType, new ExecutionConfig()));

		// -------- Write the append sequence --------

		StreamExecutionEnvironment writeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		writeEnv.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		DataStream<Tuple2<Integer, Integer>> stream = writeEnv.addSource(new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {

			private boolean running = true;

			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
				int cnt = originalNumElements;
				int partition = getRuntimeContext().getIndexOfThisSubtask();

				while (running && cnt < numElementsToAppend + originalNumElements) {
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

		kafkaServer.produceIntoKafka(stream, topicName, serSchema, producerProperties, new Tuple2FlinkPartitioner(parallelism))
			.setParallelism(parallelism);

		try {
			writeEnv.execute("Write sequence");
		}
		catch (Exception e) {
			throw new Exception("Failed to append sequence to Kafka; append job failed.", e);
		}

		LOG.info("Finished writing append sequence");

		// we need to validate the sequence, because kafka's producers are not exactly once
		LOG.info("Validating sequence");
		while (!getRunningJobs(client).isEmpty()){
			Thread.sleep(50);
		}

		if (!validateSequence(topicName, parallelism, deserSchema, originalNumElements + numElementsToAppend)) {
			throw new Exception("Could not append a valid sequence to Kafka.");
		}
	}

	private boolean validateSequence(
			final String topic,
			final int parallelism,
			KafkaDeserializationSchema<Tuple2<Integer, Integer>> deserSchema,
			final int totalNumElements) throws Exception {

		final StreamExecutionEnvironment readEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		readEnv.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		readEnv.setParallelism(parallelism);

		Properties readProps = (Properties) standardProps.clone();
		readProps.setProperty("group.id", "flink-tests-validator");
		readProps.putAll(secureProps);
		FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> consumer = kafkaServer.getConsumer(topic, deserSchema, readProps);
		consumer.setStartFromEarliest();

		readEnv
			.addSource(consumer)
			.map(new RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

				private final int totalCount = parallelism * totalNumElements;
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
			.addSink(new DiscardingSink<>()).setParallelism(1);

		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(readEnv.getStreamGraph());
		final JobID jobId = jobGraph.getJobID();

		Thread runner = new Thread(() -> {
			try {
				ClientUtils.submitJobAndWaitForResult(client, jobGraph, KafkaConsumerTestBase.class.getClassLoader());
				tryExecute(readEnv, "sequence validation");
			} catch (Throwable t) {
				if (!ExceptionUtils.findThrowable(t, SuccessException.class).isPresent()) {
					errorRef.set(t);
				}
			}
		});
		runner.start();

		final long deadline = System.nanoTime() + 10_000_000_000L;
		long delay;
		while (runner.isAlive() && (delay = deadline - System.nanoTime()) > 0) {
			runner.join(delay / 1_000_000L);
		}

		boolean success;

		if (runner.isAlive()) {
			// did not finish in time, maybe the producer dropped one or more records and
			// the validation did not reach the exit point
			success = false;
			client.cancel(jobId).get();
		}
		else {
			Throwable error = errorRef.get();
			if (error != null) {
				success = false;
				LOG.info("Sequence validation job failed with exception", error);
			}
			else {
				success = true;
			}
		}

		waitUntilNoJobIsRunning(client);

		return success;
	}

	// ------------------------------------------------------------------------
	//  Debugging utilities
	// ------------------------------------------------------------------------

	private static class BrokerKillingMapper<T> extends RichMapFunction<T, T>
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

	private abstract static class AbstractTestDeserializer implements
			KafkaDeserializationSchema<Tuple3<Integer, Integer, String>> {

		protected final TypeSerializer<Tuple2<Integer, Integer>> ts;

		public AbstractTestDeserializer(ExecutionConfig ec) {
			ts = TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}).createSerializer(ec);
		}

		@Override
		public Tuple3<Integer, Integer, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(record.value()));
			Tuple2<Integer, Integer> t2 = ts.deserialize(in);
			return new Tuple3<>(t2.f0, t2.f1, record.topic());
		}

		@Override
		public boolean isEndOfStream(Tuple3<Integer, Integer, String> nextElement) {
			return false;
		}

		@Override
		public TypeInformation<Tuple3<Integer, Integer, String>> getProducedType() {
			return TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, String>>(){});
		}
	}

	private static class Tuple2WithTopicSchema extends AbstractTestDeserializer
			implements KeyedSerializationSchema<Tuple3<Integer, Integer, String>> {

		public Tuple2WithTopicSchema(ExecutionConfig ec) {
			super(ec);
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
				throw new RuntimeException("Error" , e);
			}
			return by.toByteArray();
		}

		@Override
		public String getTargetTopic(Tuple3<Integer, Integer, String> element) {
			return element.f2;
		}
	}

	private static class TestDeserializer extends AbstractTestDeserializer
			implements KafkaSerializationSchema<Tuple3<Integer, Integer, String>> {

		public TestDeserializer(ExecutionConfig ec) {
			super(ec);
		}

		@Override
		public ProducerRecord<byte[], byte[]> serialize(
				Tuple3<Integer, Integer, String> element, @Nullable Long timestamp) {
			ByteArrayOutputStream by = new ByteArrayOutputStream();
			DataOutputView out = new DataOutputViewStreamWrapper(by);
			try {
				ts.serialize(new Tuple2<>(element.f0, element.f1), out);
			} catch (IOException e) {
				throw new RuntimeException("Error" , e);
			}
			byte[] serializedValue = by.toByteArray();

			return new ProducerRecord<>(element.f2, serializedValue);
		}

	}
}
