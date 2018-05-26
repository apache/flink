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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.InstantiationUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A class containing a special Kafka broker which has a log retention of only 250 ms.
 * This way, we can make sure our consumer is properly handling cases where we run into out of offset
 * errors
 */
@SuppressWarnings("serial")
public class KafkaShortRetentionTestBase implements Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaShortRetentionTestBase.class);

	protected static final int NUM_TMS = 1;

	protected static final int TM_SLOTS = 8;

	protected static final int PARALLELISM = NUM_TMS * TM_SLOTS;

	private static KafkaTestEnvironment kafkaServer;
	private static Properties standardProps;

	@ClassRule
	public static MiniClusterResource flink = new MiniClusterResource(
		new MiniClusterResource.MiniClusterResourceConfiguration(
			getConfiguration(),
			NUM_TMS,
			TM_SLOTS));

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	protected static Properties secureProps = new Properties();

	private static Configuration getConfiguration() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 16L);
		flinkConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");
		return flinkConfig;
	}

	@BeforeClass
	public static void prepare() throws ClassNotFoundException {
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting KafkaShortRetentionTestBase ");
		LOG.info("-------------------------------------------------------------------------");

		// dynamically load the implementation for the test
		Class<?> clazz = Class.forName("org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl");
		kafkaServer = (KafkaTestEnvironment) InstantiationUtil.instantiate(clazz);

		LOG.info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.getVersion());

		if (kafkaServer.isSecureRunSupported()) {
			secureProps = kafkaServer.getSecureProperties();
		}

		Properties specificProperties = new Properties();
		specificProperties.setProperty("log.retention.hours", "0");
		specificProperties.setProperty("log.retention.minutes", "0");
		specificProperties.setProperty("log.retention.ms", "250");
		specificProperties.setProperty("log.retention.check.interval.ms", "100");
		kafkaServer.prepare(kafkaServer.createConfig().setKafkaServerProperties(specificProperties));

		standardProps = kafkaServer.getStandardProperties();
	}

	@AfterClass
	public static void shutDownServices() throws Exception {
		kafkaServer.shutdown();

		secureProps.clear();
	}

	/**
	 * This test is concurrently reading and writing from a kafka topic.
	 * The job will run for a while
	 * In a special deserializationSchema, we make sure that the offsets from the topic
	 * are non-continuous (because the data is expiring faster than its consumed --> with auto.offset.reset = 'earliest', some offsets will not show up)
	 *
	 */
	private static boolean stopProducer = false;

	public void runAutoOffsetResetTest() throws Exception {
		final String topic = "auto-offset-reset-test";

		final int parallelism = 1;
		final int elementsPerPartition = 50000;

		Properties tprops = new Properties();
		tprops.setProperty("retention.ms", "250");
		kafkaServer.createTestTopic(topic, parallelism, 1, tprops);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.noRestart()); // fail immediately
		env.getConfig().disableSysoutLogging();

		// ----------- add producer dataflow ----------

		DataStream<String> stream = env.addSource(new RichParallelSourceFunction<String>() {

			private boolean running = true;

			@Override
			public void run(SourceContext<String> ctx) throws InterruptedException {
				int cnt = getRuntimeContext().getIndexOfThisSubtask() * elementsPerPartition;
				int limit = cnt + elementsPerPartition;

				while (running && !stopProducer && cnt < limit) {
					ctx.collect("element-" + cnt);
					cnt++;
					Thread.sleep(10);
				}
				LOG.info("Stopping producer");
			}

			@Override
			public void cancel() {
				running = false;
			}
		});
		Properties props = new Properties();
		props.putAll(standardProps);
		props.putAll(secureProps);
		kafkaServer.produceIntoKafka(stream, topic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), props, null);

		// ----------- add consumer dataflow ----------

		NonContinousOffsetsDeserializationSchema deserSchema = new NonContinousOffsetsDeserializationSchema();
		FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, deserSchema, props);

		DataStreamSource<String> consuming = env.addSource(source);
		consuming.addSink(new DiscardingSink<String>());

		tryExecute(env, "run auto offset reset test");

		kafkaServer.deleteTestTopic(topic);
	}

	private class NonContinousOffsetsDeserializationSchema implements KeyedDeserializationSchema<String> {
		private int numJumps;
		long nextExpected = 0;

		@Override
		public String deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
			if (offset != nextExpected) {
				numJumps++;
				nextExpected = offset;
				LOG.info("Registered now jump at offset {}", offset);
			}
			nextExpected++;
			try {
				Thread.sleep(10); // slow down data consumption to trigger log eviction
			} catch (InterruptedException e) {
				throw new RuntimeException("Stopping it");
			}
			return "";
		}

		@Override
		public boolean isEndOfStream(String nextElement) {
			if (numJumps >= 5) {
				// we saw 5 jumps and no failures --> consumer can handle auto.offset.reset
				stopProducer = true;
				return true;
			}
			return false;
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return Types.STRING;
		}
	}

	/**
	 * Ensure that the consumer is properly failing if "auto.offset.reset" is set to "none".
	 */
	public void runFailOnAutoOffsetResetNone() throws Exception {
		final String topic = "auto-offset-reset-none-test";
		final int parallelism = 1;

		kafkaServer.createTestTopic(topic, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.noRestart()); // fail immediately
		env.getConfig().disableSysoutLogging();

		// ----------- add consumer ----------

		Properties customProps = new Properties();
		customProps.putAll(standardProps);
		customProps.putAll(secureProps);
		customProps.setProperty("auto.offset.reset", "none"); // test that "none" leads to an exception
		FlinkKafkaConsumerBase<String> source = kafkaServer.getConsumer(topic, new SimpleStringSchema(), customProps);

		DataStreamSource<String> consuming = env.addSource(source);
		consuming.addSink(new DiscardingSink<String>());

		try {
			env.execute("Test auto offset reset none");
		} catch (Throwable e) {
			// check if correct exception has been thrown
			if (!e.getCause().getCause().getMessage().contains("Unable to find previous offset")  // kafka 0.8
				&& !e.getCause().getCause().getMessage().contains("Undefined offset with no reset policy for partition") // kafka 0.9
					) {
				throw e;
			}
		}

		kafkaServer.deleteTestTopic(topic);
	}

	public void runFailOnAutoOffsetResetNoneEager() throws Exception {
		final String topic = "auto-offset-reset-none-test";
		final int parallelism = 1;

		kafkaServer.createTestTopic(topic, parallelism, 1);

		// ----------- add consumer ----------

		Properties customProps = new Properties();
		customProps.putAll(standardProps);
		customProps.putAll(secureProps);
		customProps.setProperty("auto.offset.reset", "none"); // test that "none" leads to an exception

		try {
			kafkaServer.getConsumer(topic, new SimpleStringSchema(), customProps);
			fail("should fail with an exception");
		}
		catch (IllegalArgumentException e) {
			// expected
			assertTrue(e.getMessage().contains("none"));
		}

		kafkaServer.deleteTestTopic(topic);
	}
}
