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
package org.apache.flink.streaming.connectors.kinesis.manualtests;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.test.util.TestUtils.tryExecute;

/**
 * This test first starts a data generator, producing data into kinesis.
 * Then, it starts a consuming topology, ensuring that all records up to a certain
 * point have been seen.
 *
 * Invocation:
 * --region eu-central-1 --accessKey XXXXXXXXXXXX --secretKey XXXXXXXXXXXXXXXX
 */
public class ManualExactlyOnceTest {

	private static final Logger LOG = LoggerFactory.getLogger(ManualExactlyOnceTest.class);

	static final long TOTAL_EVENT_COUNT = 1000; // the producer writes one per 10 ms, so it runs for 10k ms = 10 seconds

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);
		LOG.info("Starting exactly once test");

		// create a stream for the test:
		Properties configProps = new Properties();
		configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, pt.getRequired("accessKey"));
		configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, pt.getRequired("secretKey"));
		AmazonKinesisClient client = new AmazonKinesisClient(AWSUtil.getCredentialsProvider(configProps).getCredentials());
		client.setRegion(Region.getRegion(Regions.fromName(pt.getRequired("region"))));
		final String streamName = "flink-test-" + UUID.randomUUID().toString();
		client.createStream(streamName, 1);
		// wait until stream has been created
		DescribeStreamResult status = client.describeStream(streamName);
		LOG.info("status {}" ,status);
		while(!status.getStreamDescription().getStreamStatus().equals("ACTIVE")) {
			status = client.describeStream(streamName);
			LOG.info("Status of stream {}", status);
			Thread.sleep(1000);
		}

		final Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 16);
		flinkConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");

		ForkableFlinkMiniCluster flink = new ForkableFlinkMiniCluster(flinkConfig, false);
		flink.start();

		final int flinkPort = flink.getLeaderRPCPort();

		try {
			final Tuple1<Throwable> producerException = new Tuple1<>();
			Runnable producer = new Runnable() {
				@Override
				public void run() {
					try {
						StreamExecutionEnvironment see = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort, flinkConfig);
						see.setParallelism(2);

						// start data generator
						DataStream<String> simpleStringStream = see.addSource(new EventsGenerator(TOTAL_EVENT_COUNT)).setParallelism(1);

						FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(pt.getRequired("region"),
								pt.getRequired("accessKey"),
								pt.getRequired("secretKey"),
								new SimpleStringSchema());

						kinesis.setFailOnError(true);
						kinesis.setDefaultStream(streamName);
						kinesis.setDefaultPartition("0");
						simpleStringStream.addSink(kinesis);

						LOG.info("Starting producing topology");
						see.execute("Producing topology");
						LOG.info("Producing topo finished");
					} catch (Exception e) {
						LOG.warn("Error while running producing topology", e);
						producerException.f0 = e;
					}
				}
			};
			Thread producerThread = new Thread(producer);
			producerThread.start();


			final Tuple1<Throwable> consumerException = new Tuple1<>();
			Runnable consumer = new Runnable() {
				@Override
				public void run() {
					try {
						StreamExecutionEnvironment see = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort, flinkConfig);
						see.setParallelism(2);
						see.enableCheckpointing(500);
						// we restart two times
						see.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 500L));

						// consuming topology
						Properties consumerProps = new Properties();
						consumerProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, pt.getRequired("accessKey"));
						consumerProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, pt.getRequired("secretKey"));
						consumerProps.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, pt.getRequired("region"));
						// start reading from beginning
						consumerProps.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, InitialPosition.TRIM_HORIZON.name());
						DataStream<String> consuming = see.addSource(new FlinkKinesisConsumer<>(streamName, new SimpleStringSchema(), consumerProps));
						consuming.flatMap(new RichFlatMapFunction<String, String>() {
							int count = 0;

							@Override
							public void flatMap(String value, Collector<String> out) throws Exception {
								if (count++ >= 200 && getRuntimeContext().getAttemptNumber() == 0) {
									throw new RuntimeException("Artificial failure. Restart pls");
								}
								out.collect(value);
							}
						}).flatMap(new ExactlyOnceValidatingMapper());
						// validate consumed records for correctness
						LOG.info("Starting consuming topology");
						tryExecute(see, "Consuming topo");
						LOG.info("Consuming topo finished");
					} catch (Exception e) {
						LOG.warn("Error while running consuming topology", e);
						consumerException.f0 = e;
					}
				}
			};

			Thread consumerThread = new Thread(consumer);
			consumerThread.start();

			long deadline = System.currentTimeMillis() + (1000 * 2 * 60); // wait at most for two minutes
			while (consumerThread.isAlive() || producerThread.isAlive()) {
				Thread.sleep(1000);
				if (System.currentTimeMillis() >= deadline) {
					LOG.warn("Deadline passed");
					break; // enough waiting
				}
			}

			if (producerThread.isAlive()) {
				producerThread.interrupt();
			}

			if (consumerThread.isAlive()) {
				consumerThread.interrupt();
			}

			if (producerException.f0 != null) {
				throw new RuntimeException("Producer failed", producerException.f0);
			}
			if (consumerException.f0 != null) {
				throw new RuntimeException("Consumer failed", consumerException.f0);
			}



			LOG.info("+++ TEST passed! +++");

		} finally {
			client.deleteStream(streamName);
			client.shutdown();

			// stopping flink
			flink.stop();
		}
	}

	// validate exactly once
	private static class ExactlyOnceValidatingMapper implements FlatMapFunction<String, String>, Checkpointed<BitSet> {
		BitSet validator = new BitSet((int)TOTAL_EVENT_COUNT);
		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			LOG.info("Consumed {}", value);

			int id = Integer.parseInt(value.split("-")[0]);
			if(validator.get(id)) {
				throw new RuntimeException("Saw id " + id +" twice!");
			}
			validator.set(id);
			if(id > 999) {
				throw new RuntimeException("Out of bounds ID observed");
			}

			if(validator.nextClearBit(0) == 1000) { // 0 - 1000 are set
				throw new SuccessException();
			}
		}

		@Override
		public BitSet snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return validator;
		}

		@Override
		public void restoreState(BitSet state) throws Exception {
			this.validator = state;
		}
	}

	public static class EventsGenerator implements SourceFunction<String> {
		private boolean running = true;
		private final long limit;

		public EventsGenerator(long limit) {
			this.limit = limit;
		}


		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			long seq = 0;
			while(running) {
				Thread.sleep(10);
				String evt = (seq++) + "-" + RandomStringUtils.randomAlphabetic(12);
				ctx.collect(evt);
				LOG.info("Emitting event {}", evt);
				if(seq >= limit) {
					break;
				}
			}
			ctx.close();
			LOG.info("Stopping events generator");
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
