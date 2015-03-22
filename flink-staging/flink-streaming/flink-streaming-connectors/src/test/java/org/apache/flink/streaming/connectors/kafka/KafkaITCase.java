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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.Properties;

import org.apache.curator.test.TestingServer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.Offset;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

/**
 * Code in this test is based on the following GitHub repository:
 * (as per commit bc6b2b2d5f6424d5f377aa6c0871e82a956462ef)
 * <p/>
 * https://github.com/sakserv/hadoop-mini-clusters (ASL licensed)
 */

public class KafkaITCase {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaITCase.class);

	private final String TOPIC = "myTopic";

	private int zkPort;
	private int kafkaPort;
	private String kafkaHost;

	private String zookeeperConnectionString;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	public File tmpZkDir;
	public File tmpKafkaDir;

	@Before
	public void prepare() throws IOException {
		tmpZkDir = tempFolder.newFolder();
		tmpKafkaDir = tempFolder.newFolder();
		kafkaHost = InetAddress.getLocalHost().getHostName();
		zkPort = NetUtils.getAvailablePort();
		kafkaPort = NetUtils.getAvailablePort();
		zookeeperConnectionString = "localhost:" + zkPort;
	}

	@Test
	public void test() {
		LOG.info("Starting KafkaITCase.test()");
		TestingServer zookeeper = null;
		KafkaServer broker1 = null;
		try {
			zookeeper = getZookeeper();
			broker1 = getKafkaServer(0);
			LOG.info("ZK and KafkaServer started. Creating test topic:");
			createTestTopic();

			LOG.info("Starting Kafka Topology in Flink:");
			startKafkaTopology();

			LOG.info("Test succeeded.");
		} catch (Throwable t) {
			LOG.warn("Test failed with exception", t);
			Assert.fail("Test failed with: " + t.getMessage());
		} finally {
			LOG.info("Shutting down all services");
			if (broker1 != null) {
				broker1.shutdown();
			}
			if (zookeeper != null) {
				try {
					zookeeper.stop();
				} catch (IOException e) {
					LOG.warn("ZK.stop() failed", e);
				}
			}
		}

	}

	private void createTestTopic() {
		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperConnectionString);
		kafkaTopicUtils.createTopic(TOPIC, 1, 1);
	}

	private void startKafkaTopology() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<String> consuming = env.addSource(
				new PersistentKafkaSource<String>(zookeeperConnectionString, TOPIC, new JavaDefaultStringSchema(), 5000, 100, Offset.FROM_BEGINNING));
		consuming.addSink(new SinkFunction<String>() {
			int elCnt = 0;
			int start = -1;
			BitSet validator = new BitSet(101);

			@Override
			public void invoke(String value) throws Exception {
				LOG.info("Got " + value);
				String[] sp = value.split("-");
				int v = Integer.parseInt(sp[1]);
				if (start == -1) {
					start = v;
				}
				Assert.assertFalse("Received tuple twice", validator.get(v - start));
				validator.set(v - start);
				elCnt++;
				if (elCnt == 100) {
					// check if everything in the bitset is set to true
					int nc;
					if ((nc = validator.nextClearBit(0)) != 100) {
						throw new RuntimeException("The bitset was not set to 1 on all elements. Next clear:" + nc + " Set: " + validator);
					}
					throw new SuccessException();
				}
			}
		});

		// add producing topology
		DataStream<String> stream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;

			@Override
			public void run(Collector<String> collector) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				while (running) {
					collector.collect("kafka-" + cnt++);
					try {
						Thread.sleep(100);
					} catch (InterruptedException ignored) {
					}
				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got chancel()");
				running = false;
			}
		});
		stream.addSink(new KafkaSink<String>(zookeeperConnectionString, TOPIC, new JavaDefaultStringSchema()));

		try {
			env.setDegreeOfParallelism(1);
			env.execute();
		} catch (JobExecutionException good) {
			Throwable t = good.getCause();
			int limit = 0;
			while (!(t instanceof SuccessException)) {
				t = t.getCause();
				if (limit++ == 20) {
					throw good;
				}
			}
		}
	}


	private TestingServer getZookeeper() throws Exception {
		return new TestingServer(zkPort, tmpZkDir);
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	private KafkaServer getKafkaServer(int brokerId) throws UnknownHostException {
		Properties kafkaProperties = new Properties();
		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", kafkaHost);
		kafkaProperties.put("port", Integer.toString(kafkaPort));
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpKafkaDir.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

		KafkaServer server = new KafkaServer(kafkaConfig, new LocalSystemTime());
		server.startup();
		return server;
	}

	public class LocalSystemTime implements Time {

		@Override
		public long milliseconds() {
			return System.currentTimeMillis();
		}

		public long nanoseconds() {
			return System.nanoTime();
		}

		@Override
		public void sleep(long ms) {
			try {
				Thread.sleep(ms);
			} catch (InterruptedException e) {
				LOG.warn("Interruption", e);
			}
		}

	}

	public static class SuccessException extends Exception {

		private static final long serialVersionUID = 1L;

	}

}
