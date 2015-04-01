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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.Properties;

import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.Offset;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Collector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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

	private static int zkPort;
	private static int kafkaPort;
	private static String kafkaHost;

	private static String zookeeperConnectionString;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();
	public static File tmpZkDir;
	public static File tmpKafkaDir;

	private static TestingServer zookeeper;
	private static KafkaServer broker1;


	@BeforeClass
	public static void prepare() throws IOException {
		LOG.info("Starting KafkaITCase.prepare()");
		tmpZkDir = tempFolder.newFolder();
		tmpKafkaDir = tempFolder.newFolder();
		kafkaHost = InetAddress.getLocalHost().getHostName();
		zkPort = NetUtils.getAvailablePort();
		kafkaPort = NetUtils.getAvailablePort();
		zookeeperConnectionString = "localhost:" + zkPort;

		zookeeper = null;
		broker1 = null;

		try {
			LOG.info("Starting Zookeeper");
			zookeeper = getZookeeper();
			LOG.info("Starting KafkaServer");
			broker1 = getKafkaServer(0);
			LOG.info("ZK and KafkaServer started.");
		} catch (Throwable t) {
			LOG.warn("Test failed with exception", t);
			Assert.fail("Test failed with: " + t.getMessage());
		}
	}

	@AfterClass
	public static void shutDownServices() {
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

	@Test
	public void regularKafkaSourceTest() throws Exception {
		LOG.info("Starting KafkaITCase.regularKafkaSourceTest()");

		String topic = "regularKafkaSourceTestTopic";
		createTestTopic(topic, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(
				new KafkaSource<Tuple2<Long, String>>(zookeeperConnectionString, topic, "myFlinkGroup", new TupleSerializationSchema(), 5000));
		consuming.addSink(new SinkFunction<Tuple2<Long, String>>() {
			int elCnt = 0;
			int start = -1;
			BitSet validator = new BitSet(101);

			@Override
			public void invoke(Tuple2<Long, String> value) throws Exception {
				LOG.info("Got " + value);
				String[] sp = value.f1.split("-");
				int v = Integer.parseInt(sp[1]);

				assertEquals(value.f0 - 1000, (long) v);

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
		DataStream<Tuple2<Long, String>> stream = env.addSource(new SourceFunction<Tuple2<Long, String>>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;

			@Override
			public void run(Collector<Tuple2<Long, String>> collector) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				while (running) {
					collector.collect(new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++));
					try {
						Thread.sleep(100);
					} catch (InterruptedException ignored) {
					}
				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got cancel()");
				running = false;
			}
		});
		stream.addSink(new KafkaSink<Tuple2<Long, String>>(zookeeperConnectionString, topic, new TupleSerializationSchema()));

		try {
			env.setParallelism(1);
			env.execute();
		} catch (JobExecutionException good) {
			Throwable t = good.getCause();
			int limit = 0;
			while (!(t instanceof SuccessException)) {
				t = t.getCause();
				if (limit++ == 20) {
					LOG.warn("Test failed with exception", good);
					Assert.fail("Test failed with: " + good.getMessage());
				}
			}
		}

		LOG.info("Finished KafkaITCase.regularKafkaSourceTest()");
	}

	@Test
	public void tupleTestTopology() throws Exception {
		LOG.info("Starting KafkaITCase.tupleTestTopology()");

		String topic = "tupleTestTopic";
		createTestTopic(topic, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(
				new PersistentKafkaSource<Tuple2<Long, String>>(zookeeperConnectionString, topic, new TupleSerializationSchema(), 5000, 100, Offset.FROM_BEGINNING));
		consuming.addSink(new SinkFunction<Tuple2<Long, String>>() {
			int elCnt = 0;
			int start = -1;
			BitSet validator = new BitSet(101);

			@Override
			public void invoke(Tuple2<Long, String> value) throws Exception {
				LOG.info("Got " + value);
				String[] sp = value.f1.split("-");
				int v = Integer.parseInt(sp[1]);

				assertEquals(value.f0 - 1000, (long) v);

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
		DataStream<Tuple2<Long, String>> stream = env.addSource(new SourceFunction<Tuple2<Long, String>>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;

			@Override
			public void run(Collector<Tuple2<Long, String>> collector) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				while (running) {
					collector.collect(new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++));
					try {
						Thread.sleep(100);
					} catch (InterruptedException ignored) {
					}
				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got cancel()");
				running = false;
			}
		});
		stream.addSink(new KafkaSink<Tuple2<Long, String>>(zookeeperConnectionString, topic, new TupleSerializationSchema()));

		try {
			env.setParallelism(1);
			env.execute();
		} catch (JobExecutionException good) {
			Throwable t = good.getCause();
			int limit = 0;
			while (!(t instanceof SuccessException)) {
				t = t.getCause();
				if (limit++ == 20) {
					LOG.warn("Test failed with exception", good);
					Assert.fail("Test failed with: " + good.getMessage());
				}
			}
		}

		LOG.info("Finished KafkaITCase.tupleTestTopology()");
	}

	private static boolean partitionerHasBeenCalled = false;

	@Test
	public void customPartitioningTestTopology() throws Exception {
		LOG.info("Starting KafkaITCase.customPartitioningTestTopology()");

		String topic = "customPartitioningTestTopic";
		
		createTestTopic(topic, 3);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(
				new PersistentKafkaSource<Tuple2<Long, String>>(zookeeperConnectionString, topic, new TupleSerializationSchema(), 5000, 100, Offset.FROM_BEGINNING));
		consuming.addSink(new SinkFunction<Tuple2<Long, String>>() {
			int start = -1;
			BitSet validator = new BitSet(101);

			boolean gotPartition1 = false;
			boolean gotPartition2 = false;
			boolean gotPartition3 = false;

			@Override
			public void invoke(Tuple2<Long, String> value) throws Exception {
				LOG.info("Got " + value);
				String[] sp = value.f1.split("-");
				int v = Integer.parseInt(sp[1]);

				assertEquals(value.f0 - 1000, (long) v);

				switch (v) {
					case 9:
						gotPartition1 = true;
						break;
					case 19:
						gotPartition2 = true;
						break;
					case 99:
						gotPartition3 = true;
						break;
				}

				if (start == -1) {
					start = v;
				}
				Assert.assertFalse("Received tuple twice", validator.get(v - start));
				validator.set(v - start);

				if (gotPartition1 && gotPartition2 && gotPartition3) {
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
		DataStream<Tuple2<Long, String>> stream = env.addSource(new SourceFunction<Tuple2<Long, String>>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;

			@Override
			public void run(Collector<Tuple2<Long, String>> collector) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				while (running) {
					collector.collect(new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++));
					try {
						Thread.sleep(100);
					} catch (InterruptedException ignored) {
					}
				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got cancel()");
				running = false;
			}
		});
		stream.addSink(new KafkaSink<Tuple2<Long, String>>(zookeeperConnectionString, topic, new TupleSerializationSchema(), new CustomPartitioner()));

		try {
			env.setParallelism(1);
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

			assertTrue(partitionerHasBeenCalled);
		}

		LOG.info("Finished KafkaITCase.customPartitioningTestTopology()");
	}

	/**
	 * This is for a topic with 3 partitions and Tuple2<Long, String>
	 */
	private static class CustomPartitioner implements SerializableKafkaPartitioner {

		@Override
		public int partition(Object key, int numPartitions) {
			partitionerHasBeenCalled = true;

			Tuple2<Long, String> tuple = (Tuple2<Long, String>) key;
			if (tuple.f0 < 10) {
				return 0;
			} else if (tuple.f0 < 20) {
				return 1;
			} else {
				return 2;
			}
		}
	}

	private static class TupleSerializationSchema implements DeserializationSchema<Tuple2<Long, String>>, SerializationSchema<Tuple2<Long, String>, byte[]> {

		@SuppressWarnings("unchecked")
		@Override
		public Tuple2<Long, String> deserialize(byte[] message) {
			Object deserializedObject = SerializationUtils.deserialize(message);
			return (Tuple2<Long, String>) deserializedObject;
		}

		@Override
		public byte[] serialize(Tuple2<Long, String> element) {
			return SerializationUtils.serialize(element);
		}

		@Override
		public boolean isEndOfStream(Tuple2<Long, String> nextElement) {
			return false;
		}

	}

	@Test
	public void simpleTestTopology() throws Exception {
		String topic = "simpleTestTopic";

		createTestTopic(topic, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<String> consuming = env.addSource(
				new PersistentKafkaSource<String>(zookeeperConnectionString, topic, new JavaDefaultStringSchema(), 5000, 100, Offset.FROM_BEGINNING));
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
				LOG.info("Source got cancel()");
				running = false;
			}
		});
		stream.addSink(new KafkaSink<String>(zookeeperConnectionString, topic, new JavaDefaultStringSchema()));

		try {
			env.setParallelism(1);
			env.execute();
		} catch (JobExecutionException good) {
			Throwable t = good.getCause();
			int limit = 0;
			while (!(t instanceof SuccessException)) {
				t = t.getCause();
				if (limit++ == 20) {
					LOG.warn("Test failed with exception", good);
					Assert.fail("Test failed with: " + good.getMessage());
				}
			}
		}
	}


	private void createTestTopic(String topic, int numberOfPartitions) {
		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperConnectionString);
		kafkaTopicUtils.createTopic(topic, numberOfPartitions, 1);
	}


	private static TestingServer getZookeeper() throws Exception {
		return new TestingServer(zkPort, tmpZkDir);
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	private static KafkaServer getKafkaServer(int brokerId) throws UnknownHostException {
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

	public static class LocalSystemTime implements Time {

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
