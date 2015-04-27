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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.consumer.ConsumerConfig;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.Offset;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.util.KafkaLocalSystemTime;
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

/**
 * Code in this test is based on the following GitHub repository:
 * (as per commit bc6b2b2d5f6424d5f377aa6c0871e82a956462ef)
 * <p/>
 * https://github.com/sakserv/hadoop-mini-clusters (ASL licensed)
 */

public class KafkaITCase {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaITCase.class);
	private static final int NUMBER_OF_KAFKA_SERVERS = 3;

	private static int zkPort;
	private static String kafkaHost;

	private static String zookeeperConnectionString;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();
	public static File tmpZkDir;
	public static List<File> tmpKafkaDirs;

	private static TestingServer zookeeper;
	private static List<KafkaServer> brokers;

	private static boolean shutdownKafkaBroker;

	@BeforeClass
	public static void prepare() throws IOException {
		LOG.info("Starting KafkaITCase.prepare()");
		tmpZkDir = tempFolder.newFolder();

		tmpKafkaDirs = new ArrayList<File>(NUMBER_OF_KAFKA_SERVERS);
		for (int i = 0; i < NUMBER_OF_KAFKA_SERVERS; i++) {
			tmpKafkaDirs.add(tempFolder.newFolder());
		}

		kafkaHost = InetAddress.getLocalHost().getHostName();
		zkPort = NetUtils.getAvailablePort();
		zookeeperConnectionString = "localhost:" + zkPort;

		zookeeper = null;
		brokers = null;

		try {
			LOG.info("Starting Zookeeper");
			zookeeper = getZookeeper();
			LOG.info("Starting KafkaServer");
			brokers = new ArrayList<KafkaServer>(NUMBER_OF_KAFKA_SERVERS);
			for (int i = 0; i < NUMBER_OF_KAFKA_SERVERS; i++) {
				brokers.add(getKafkaServer(i, tmpKafkaDirs.get(i)));
			}

			LOG.info("ZK and KafkaServer started.");
		} catch (Throwable t) {
			LOG.warn("Test failed with exception", t);
			Assert.fail("Test failed with: " + t.getMessage());
		}
	}

	@AfterClass
	public static void shutDownServices() {
		LOG.info("Shutting down all services");
		for (KafkaServer broker : brokers) {
			if (broker != null) {
				broker.shutdown();
			}
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
		createTestTopic(topic, 1, 1);

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
				LOG.debug("Got value = " + value);
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
		createTestTopic(topic, 1, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(
				new PersistentKafkaSource<Tuple2<Long, String>>(zookeeperConnectionString, topic, new TupleSerializationSchema(), 5000, 100, Offset.FROM_BEGINNING));
		consuming.addSink(new RichSinkFunction<Tuple2<Long, String>>() {
			int elCnt = 0;
			int start = -1;
			BitSet validator = new BitSet(101);

			@Override
			public void invoke(Tuple2<Long, String> value) throws Exception {
				LOG.info("Got value " + value);
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

			@Override
			public void close() throws Exception {
				super.close();
				Assert.assertTrue("No element received", elCnt > 0);
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
					LOG.info("Produced " + cnt);

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

	/**
	 * Test Flink's Kafka integration also with very big records (30MB)
	 *
	 * see http://stackoverflow.com/questions/21020347/kafka-sending-a-15mb-message
	 *
	 * @throws Exception
	 */
	@Test
	public void bigRecordTestTopology() throws Exception {

		LOG.info("Starting KafkaITCase.bigRecordTestTopology()");

		String topic = "bigRecordTestTopic";
		createTestTopic(topic, 1, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		Utils.TypeInformationSerializationSchema<Tuple2<Long, byte[]>> serSchema = new Utils.TypeInformationSerializationSchema<Tuple2<Long, byte[]>>(new Tuple2<Long, byte[]>(0L, new byte[]{0}), env.getConfig());
		Properties consumerProps = new Properties();
		consumerProps.setProperty("fetch.message.max.bytes", Integer.toString(1024 * 1024 * 30));
		consumerProps.setProperty("zookeeper.connect", zookeeperConnectionString);
		consumerProps.setProperty("group.id", "test");

		ConsumerConfig cc = new ConsumerConfig(consumerProps);
		DataStreamSource<Tuple2<Long, byte[]>> consuming = env.addSource(
				new PersistentKafkaSource<Tuple2<Long, byte[]>>(topic, serSchema, Offset.FROM_BEGINNING, cc));

		consuming.addSink(new SinkFunction<Tuple2<Long, byte[]>>() {
			int elCnt = 0;

			@Override
			public void invoke(Tuple2<Long, byte[]> value) throws Exception {
				elCnt++;
				if(value.f0 == -1) {
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
		DataStream<Tuple2<Long, byte[]>> stream = env.addSource(new RichSourceFunction<Tuple2<Long, byte[]>>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
			}

			@Override
			public void run(Collector<Tuple2<Long, byte[]>> collector) throws Exception {
				LOG.info("Starting source.");
				long cnt = 0;
				Random rnd = new Random(1337);
				while (running) {
					//
					byte[] wl = new byte[Math.abs(rnd.nextInt(1024 * 1024 * 30))];
					collector.collect(new Tuple2<Long, byte[]>(cnt++, wl));
					LOG.info("Emitted cnt=" + (cnt - 1) + " with byte.length = " + wl.length);

					try {
						Thread.sleep(100);
					} catch (InterruptedException ignored) {
					}
					if(cnt == 10) {
						// signal end
						collector.collect(new Tuple2<Long, byte[]>(-1L, new byte[]{1}));
						running = false;
					}
				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got cancel()");
				running = false;
			}
		});

		stream.addSink(new KafkaSink<Tuple2<Long, byte[]>>(zookeeperConnectionString, topic,
				new Utils.TypeInformationSerializationSchema<Tuple2<Long, byte[]>>(new Tuple2<Long, byte[]>(0L, new byte[]{0}), env.getConfig()))
		);

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

		LOG.info("Finished KafkaITCase.bigRecordTestTopology()");
	}


	private static boolean partitionerHasBeenCalled = false;

	@Test
	public void customPartitioningTestTopology() throws Exception {
		LOG.info("Starting KafkaITCase.customPartitioningTestTopology()");

		String topic = "customPartitioningTestTopic";

		createTestTopic(topic, 3, 1);

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
				LOG.debug("Got " + value);
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

			@SuppressWarnings("unchecked")
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

		createTestTopic(topic, 1, 1);

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
				LOG.debug("Got " + value);
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

	private static boolean leaderHasShutDown = false;

	@Test
	public void brokerFailureTest() throws Exception {
		String topic = "brokerFailureTestTopic";

		createTestTopic(topic, 2, 2);

		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperConnectionString);
		final String leaderToShutDown = kafkaTopicUtils.waitAndGetPartitionMetadata(topic, 0).leader().get().connectionString();

		final Thread brokerShutdown = new Thread(new Runnable() {
			@Override
			public void run() {
				shutdownKafkaBroker = false;
				while (!shutdownKafkaBroker) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						LOG.warn("Interruption", e);
					}
				}

				for (KafkaServer kafkaServer : brokers) {
					if (leaderToShutDown.equals(
							kafkaServer.config().advertisedHostName()
									+ ":"
									+ kafkaServer.config().advertisedPort()
					)) {
						LOG.info("Killing Kafka Server {}", leaderToShutDown);
						kafkaServer.shutdown();
						leaderHasShutDown = true;
						break;
					}
				}
			}
		});
		brokerShutdown.start();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<String> consuming = env.addSource(
				new PersistentKafkaSource<String>(zookeeperConnectionString, topic, new JavaDefaultStringSchema(), 5000, 10, Offset.FROM_BEGINNING));
		consuming.setParallelism(1);

		consuming.addSink(new SinkFunction<String>() {
			int elCnt = 0;
			int start = 0;
			int numOfMessagesToBeCorrect = 100;
			int stopAfterMessages = 150;

			BitSet validator = new BitSet(numOfMessagesToBeCorrect + 1);

			@Override
			public void invoke(String value) throws Exception {
				LOG.info("Got message = " + value + " leader has shut down "+leaderHasShutDown+" el cnt = "+elCnt+" to rec"+ numOfMessagesToBeCorrect);
				String[] sp = value.split("-");
				int v = Integer.parseInt(sp[1]);

				if (start == -1) {
					start = v;
				}
				Assert.assertFalse("Received tuple twice", validator.get(v - start));
				if (v - start < 0 && LOG.isWarnEnabled()) {
					LOG.warn("Not in order: {}", value);
				}

				validator.set(v - start);
				elCnt++;
				if (elCnt == 20) {
					// shut down a Kafka broker
					shutdownKafkaBroker = true;
				}

				if(leaderHasShutDown) { // it only makes sence to check once the shutdown is completed
					if (elCnt >= stopAfterMessages ) {
						// check if everything in the bitset is set to true
						int nc;
						if ((nc = validator.nextClearBit(0)) < numOfMessagesToBeCorrect) {
							throw new RuntimeException("The bitset was not set to 1 on all elements to be checked. Next clear:" + nc + " Set: " + validator);
						}
						throw new SuccessException();
					}
				}
			}
		});

		// add producing topology
		DataStream<String> stream = env.addSource(new SourceFunction<String>() {
			boolean running = true;

			@Override
			public void run(Collector<String> collector) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				while (running) {
					String msg = "kafka-" + cnt++;
					collector.collect(msg);
					LOG.info("sending message = "+msg);

					if ((cnt - 1) % 20 == 0) {
						LOG.debug("Sending message #{}", cnt - 1);
					}

					try {
						Thread.sleep(10);
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
		stream.addSink(new KafkaSink<String>(zookeeperConnectionString, topic, new JavaDefaultStringSchema()))
				.setParallelism(1);

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


	private void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperConnectionString);
		kafkaTopicUtils.createTopic(topic, numberOfPartitions, replicationFactor);
	}

	private static TestingServer getZookeeper() throws Exception {
		return new TestingServer(zkPort, tmpZkDir);
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	private static KafkaServer getKafkaServer(int brokerId, File tmpFolder) throws UnknownHostException {
		Properties kafkaProperties = new Properties();

		int kafkaPort = NetUtils.getAvailablePort();

		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", kafkaHost);
		kafkaProperties.put("port", Integer.toString(kafkaPort));
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpFolder.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		kafkaProperties.put("message.max.bytes", "" + (35 * 1024 * 1024));
		kafkaProperties.put("replica.fetch.max.bytes", "" + (35 * 1024 * 1024));
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

		KafkaServer server = new KafkaServer(kafkaConfig, new KafkaLocalSystemTime());
		server.startup();
		return server;
	}

	public static class SuccessException extends Exception {
		private static final long serialVersionUID = 1L;
	}

}
