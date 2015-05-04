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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.network.SocketServer;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.util.KafkaLocalSystemTime;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;
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
import scala.collection.Seq;

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
	private static String brokerConnectionStrings = "";

	private static ConsumerConfig standardCC;

	private static ZkClient zkClient;


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
				SocketServer socketServer = brokers.get(i).socketServer();
				String host = "localhost";
				if(socketServer.host() != null) {
					host = socketServer.host();
				}
				brokerConnectionStrings += host+":"+socketServer.port()+",";
			}

			LOG.info("ZK and KafkaServer started.");
		} catch (Throwable t) {
			LOG.warn("Test failed with exception", t);
			Assert.fail("Test failed with: " + t.getMessage());
		}

		Properties cProps = new Properties();
		cProps.setProperty("zookeeper.connect", zookeeperConnectionString);
		cProps.setProperty("group.id", "flink-tests");
		cProps.setProperty("auto.commit.enable", "false");

		cProps.setProperty("auto.offset.reset", "smallest"); // read from the beginning.

		standardCC = new ConsumerConfig(cProps);

		zkClient = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new PersistentKafkaSource.KafkaZKStringSerializer());
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
		zkClient.close();
	}


	@Test
	public void testOffsetManipulation() {
		ZkClient zk = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new PersistentKafkaSource.KafkaZKStringSerializer());

		final String topicName = "testOffsetManipulation";

		// create topic
		Properties topicConfig = new Properties();
		LOG.info("Creating topic {}", topicName);
		AdminUtils.createTopic(zk, topicName, 3, 2, topicConfig);

		PersistentKafkaSource.setOffset(zk, standardCC.groupId(), topicName, 0, 1337);

		Assert.assertEquals(1337L, PersistentKafkaSource.getOffset(zk, standardCC.groupId(), topicName, 0));

		zk.close();
	}
	/**
	 * We want to use the High level java consumer API but manage the offset in Zookeeper manually.
	 *
	 */
	@Test
	public void testPersistentSourceWithOffsetUpdates() throws Exception {
		LOG.info("Starting testPersistentSourceWithOffsetUpdates()");

		ZkClient zk = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new PersistentKafkaSource.KafkaZKStringSerializer());

		final String topicName = "testOffsetHacking";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(3);
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(50);
		env.setNumberOfExecutionRetries(0);

		// create topic
		Properties topicConfig = new Properties();
		LOG.info("Creating topic {}", topicName);
		AdminUtils.createTopic(zk, topicName, 3, 2, topicConfig);

		// write a sequence from 0 to 99 to each of the three partitions.
		writeSequence(env, topicName, 0, 99);


		readSequence(env, standardCC, topicName, 0, 100, 300);

		// check offsets to be set at least higher than 50.
		// correctly, we would expect them to be set to 99, but right now there is no way of stopping a topology once all pending
		// checkpoints have been committed.
		// To work around that limitation, the persistent kafka consumer is throtteled with a thread.sleep().
		long o1 = PersistentKafkaSource.getOffset(zk, standardCC.groupId(), topicName, 0);
		long o2 = PersistentKafkaSource.getOffset(zk, standardCC.groupId(), topicName, 1);
		long o3 = PersistentKafkaSource.getOffset(zk, standardCC.groupId(), topicName, 2);
		Assert.assertTrue("The offset seems incorrect, got "+o1, o1 > 50L);
		Assert.assertTrue("The offset seems incorrect, got "+o2, o2 > 50L);
		Assert.assertTrue("The offset seems incorrect, got "+o3, o3 > 50L);
		/** Once we have proper shutdown of streaming jobs, enable these tests
		Assert.assertEquals("The offset seems incorrect", 99L, PersistentKafkaSource.getOffset(zk, standardCC.groupId(), topicName, 0));
		Assert.assertEquals("The offset seems incorrect", 99L, PersistentKafkaSource.getOffset(zk, standardCC.groupId(), topicName, 1));
		Assert.assertEquals("The offset seems incorrect", 99L, PersistentKafkaSource.getOffset(zk, standardCC.groupId(), topicName, 2));*/


		LOG.info("Manipulating offsets");
		// set the offset to 25, 50, and 75 for the three partitions
		PersistentKafkaSource.setOffset(zk, standardCC.groupId(), topicName, 0, 50);
		PersistentKafkaSource.setOffset(zk, standardCC.groupId(), topicName, 1, 50);
		PersistentKafkaSource.setOffset(zk, standardCC.groupId(), topicName, 2, 50);

		// create new env
		env = StreamExecutionEnvironment.createLocalEnvironment(3);
		env.getConfig().disableSysoutLogging();
		readSequence(env, standardCC, topicName, 50, 50, 150);

		zk.close();

		LOG.info("Finished testPersistentSourceWithOffsetUpdates()");
	}

	private void readSequence(StreamExecutionEnvironment env, ConsumerConfig cc, final String topicName, final int valuesStartFrom, final int valuesCount, final int finalCount) throws Exception {
		LOG.info("Reading sequence for verification until final count {}", finalCount);
		DataStream<Tuple2<Integer, Integer>> source = env.addSource(
				new PersistentKafkaSource<Tuple2<Integer, Integer>>(topicName, new Utils.TypeInformationSerializationSchema<Tuple2<Integer, Integer>>(new Tuple2<Integer, Integer>(1,1), env.getConfig()), cc)
		)
		//add a sleeper mapper. Since there is no good way of "shutting down" a running topology, we have
		// to play this trick. The problem is that we have to wait until all checkpoints are confirmed
		.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				Thread.sleep(150);
				return value;
			}
		}).setParallelism(3);

		// verify data
		DataStream<Integer> validIndexes = source.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Integer>() {
			int[] values = new int[valuesCount];
			int count = 0;

			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Integer> out) throws Exception {
				values[value.f1 - valuesStartFrom]++;
				count++;

				LOG.info("Reader " + getRuntimeContext().getIndexOfThisSubtask() + " got " + value + " count=" + count + "/" + finalCount);
				// verify if we've seen everything

				if (count == finalCount) {
					LOG.info("Received all values");
					for (int i = 0; i < values.length; i++) {
						int v = values[i];
						if (v != 3) {
							LOG.warn("Test is going to fail");
							printTopic(topicName, valuesCount, this.getRuntimeContext().getExecutionConfig());
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



	private void writeSequence(StreamExecutionEnvironment env, String topicName, final int from, final int to) throws Exception {
		LOG.info("Writing sequence from {} to {} to topic {}", from, to, topicName);

		DataStream<Tuple2<Integer, Integer>> stream = env.addSource(new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 1L;
			int cnt = from;
			int partition;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				partition = getRuntimeContext().getIndexOfThisSubtask();

			}

			@Override
			public boolean reachedEnd() throws Exception {
				return cnt > to;
			}

			@Override
			public Tuple2<Integer, Integer> next() throws Exception {
				LOG.info("Writing " + cnt + " to partition " + partition);
				Tuple2<Integer, Integer> result = new Tuple2<Integer, Integer>(getRuntimeContext().getIndexOfThisSubtask(), cnt);
				cnt++;
				return result;
			}
		}).setParallelism(3);

		stream.addSink(new KafkaSink<Tuple2<Integer, Integer>>(brokerConnectionStrings,
				topicName,
				new Utils.TypeInformationSerializationSchema<Tuple2<Integer, Integer>>(new Tuple2<Integer, Integer>(1, 1), env.getConfig()),
				new T2Partitioner()
		)).setParallelism(3);
		env.execute("Write sequence from " + from + " to " + to + " to topic " + topicName);
		LOG.info("Finished writing sequence");
	}

	private static class T2Partitioner implements SerializableKafkaPartitioner {
		@Override
		public int partition(Object key, int numPartitions) {
			if(numPartitions != 3) {
				throw new IllegalArgumentException("Expected three partitions");
			}
			Tuple2<Integer, Integer> element = (Tuple2<Integer, Integer>) key;
			return element.f0;
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
				new KafkaSource<Tuple2<Long, String>>(zookeeperConnectionString, topic, "myFlinkGroup", new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig()), 5000));
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
			int cnt = 0;

			@Override
			public boolean reachedEnd() throws Exception {
				return false;
			}

			@Override
			public Tuple2<Long, String> next() throws Exception {
				Thread.sleep(100);
				return new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++);
			}
		});
		stream.addSink(new KafkaSink<Tuple2<Long, String>>(brokerConnectionStrings, topic, new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig())));

		tryExecute(env, "regular kafka source test");

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
				new PersistentKafkaSource<Tuple2<Long, String>>(topic,
						new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig()),
						standardCC
				));
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
			int cnt = 0;

			@Override
			public boolean reachedEnd() throws Exception {
				return false;
			}

			@Override
			public Tuple2<Long, String> next() throws Exception {
				Thread.sleep(100);
				return new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++);
			}
		});
		stream.addSink(new KafkaSink<Tuple2<Long, String>>(brokerConnectionStrings, topic, new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig())));

		tryExecute(env, "tupletesttopology");

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
		consumerProps.setProperty("auto.commit.enable", "false");
		consumerProps.setProperty("auto.offset.reset", "smallest");

		ConsumerConfig cc = new ConsumerConfig(consumerProps);
		DataStreamSource<Tuple2<Long, byte[]>> consuming = env.addSource(
				new PersistentKafkaSource<Tuple2<Long, byte[]>>(topic, serSchema, cc));

		consuming.addSink(new SinkFunction<Tuple2<Long, byte[]>>() {
			int elCnt = 0;

			@Override
			public void invoke(Tuple2<Long, byte[]> value) throws Exception {
				LOG.info("Received {}", value.f0);
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
		}).setParallelism(1);

		// add producing topology
		DataStream<Tuple2<Long, byte[]>> stream = env.addSource(new RichSourceFunction<Tuple2<Long, byte[]>>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;
			long cnt;
			transient Random rnd;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				cnt = 0;
				rnd = new Random(1337);

			}

			@Override
			public boolean reachedEnd() throws Exception {
				return cnt > 10;
			}

			@Override
			public Tuple2<Long, byte[]> next() throws Exception {
				Thread.sleep(100);

				if (cnt < 10) {
				byte[] wl = new byte[Math.abs(rnd.nextInt(1024 * 1024 * 30))];
					Tuple2<Long, byte[]> result = new Tuple2<Long, byte[]>(cnt++, wl);
					LOG.info("Emitted cnt=" + (cnt - 1) + " with byte.length = " + wl.length);
					return result;

				} else if (cnt == 10) {
					Tuple2<Long, byte[]> result = new Tuple2<Long, byte[]>(-1L, new byte[]{1});
					cnt++;
					return result;
				} else {
					throw new RuntimeException("Source is exhausted.");
				}
			}
		});

		stream.addSink(new KafkaSink<Tuple2<Long, byte[]>>(brokerConnectionStrings, topic,
				new Utils.TypeInformationSerializationSchema<Tuple2<Long, byte[]>>(new Tuple2<Long, byte[]>(0L, new byte[]{0}), env.getConfig()))
		);

		tryExecute(env, "big topology test");

		LOG.info("Finished KafkaITCase.bigRecordTestTopology()");
	}


	@Test
	public void customPartitioningTestTopology() throws Exception {
		LOG.info("Starting KafkaITCase.customPartitioningTestTopology()");

		String topic = "customPartitioningTestTopic";

		createTestTopic(topic, 3, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(
				new PersistentKafkaSource<Tuple2<Long, String>>(topic,
						new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig()),
						standardCC));
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
			int cnt = 0;

			@Override
			public boolean reachedEnd() throws Exception {
				return false;
			}

			@Override
			public Tuple2<Long, String> next() throws Exception {
				Thread.sleep(100);
				return new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++);
			}
		});
		stream.addSink(new KafkaSink<Tuple2<Long, String>>(brokerConnectionStrings, topic, new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig()), new CustomPartitioner()));

		tryExecute(env, "custom partitioning test");

		LOG.info("Finished KafkaITCase.customPartitioningTestTopology()");
	}

	/**
	 * This is for a topic with 3 partitions and Tuple2<Long, String>
	 */
	private static class CustomPartitioner implements SerializableKafkaPartitioner {

		@Override
		public int partition(Object key, int numPartitions) {

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


	@Test
	public void simpleTestTopology() throws Exception {
		String topic = "simpleTestTopic";

		createTestTopic(topic, 1, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// add consuming topology:
		DataStreamSource<String> consuming = env.addSource(
				new PersistentKafkaSource<String>(topic, new JavaDefaultStringSchema(), standardCC));
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
			int cnt = 0;

			@Override
			public boolean reachedEnd() throws Exception {
				return false;
			}

			@Override
			public String next() throws Exception {
				Thread.sleep(100);
				return "kafka-" + cnt++;
			}
		});
		stream.addSink(new KafkaSink<String>(brokerConnectionStrings, topic, new JavaDefaultStringSchema()));

		tryExecute(env, "simpletest");
	}

	private static boolean leaderHasShutDown = false;
	private static boolean shutdownKafkaBroker;

	@Test(timeout=60000)
	public void brokerFailureTest() throws Exception {
		String topic = "brokerFailureTestTopic";

		createTestTopic(topic, 2, 2);

		// --------------------------- write data to topic ---------------------
		LOG.info("Writing data to topic {}", topic);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<String> stream = env.addSource(new SourceFunction<String>() {

			private int cnt = 0;

			@Override
			public boolean reachedEnd() throws Exception {
				return cnt == 200;
			}

			@Override
			public String next() throws Exception {
				String msg = "kafka-" + cnt++;
				LOG.info("sending message = "+msg);

				if ((cnt - 1) % 20 == 0) {
					LOG.debug("Sending message #{}", cnt - 1);
				}

				return msg;
			}

		});
		stream.addSink(new KafkaSink<String>(brokerConnectionStrings, topic, new JavaDefaultStringSchema()))
				.setParallelism(1);

		tryExecute(env, "broker failure test - writer");

		// --------------------------- read and let broker fail ---------------------

		LOG.info("Reading data from topic {} and let a broker fail", topic);
		PartitionMetadata firstPart = null;
		do {
			if(firstPart != null) {
				LOG.info("Unable to find leader. error code {}", firstPart.errorCode());
				// not the first try. Sleep a bit
				Thread.sleep(150);
			}
			Seq<PartitionMetadata> partitionMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient).partitionsMetadata();
			firstPart = partitionMetadata.head();
		} while(firstPart.errorCode() != 0);

		final String leaderToShutDown = firstPart.leader().get().connectionString();
		LOG.info("Leader to shutdown {}", leaderToShutDown);

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
					if (leaderToShutDown.equals(kafkaServer.config().advertisedHostName()+ ":"+ kafkaServer.config().advertisedPort())) {
						LOG.info("Killing Kafka Server {}", leaderToShutDown);
						kafkaServer.shutdown();
						leaderHasShutDown = true;
						break;
					}
				}
			}
		});
		brokerShutdown.start();

		// add consuming topology:
		DataStreamSource<String> consuming = env.addSource(new PersistentKafkaSource<String>(topic, new JavaDefaultStringSchema(), standardCC));
		consuming.setParallelism(1);

		consuming.addSink(new SinkFunction<String>() {
			int elCnt = 0;
			int start = 0;
			int numOfMessagesToBeCorrect = 100;
			int stopAfterMessages = 150;

			BitSet validator = new BitSet(numOfMessagesToBeCorrect + 1);

			@Override
			public void invoke(String value) throws Exception {
				LOG.info("Got message = " + value + " leader has shut down " + leaderHasShutDown + " el cnt = " + elCnt + " to rec" + numOfMessagesToBeCorrect);
				String[] sp = value.split("-");
				int v = Integer.parseInt(sp[1]);

				if (start == -1) {
					start = v;
				}
				int offset = v - start;
				Assert.assertFalse("Received tuple with value " + offset + " twice", validator.get(offset));
				if (v - start < 0 && LOG.isWarnEnabled()) {
					LOG.warn("Not in order: {}", value);
				}

				validator.set(offset);
				elCnt++;
				if (elCnt == 20) {
					LOG.info("Asking leading broker to shut down");
					// shut down a Kafka broker
					shutdownKafkaBroker = true;
				}
				if (shutdownKafkaBroker) {
					// we become a bit slower because the shutdown takes some time and we have
					// only a fixed nubmer of elements to read
					Thread.sleep(20);
				}
				if (leaderHasShutDown) { // it only makes sence to check once the shutdown is completed
					if (elCnt >= stopAfterMessages) {
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
		tryExecute(env, "broker failure test - reader");

	}

	public static void tryExecute(StreamExecutionEnvironment see, String name) throws Exception {
		try {
			see.execute(name);
		} catch (JobExecutionException good) {
			Throwable t = good.getCause();
			int limit = 0;
			while (!(t instanceof SuccessException)) {
				if(t == null) {
					LOG.warn("Test failed with exception", good);
					Assert.fail("Test failed with: " + good.getMessage());
				}

				t = t.getCause();
				if (limit++ == 20) {
					LOG.warn("Test failed with exception", good);
					Assert.fail("Test failed with: " + good.getMessage());
				}
			}
		}
	}

	private void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
		// create topic
		Properties topicConfig = new Properties();
		LOG.info("Creating topic {}", topic);
		AdminUtils.createTopic(zkClient, topic, numberOfPartitions, replicationFactor, topicConfig);
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


	// ----------------------- Debugging utilities --------------------

	/**
	 * Read topic to list, only using Kafka code.
	 * @return
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

	private static void printTopic(String topicName, ConsumerConfig config, DeserializationSchema deserializationSchema, int stopAfter){
		List<MessageAndMetadata<byte[], byte[]>> contents = readTopicToList(topicName, config, stopAfter);
		LOG.info("Printing contents of topic {} in consumer grouo {}", topicName, config.groupId());
		for(MessageAndMetadata<byte[], byte[]> message: contents) {
			Object out = deserializationSchema.deserialize(message.message());
			LOG.info("Message: partition: {} offset: {} msg: {}", message.partition(), message.offset(), out.toString());
		}
	}

	private static void printTopic(String topicName, int elements, ExecutionConfig ec) {
		// write the sequence to log for debugging purposes
		Properties stdProps = standardCC.props().props();
		Properties newProps = new Properties(stdProps);
		newProps.setProperty("group.id", "topic-printer"+UUID.randomUUID().toString());
		newProps.setProperty("auto.offset.reset", "smallest");
		newProps.setProperty("zookeeper.connect", standardCC.zkConnect());

		ConsumerConfig printerConfig = new ConsumerConfig(newProps);
		DeserializationSchema deserializer = new Utils.TypeInformationSerializationSchema<Tuple2<Integer, Integer>>(new Tuple2<Integer, Integer>(1,1), ec);
		printTopic(topicName, printerConfig, deserializer, elements);
	}

}
