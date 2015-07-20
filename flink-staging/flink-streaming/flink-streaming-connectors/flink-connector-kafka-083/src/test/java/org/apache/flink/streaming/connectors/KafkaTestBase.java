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

package org.apache.flink.streaming.connectors;

import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.internals.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;
import org.apache.flink.util.Collector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Code in this test is based on the following GitHub repository:
 * (as per commit bc6b2b2d5f6424d5f377aa6c0871e82a956462ef)
 * <p/>
 * https://github.com/sakserv/hadoop-mini-clusters (ASL licensed)
 */
public abstract class KafkaTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);
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
	private static Properties standardProps;

	private static ZkClient zkClient;

	// ----------------- Required methods by the abstract test base --------------------------

	abstract <T> FlinkKafkaConsumerBase<T> getConsumer(String topic, DeserializationSchema deserializationSchema, Properties props);
	abstract long[] getFinalOffsets();
	abstract void resetOffsets();

	// ----------------- Setup of Zookeeper and Kafka Brokers --------------------------

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
		cProps.setProperty("bootstrap.servers", brokerConnectionStrings);
		cProps.setProperty("group.id", "flink-tests");
		cProps.setProperty("auto.commit.enable", "false");
		cProps.setProperty("auto.offset.reset", "earliest"); // read from the beginning.

		cProps.setProperty("fetch.message.max.bytes", "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)

		standardProps = cProps;
		Properties consumerConfigProps = new Properties();
		consumerConfigProps.putAll(cProps);
		consumerConfigProps.setProperty("auto.offset.reset", "smallest");
		consumerConfigProps.setProperty("flink.kafka.consumer.queue.size", "1"); //this makes the behavior of the reader more predictable
		standardCC = new ConsumerConfig(consumerConfigProps);

		zkClient = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new FlinkKafkaConsumer081.KafkaZKStringSerializer());
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
		if(zkClient != null) {
			zkClient.close();
		}
	}


	// --------------------------  test checkpointing ------------------------
	@Test
	@Ignore("The test currently assumes a specific KafkaConsumer impl. For supporting the pluggable consumers, the test needs to behave like a regular source lifecycle")
	public void testCheckpointing() throws Exception {
		createTestTopic("testCheckpointing", 1, 1);

		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnectionString);
		props.setProperty("group.id", "testCheckpointing");
		props.setProperty("auto.commit.enable", "false");


		FlinkKafkaConsumerBase<String> source = getConsumer("testCheckpointing", new FakeDeserializationSchema(), standardProps);


		Field pendingCheckpointsField = FlinkKafkaConsumerBase.class.getDeclaredField("pendingCheckpoints");
		pendingCheckpointsField.setAccessible(true);
		LinkedMap pendingCheckpoints = (LinkedMap) pendingCheckpointsField.get(source);


		Assert.assertEquals(0, pendingCheckpoints.size());
		MockRuntimeContext mockCtx = new MockRuntimeContext();
		source.setRuntimeContext(mockCtx);
		mockCtx.indexOfThisSubtask = 0;
		mockCtx.numberOfParallelSubtasks = 1;

		// first restore
		source.restoreState(new long[]{1337});
		// then open
		source.open(new Configuration());
		long[] state1 = source.snapshotState(1, 15);
		Assert.assertArrayEquals(new long[]{1337}, state1);
		long[] state2 = source.snapshotState(2, 30);
		Assert.assertArrayEquals(new long[]{1337}, state2);
		Assert.assertEquals(2, pendingCheckpoints.size());

		source.notifyCheckpointComplete(1);
		Assert.assertEquals(1, pendingCheckpoints.size());

		source.notifyCheckpointComplete(2);
		Assert.assertEquals(0, pendingCheckpoints.size());

		source.notifyCheckpointComplete(666); // invalid checkpoint
		Assert.assertEquals(0, pendingCheckpoints.size());

		// create 500 snapshots
		for(int i = 0; i < 500; i++) {
			source.snapshotState(i, 15 * i);
		}
		Assert.assertEquals(500, pendingCheckpoints.size());

		// commit only the second last
		source.notifyCheckpointComplete(498);
		Assert.assertEquals(1, pendingCheckpoints.size());

		// access invalid checkpoint
		source.notifyCheckpointComplete(490);

		// and the last
		source.notifyCheckpointComplete(499);
		Assert.assertEquals(0, pendingCheckpoints.size());
	}


	private static class FakeDeserializationSchema implements DeserializationSchema<String> {

		@Override
		public String deserialize(byte[] message) {
			return null;
		}

		@Override
		public boolean isEndOfStream(String nextElement) {
			return false;
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return null;
		}
	}

	// ---------------------------------------------------------------


	@Test
	public void testOffsetManipulation() {
		ZkClient zk = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new FlinkKafkaConsumer081.KafkaZKStringSerializer());

		final String topicName = "testOffsetManipulation";

		// create topic
		Properties topicConfig = new Properties();
		LOG.info("Creating topic {}", topicName);
		AdminUtils.createTopic(zk, topicName, 3, 2, topicConfig);

		FlinkKafkaConsumerBase.setOffset(zk, standardCC.groupId(), topicName, 0, 1337);

		Assert.assertEquals(1337L, FlinkKafkaConsumerBase.getOffset(zk, standardCC.groupId(), topicName, 0));

		zk.close();
	}


	/**
	 * We want to use the High level java consumer API but manage the offset in Zookeeper manually.
	 *
	 */
	@Test
	public void testFlinkKafkaConsumerWithOffsetUpdates() throws Exception {
		LOG.info("Starting testFlinkKafkaConsumerWithOffsetUpdates()");

		ZkClient zk = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new FlinkKafkaConsumer081.KafkaZKStringSerializer());

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

		resetOffsets();

		readSequence(env, standardProps, topicName, 0, 100, 300);

		long[] finalOffsets = getFinalOffsets();
		LOG.info("State in persistent kafka sources {}", finalOffsets);


		long o1 = -1, o2 = -1, o3 = -1;
		if(finalOffsets[0] > 0) {
			o1 = FlinkKafkaConsumer081.getOffset(zk, standardCC.groupId(), topicName, 0);
			Assert.assertTrue("The offset seems incorrect, got " + o1, o1 == finalOffsets[0]);
		}
		if(finalOffsets[1] > 0) {
			o2 = FlinkKafkaConsumer081.getOffset(zk, standardCC.groupId(), topicName, 1);
			Assert.assertTrue("The offset seems incorrect, got " + o2, o2 == finalOffsets[1]);
		}
		if(finalOffsets[2] > 0) {
			o3 = FlinkKafkaConsumer081.getOffset(zk, standardCC.groupId(), topicName, 2);
			Assert.assertTrue("The offset seems incorrect, got " + o3, o3 == finalOffsets[2]);
		}
		Assert.assertFalse("no offset has been set", finalOffsets[0] == 0 &&
													finalOffsets[1] == 0 &&
													finalOffsets[2] == 0);
		LOG.info("Got final offsets from zookeeper o1={}, o2={}, o3={}", o1, o2, o3);

		LOG.info("Manipulating offsets");
		// set the offset to 50 for the three partitions
		FlinkKafkaConsumer081.setOffset(zk, standardCC.groupId(), topicName, 0, 49);
		FlinkKafkaConsumer081.setOffset(zk, standardCC.groupId(), topicName, 1, 49);
		FlinkKafkaConsumer081.setOffset(zk, standardCC.groupId(), topicName, 2, 49);

		// create new env
		env = StreamExecutionEnvironment.createLocalEnvironment(3);
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(0);
		readSequence(env, standardProps, topicName, 50, 50, 150);

		zk.close();

		LOG.info("Finished testFlinkKafkaConsumerWithOffsetUpdates()");
	}

	private void readSequence(StreamExecutionEnvironment env, Properties cc, final String topicName, final int valuesStartFrom, final int valuesCount, final int finalCount) throws Exception {
		LOG.info("Reading sequence for verification until final count {}", finalCount);

		DeserializationSchema<Tuple2<Integer, Integer>> deser = new Utils.TypeInformationSerializationSchema<Tuple2<Integer, Integer>>(new Tuple2<Integer, Integer>(1, 1), env.getConfig());
		FlinkKafkaConsumerBase<Tuple2<Integer, Integer>> pks = getConsumer(topicName, deser, cc);
				DataStream < Tuple2 < Integer, Integer >> source = env.addSource(pks).map(new ThrottleMap<Tuple2<Integer, Integer>>(100));

		// verify data
		DataStream<Integer> validIndexes = source.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Integer>() {
			private static final long serialVersionUID = 1L;

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
			boolean running = true;

			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
				LOG.info("Starting source.");
				int cnt = from;
				int partition = getRuntimeContext().getIndexOfThisSubtask();
				while (running) {
					LOG.info("Writing " + cnt + " to partition " + partition);
					ctx.collect(new Tuple2<Integer, Integer>(partition, cnt));
					if (cnt == to) {
						LOG.info("Writer reached end.");
						return;
					}
					cnt++;
				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got cancel()");
				running = false;
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
		private static final long serialVersionUID = 1L;

		@Override
		public int partition(Object key, int numPartitions) {
			if(numPartitions != 3) {
				throw new IllegalArgumentException("Expected three partitions");
			}
			Tuple2<Integer, Integer> element = (Tuple2<Integer, Integer>) key;
			return element.f0;
		}
	}


	/**
	 * Ensure Kafka is working with Tuple2 types
	 * @throws Exception
	 */
	@Test
	public void tupleTestTopology() throws Exception {
		LOG.info("Starting KafkaITCase.tupleTestTopology()");

		String topic = "tupleTestTopic";
		createTestTopic(topic, 1, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		env.setNumberOfExecutionRetries(0);

		DeserializationSchema deser = new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig());
		FlinkKafkaConsumerBase<Tuple2<Long, String>> source = getConsumer(topic, deser, standardProps);
		// add consuming topology:
		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(source);
		consuming.addSink(new RichSinkFunction<Tuple2<Long, String>>() {
			private static final long serialVersionUID = 1L;

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
			public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				while (running) {
					ctx.collect(new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++));
					LOG.info("Produced " + cnt);
				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got cancel()");
				running = false;
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
		env.setNumberOfExecutionRetries(0);

		// add consuming topology:
		Utils.TypeInformationSerializationSchema<Tuple2<Long, byte[]>> serSchema = new Utils.TypeInformationSerializationSchema<Tuple2<Long, byte[]>>(new Tuple2<Long, byte[]>(0L, new byte[]{0}), env.getConfig());
		Properties consumerProps = new Properties();
		consumerProps.setProperty("fetch.message.max.bytes", Integer.toString(1024 * 1024 * 30));
		consumerProps.setProperty("max.partition.fetch.bytes", Integer.toString(1024 * 1024 * 30)); // for the new fetcher
		consumerProps.setProperty("bootstrap.servers", brokerConnectionStrings);
		consumerProps.setProperty("zookeeper.connect", zookeeperConnectionString);
		consumerProps.setProperty("group.id", "test");
		consumerProps.setProperty("auto.commit.enable", "false");
		consumerProps.setProperty("auto.offset.reset", "earliest");

		FlinkKafkaConsumerBase<Tuple2<Long, byte[]>> source = getConsumer(topic, serSchema, consumerProps);
		DataStreamSource<Tuple2<Long, byte[]>> consuming = env.addSource(source);

		consuming.addSink(new SinkFunction<Tuple2<Long, byte[]>>() {
			private static final long serialVersionUID = 1L;

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
			boolean running;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				running = true;
			}

			@Override
			public void run(SourceContext<Tuple2<Long, byte[]>> ctx) throws Exception {
				LOG.info("Starting source.");
				long cnt = 0;
				Random rnd = new Random(1337);
				while (running) {
					//
					byte[] wl = new byte[Math.abs(rnd.nextInt(1024 * 1024 * 30))];
					ctx.collect(new Tuple2<Long, byte[]>(cnt++, wl));
					LOG.info("Emitted cnt=" + (cnt - 1) + " with byte.length = " + wl.length);

					try {
						Thread.sleep(100);
					} catch (InterruptedException ignored) {
					}
					if(cnt == 10) {
						LOG.info("Send end signal");
						// signal end
						ctx.collect(new Tuple2<Long, byte[]>(-1L, new byte[]{1}));
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
		env.setNumberOfExecutionRetries(0);

		Utils.TypeInformationSerializationSchema<Tuple2<Long, String>> serSchema = new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig());
		FlinkKafkaConsumerBase<Tuple2<Long, String>> source = getConsumer(topic, serSchema, standardProps);
		// add consuming topology:
		DataStreamSource<Tuple2<Long, String>> consuming = env.addSource(source);

		consuming.addSink(new SinkFunction<Tuple2<Long, String>>() {
			private static final long serialVersionUID = 1L;

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
			public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				while (running) {
					ctx.collect(new Tuple2<Long, String>(1000L + cnt, "kafka-" + cnt++));
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
		stream.addSink(new KafkaSink<Tuple2<Long, String>>(brokerConnectionStrings, topic, new Utils.TypeInformationSerializationSchema<Tuple2<Long, String>>(new Tuple2<Long, String>(1L, ""), env.getConfig()), new CustomPartitioner()));

		tryExecute(env, "custom partitioning test");

		LOG.info("Finished KafkaITCase.customPartitioningTestTopology()");
	}

	/**
	 * This is for a topic with 3 partitions and Tuple2<Long, String>
	 */
	private static class CustomPartitioner implements SerializableKafkaPartitioner {
		private static final long serialVersionUID = 1L;

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

	// ------------------------ Broker failure / exactly once test ---------------------------------

	private static boolean leaderHasShutDown = false;
	private static boolean shutdownKafkaBroker;
	final static int NUM_MESSAGES = 200;
	private static Map<Integer, Integer> finalCount = new HashMap<Integer, Integer>();

	/**
	 * This test covers:
	 *  - passing of the execution retries into the JobGraph
	 *  - Restart behavior of the Kafka Sources in case of a broker failure
	 * @throws Exception
	 */
	@Test(timeout=60000)
	public void brokerFailureTest() throws Exception {
		String topic = "brokerFailureTestTopic";


		createTestTopic(topic, 2, 2);

		// --------------------------- write data to topic ---------------------
		LOG.info("Writing data to topic {}", topic);

		Configuration conf = new Configuration();
		conf.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 s");
		conf.setString(ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY, "4096");
		conf.setString(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, "32");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4, conf);
		env.setParallelism(4);
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(0);
		env.setBufferTimeout(0);

		DataStreamSource<String> stream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 1L;

			boolean running = true;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				LOG.info("Starting source.");
				int cnt = 0;
				String payload = "";
				for(int i = 0; i < 160; i++) {
					payload += "A";
				}
				while (running) {
					String msg = "kafka-" + (cnt++) + "-"+payload;
					ctx.collect(msg);
					if(cnt == NUM_MESSAGES) {
						LOG.info("Stopping to produce after 200 msgs");
						break;
					}

				}
			}

			@Override
			public void cancel() {
				LOG.info("Source got chancel()");
				running = false;
			}
		});
		// we write with a parallelism of 1 to ensure that only 200 messages are created
		stream.setParallelism(1);
		stream.addSink(new KafkaSink<String>(brokerConnectionStrings, topic, new JavaDefaultStringSchema())).setParallelism(1);

		tryExecute(env, "broker failure test - writer");

		// --------------------------- read and let broker fail ---------------------

		env.setNumberOfExecutionRetries(1); // allow for one restart
		env.enableCheckpointing(150);
		LOG.info("Reading data from topic {} and let a broker fail", topic);
		// find leader to shut down
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

		// add consuming topology:

		FlinkKafkaConsumerBase<String> src = getConsumer(topic, new JavaDefaultStringSchema(), standardProps);
		DataStreamSource<String> consuming = env.addSource(src);
		consuming.setParallelism(2).map(new ThrottleMap<String>(10)).setParallelism(2); // read in parallel
		DataStream<String> mapped = consuming.map(new PassThroughCheckpointed()).setParallelism(4);
		mapped.addSink(new ExactlyOnceSink(leaderToShutDown)).setParallelism(1); // read only in one instance, to have proper counts

		tryExecute(env, "broker failure test - reader");
		Assert.assertEquals("Count was not correct", 4, finalCount.size());
		int count = 0;
		for(int i = 0; i < 4; i++) {
			count += finalCount.get(i);
		}
		Assert.assertEquals("Count was not correct", NUM_MESSAGES, count); // test for exactly once
	}

	/**
	 * Slow down the previous operator by chaining behind it.
	 * @param <T>
	 */
	private static class ThrottleMap<T> implements MapFunction<T,T> {

		int sleep;
		public ThrottleMap(int sleep) {
			this.sleep = sleep;
		}

		@Override
		public T map(T value) throws Exception {
			Thread.sleep(this.sleep);
			return value;
		}
	}

	private static class PassThroughCheckpointed extends RichMapFunction<String, String> implements Checkpointed<Integer> {

		private Integer count = 0;

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			LOG.info("Counter snapshot");
			return count;
		}

		@Override
		public void restoreState(Integer state) {
			LOG.info("counter restore");
			this.count = state;
		}

		@Override
		public String map(String value) throws Exception {
			count++;
			return value;
		}

		@Override
		public void close() throws Exception {
			finalCount.put(getRuntimeContext().getIndexOfThisSubtask(), count);
		}
	}

	/**
	 * A sink ensuring that no data has been send twice through the topology.
	 * It also fails the one broker (ctor arg) after 15 elements.
	 */
	private static class ExactlyOnceSink extends RichSinkFunction<String> implements Checkpointed<ExactlyOnceSink> {

		private static final long serialVersionUID = 1L;
		int elCnt = 0;
		BitSet validator = new BitSet(NUM_MESSAGES);
		private String leaderToShutDown;

		public ExactlyOnceSink(String leaderToShutDown) {
			this.leaderToShutDown = leaderToShutDown;
		}

		@Override
		public void invoke(String value) throws Exception {
			//LOG.info("Got message = " + value + " leader has shut down " + leaderHasShutDown + " el cnt = " + elCnt);
			String[] sp = value.split("-");
			int recordId = Integer.parseInt(sp[1]);

			Assert.assertFalse("Received tuple with value " + recordId + " twice", validator.get(recordId));
			validator.set(recordId);

			elCnt++;
			if (elCnt == 15 && !shutdownKafkaBroker) {
				// shut down a Kafka broker
				shutdownKafkaBroker = true;
				LOG.info("Stopping lead broker");
				for (KafkaServer kafkaServer : brokers) {
					if (leaderToShutDown.equals(kafkaServer.config().advertisedHostName()+ ":"+ kafkaServer.config().advertisedPort())) {
						LOG.info("Stopping broker {}", leaderToShutDown);
						kafkaServer.shutdown();
						leaderHasShutDown = true;
						break;
					}
				}
				Assert.assertTrue("unable to find leader", leaderHasShutDown);
			}

			if (leaderHasShutDown) { // it only makes sense to check once the shutdown is completed
				if (elCnt >= NUM_MESSAGES) {
					// check if everything in the bitset is set to true
					int nc;
					if ((nc = validator.nextClearBit(0)) != NUM_MESSAGES) {
						throw new RuntimeException("The bitset was not set to 1 on all elements to be checked. Next clear:" + nc + " Set: " + validator);
					}
					throw new SuccessException();
				}
			}
		}

		@Override
		public ExactlyOnceSink snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			LOG.info("Snapshotting state of sink");
			return this;
		}

		@Override
		public void restoreState(ExactlyOnceSink state) {
			LOG.info("Restoring state of sink");
			this.elCnt = state.elCnt;
			this.validator = state.validator;
		}
	}

	// -------------------------------- Utilities --------------------------------------------------


	/**
	 * Execute stream.
	 * forwards any exception except the SuccessException
	 *
	 */
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
		try {
			// I know, bad style.
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}
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

	private static class MockRuntimeContext implements RuntimeContext {

		int numberOfParallelSubtasks = 0;
		int indexOfThisSubtask = 0;
		@Override
		public String getTaskName() {
			return null;
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return numberOfParallelSubtasks;
		}

		@Override
		public int getIndexOfThisSubtask() {
			return indexOfThisSubtask;
		}

		@Override
		public ExecutionConfig getExecutionConfig() {
			return null;
		}

		@Override
		public ClassLoader getUserCodeClassLoader() {
			return null;
		}

		@Override
		public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {

		}

		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
			return null;
		}

		@Override
		public Map<String, Accumulator<?, ?>> getAllAccumulators() {
			return null;
		}

		@Override
		public IntCounter getIntCounter(String name) {
			return null;
		}

		@Override
		public LongCounter getLongCounter(String name) {
			return null;
		}

		@Override
		public DoubleCounter getDoubleCounter(String name) {
			return null;
		}

		@Override
		public Histogram getHistogram(String name) {
			return null;
		}

		@Override
		public <RT> List<RT> getBroadcastVariable(String name) {
			return null;
		}

		@Override
		public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
			return null;
		}

		@Override
		public DistributedCache getDistributedCache() {
			return null;
		}

		@Override
		public <S, C extends Serializable> OperatorState<S> getOperatorState(String name, S defaultState, boolean partitioned, StateCheckpointer<S, C> checkpointer) throws IOException {
			return null;
		}

		@Override
		public <S extends Serializable> OperatorState<S> getOperatorState(String name, S defaultState, boolean partitioned) throws IOException {
			return null;
		}
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
