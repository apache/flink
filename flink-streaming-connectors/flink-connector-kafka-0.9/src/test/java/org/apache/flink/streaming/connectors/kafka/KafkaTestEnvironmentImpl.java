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

import kafka.admin.AdminUtils;
import kafka.common.KafkaException;
import kafka.consumer.ConsumerConfig;
import kafka.api.PartitionMetadata;
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.streaming.connectors.kafka.internals.ZooKeeperStringSerializer;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.File;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An implementation of the KafkaServerProvider for Kafka 0.9
 */
public class KafkaTestEnvironmentImpl extends KafkaTestEnvironment {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestEnvironmentImpl.class);
	private File tmpZkDir;
	private File tmpKafkaParent;
	private List<File> tmpKafkaDirs;
	private List<KafkaServer> brokers;
	private TestingServer zookeeper;
	private String zookeeperConnectionString;
	private String brokerConnectionString = "";
	private Properties standardProps;
	private ConsumerConfig standardCC;


	public String getBrokerConnectionString() {
		return brokerConnectionString;
	}

	@Override
	public ConsumerConfig getStandardConsumerConfig() {
		return standardCC;
	}

	@Override
	public Properties getStandardProperties() {
		return standardProps;
	}

	@Override
	public String getVersion() {
		return "0.9";
	}

	@Override
	public List<KafkaServer> getBrokers() {
		return brokers;
	}

	@Override
	public <T> FlinkKafkaConsumerBase<T> getConsumer(List<String> topics, KeyedDeserializationSchema<T> readSchema, Properties props) {
		return new FlinkKafkaConsumer09<>(topics, readSchema, props);
	}

	@Override
	public <T> FlinkKafkaProducerBase<T> getProducer(String topic, KeyedSerializationSchema<T> serSchema, Properties props, KafkaPartitioner<T> partitioner) {
		return new FlinkKafkaProducer09<>(topic, serSchema, props, partitioner);
	}

	@Override
	public void restartBroker(int leaderId) throws Exception {
		brokers.set(leaderId, getKafkaServer(leaderId, tmpKafkaDirs.get(leaderId), KAFKA_HOST, zookeeperConnectionString));
	}

	@Override
	public int getLeaderToShutDown(String topic) throws Exception {
		ZkUtils zkUtils = getZkUtils();
		try {
			PartitionMetadata firstPart = null;
			do {
				if (firstPart != null) {
					LOG.info("Unable to find leader. error code {}", firstPart.errorCode());
					// not the first try. Sleep a bit
					Thread.sleep(150);
				}

				Seq<PartitionMetadata> partitionMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils).partitionsMetadata();
				firstPart = partitionMetadata.head();
			}
			while (firstPart.errorCode() != 0);

			return firstPart.leader().get().id();
		} finally {
			zkUtils.close();
		}
	}

	@Override
	public int getBrokerId(KafkaServer server) {
		return server.config().brokerId();
	}

	@Override
	public void prepare(int numKafkaServers) {
		File tempDir = new File(System.getProperty("java.io.tmpdir"));

		tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
		assertTrue("cannot create zookeeper temp dir", tmpZkDir.mkdirs());

		tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir*" + (UUID.randomUUID().toString()));
		assertTrue("cannot create kafka temp dir", tmpKafkaParent.mkdirs());

		tmpKafkaDirs = new ArrayList<>(numKafkaServers);
		for (int i = 0; i < numKafkaServers; i++) {
			File tmpDir = new File(tmpKafkaParent, "server-" + i);
			assertTrue("cannot create kafka temp dir", tmpDir.mkdir());
			tmpKafkaDirs.add(tmpDir);
		}

		zookeeper = null;
		brokers = null;

		try {
			LOG.info("Starting Zookeeper");
			zookeeper = new TestingServer(-1, tmpZkDir);
			zookeeperConnectionString = zookeeper.getConnectString();

			LOG.info("Starting KafkaServer");
			brokers = new ArrayList<>(numKafkaServers);

			for (int i = 0; i < numKafkaServers; i++) {
				brokers.add(getKafkaServer(i, tmpKafkaDirs.get(i), KafkaTestEnvironment.KAFKA_HOST, zookeeperConnectionString));

				SocketServer socketServer = brokers.get(i).socketServer();
				brokerConnectionString += hostAndPortToUrlString(KafkaTestEnvironment.KAFKA_HOST, brokers.get(i).socketServer().boundPort(SecurityProtocol.PLAINTEXT)) + ",";
			}

			LOG.info("ZK and KafkaServer started.");
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail("Test setup failed: " + t.getMessage());
		}

		standardProps = new Properties();
		standardProps.setProperty("zookeeper.connect", zookeeperConnectionString);
		standardProps.setProperty("bootstrap.servers", brokerConnectionString);
		standardProps.setProperty("group.id", "flink-tests");
		standardProps.setProperty("auto.commit.enable", "false");
		standardProps.setProperty("zookeeper.session.timeout.ms", "12000"); // 6 seconds is default. Seems to be too small for travis.
		standardProps.setProperty("zookeeper.connection.timeout.ms", "20000");
		standardProps.setProperty("auto.offset.reset", "earliest"); // read from the beginning.
		standardProps.setProperty("fetch.message.max.bytes", "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)

		Properties consumerConfigProps = new Properties();
		consumerConfigProps.putAll(standardProps);
		consumerConfigProps.setProperty("auto.offset.reset", "smallest");
		standardCC = new ConsumerConfig(consumerConfigProps);
	}

	@Override
	public void shutdown() {
		for (KafkaServer broker : brokers) {
			if (broker != null) {
				broker.shutdown();
			}
		}
		brokers.clear();

		if (zookeeper != null) {
			try {
				zookeeper.stop();
			}
			catch (Exception e) {
				LOG.warn("ZK.stop() failed", e);
			}
			zookeeper = null;
		}

		// clean up the temp spaces

		if (tmpKafkaParent != null && tmpKafkaParent.exists()) {
			try {
				FileUtils.deleteDirectory(tmpKafkaParent);
			}
			catch (Exception e) {
				// ignore
			}
		}
		if (tmpZkDir != null && tmpZkDir.exists()) {
			try {
				FileUtils.deleteDirectory(tmpZkDir);
			}
			catch (Exception e) {
				// ignore
			}
		}
	}

	public ZkUtils getZkUtils() {
		ZkClient creator = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(),
				standardCC.zkConnectionTimeoutMs(), new ZooKeeperStringSerializer());
		return ZkUtils.apply(creator, false);
	}

	@Override
	public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
		// create topic with one client
		Properties topicConfig = new Properties();
		LOG.info("Creating topic {}", topic);

		ZkUtils zkUtils = getZkUtils();
		try {
			AdminUtils.createTopic(zkUtils, topic, numberOfPartitions, replicationFactor, topicConfig);
		} finally {
			zkUtils.close();
		}

		// validate that the topic has been created
		final long deadline = System.currentTimeMillis() + 30000;
		do {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// restore interrupted state
			}
			// we could use AdminUtils.topicExists(zkUtils, topic) here, but it's results are
			// not always correct.

			// create a new ZK utils connection
			ZkUtils checkZKConn = getZkUtils();
			if(AdminUtils.topicExists(checkZKConn, topic)) {
				checkZKConn.close();
				return;
			}
			checkZKConn.close();
		}
		while (System.currentTimeMillis() < deadline);
		fail("Test topic could not be created");
	}

	@Override
	public void deleteTestTopic(String topic) {
		ZkUtils zkUtils = getZkUtils();
		try {
			LOG.info("Deleting topic {}", topic);

			ZkClient zk = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(),
					standardCC.zkConnectionTimeoutMs(), new ZooKeeperStringSerializer());

			AdminUtils.deleteTopic(zkUtils, topic);

			zk.close();
		} finally {
			zkUtils.close();
		}
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	protected static KafkaServer getKafkaServer(int brokerId, File tmpFolder,
												String kafkaHost,
												String zookeeperConnectionString) throws Exception {
		Properties kafkaProperties = new Properties();

		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", kafkaHost);
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpFolder.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024));
		kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));

		// for CI stability, increase zookeeper session timeout
		kafkaProperties.put("zookeeper.session.timeout.ms", "20000");

		final int numTries = 5;

		for (int i = 1; i <= numTries; i++) {
			int kafkaPort = NetUtils.getAvailablePort();
			kafkaProperties.put("port", Integer.toString(kafkaPort));
			KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

			try {
				scala.Option<String> stringNone = scala.Option.apply(null);
				KafkaServer server = new KafkaServer(kafkaConfig, SystemTime$.MODULE$, stringNone);
				server.startup();
				return server;
			}
			catch (KafkaException e) {
				if (e.getCause() instanceof BindException) {
					// port conflict, retry...
					LOG.info("Port conflict when starting Kafka Broker. Retrying...");
				}
				else {
					throw e;
				}
			}
		}

		throw new Exception("Could not start Kafka after " + numTries + " retries due to port conflicts.");
	}

}
