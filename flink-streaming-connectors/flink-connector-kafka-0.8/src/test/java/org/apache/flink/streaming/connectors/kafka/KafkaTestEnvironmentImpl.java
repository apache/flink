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
import kafka.api.PartitionMetadata;
import kafka.common.KafkaException;
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionLeader;
import org.apache.flink.streaming.connectors.kafka.testutils.ZooKeeperStringSerializer;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An implementation of the KafkaServerProvider for Kafka 0.8
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
	private Properties additionalServerProperties;

	public String getBrokerConnectionString() {
		return brokerConnectionString;
	}

	@Override
	public Properties getStandardProperties() {
		return standardProps;
	}

	@Override
	public String getVersion() {
		return "0.8";
	}

	@Override
	public List<KafkaServer> getBrokers() {
		return brokers;
	}

	@Override
	public <T> FlinkKafkaConsumerBase<T> getConsumer(List<String> topics, KeyedDeserializationSchema<T> readSchema, Properties props) {
		return new FlinkKafkaConsumer08<>(topics, readSchema, props);
	}

	@Override
	public <T> FlinkKafkaProducerBase<T> getProducer(String topic, KeyedSerializationSchema<T> serSchema, Properties props, KafkaPartitioner<T> partitioner) {
		return new FlinkKafkaProducer08<T>(topic, serSchema, props, partitioner);
	}

	@Override
	public void restartBroker(int leaderId) throws Exception {
		brokers.set(leaderId, getKafkaServer(leaderId, tmpKafkaDirs.get(leaderId)));
	}

	@Override
	public int getLeaderToShutDown(String topic) throws Exception {
		ZkClient zkClient = createZkClient();
		PartitionMetadata firstPart = null;
		do {
			if (firstPart != null) {
				LOG.info("Unable to find leader. error code {}", firstPart.errorCode());
				// not the first try. Sleep a bit
				Thread.sleep(150);
			}

			Seq<PartitionMetadata> partitionMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient).partitionsMetadata();
			firstPart = partitionMetadata.head();
		}
		while (firstPart.errorCode() != 0);
		zkClient.close();

		return firstPart.leader().get().id();
	}

	@Override
	public int getBrokerId(KafkaServer server) {
		return server.socketServer().brokerId();
	}


	@Override
	public void prepare(int numKafkaServers, Properties additionalServerProperties) {
		this.additionalServerProperties = additionalServerProperties;
		File tempDir = new File(System.getProperty("java.io.tmpdir"));

		tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
		try {
			Files.createDirectories(tmpZkDir.toPath());
		} catch (IOException e) {
			fail("cannot create zookeeper temp dir: " + e.getMessage());
		}

		tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir" + (UUID.randomUUID().toString()));
		try {
			Files.createDirectories(tmpKafkaParent.toPath());
		} catch (IOException e) {
			fail("cannot create kafka temp dir: " + e.getMessage());
		}

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
				brokers.add(getKafkaServer(i, tmpKafkaDirs.get(i)));
				SocketServer socketServer = brokers.get(i).socketServer();

				String host = socketServer.host() == null ? "localhost" : socketServer.host();
				brokerConnectionString += hostAndPortToUrlString(host, socketServer.port()) + ",";
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
		standardProps.setProperty("zookeeper.session.timeout.ms", "30000"); // 6 seconds is default. Seems to be too small for travis.
		standardProps.setProperty("zookeeper.connection.timeout.ms", "30000");
		standardProps.setProperty("auto.offset.reset", "smallest"); // read from the beginning. (smallest is kafka 0.8)
		standardProps.setProperty("fetch.message.max.bytes", "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)
	}

	@Override
	public void shutdown() {
		if (brokers != null) {
			for (KafkaServer broker : brokers) {
				if (broker != null) {
					broker.shutdown();
				}
			}
			brokers.clear();
		}

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

	@Override
	public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor, Properties topicConfig) {
		// create topic with one client
		LOG.info("Creating topic {}", topic);

		ZkClient creator = createZkClient();

		AdminUtils.createTopic(creator, topic, numberOfPartitions, replicationFactor, topicConfig);
		creator.close();

		// validate that the topic has been created
		final long deadline = System.currentTimeMillis() + 30000;
		do {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				// restore interrupted state
			}
			List<KafkaTopicPartitionLeader> partitions = FlinkKafkaConsumer08.getPartitionsForTopic(Collections.singletonList(topic), standardProps);
			if (partitions != null && partitions.size() > 0) {
				return;
			}
		}
		while (System.currentTimeMillis() < deadline);
		fail ("Test topic could not be created");
	}

	@Override
	public void deleteTestTopic(String topic) {
		LOG.info("Deleting topic {}", topic);

		ZkClient zk = createZkClient();
		AdminUtils.deleteTopic(zk, topic);
		zk.close();
	}

	private ZkClient createZkClient() {
		return new ZkClient(zookeeperConnectionString, Integer.valueOf(standardProps.getProperty("zookeeper.session.timeout.ms")),
				Integer.valueOf(standardProps.getProperty("zookeeper.connection.timeout.ms")), new ZooKeeperStringSerializer());
	}

	/**
	 * Only for the 0.8 server we need access to the zk client.
	 */
	public CuratorFramework createCuratorClient() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 10);
		CuratorFramework curatorClient = CuratorFrameworkFactory.newClient(standardProps.getProperty("zookeeper.connect"), retryPolicy);
		curatorClient.start();
		return curatorClient;
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	protected KafkaServer getKafkaServer(int brokerId, File tmpFolder) throws Exception {
		LOG.info("Starting broker with id {}", brokerId);
		Properties kafkaProperties = new Properties();

		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", KAFKA_HOST);
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpFolder.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024));
		kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));

		// for CI stability, increase zookeeper session timeout
		kafkaProperties.put("zookeeper.session.timeout.ms", "30000");
		kafkaProperties.put("zookeeper.connection.timeout.ms", "30000");
		if(additionalServerProperties != null) {
			kafkaProperties.putAll(additionalServerProperties);
		}

		final int numTries = 5;

		for (int i = 1; i <= numTries; i++) {
			int kafkaPort = NetUtils.getAvailablePort();
			kafkaProperties.put("port", Integer.toString(kafkaPort));
			KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

			try {
				KafkaServer server = new KafkaServer(kafkaConfig, new KafkaLocalSystemTime());
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
