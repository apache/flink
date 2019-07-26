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

import org.apache.flink.networking.NetworkFailuresProxy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.internals.Kafka08PartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionLeader;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.streaming.connectors.kafka.internals.ZookeeperOffsetHandler;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.ZooKeeperStringSerializer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;

import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.common.KafkaException;
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.BindException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import scala.collection.Seq;

import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An implementation of the KafkaServerProvider for Kafka 0.8 .
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
	private Config config;

	public String getBrokerConnectionString() {
		return brokerConnectionString;
	}

	@Override
	public Properties getStandardProperties() {
		return standardProps;
	}

	@Override
	public Properties getSecureProperties() {
		return null;
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
	public <T> FlinkKafkaConsumerBase<T> getConsumer(List<String> topics, KafkaDeserializationSchema<T> readSchema, Properties props) {
		return new FlinkKafkaConsumer08<>(topics, readSchema, props);
	}

	@Override
	public <K, V> Collection<ConsumerRecord<K, V>> getAllRecordsFromTopic(Properties properties, String topic, int partition, long timeout) {
		List<ConsumerRecord<K, V>> result = new ArrayList<>();
		try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties)) {
			consumer.subscribe(new TopicPartition(topic, partition));

			while (true) {
				Map<String, ConsumerRecords<K, V>> topics = consumer.poll(timeout);
				if (topics == null || !topics.containsKey(topic)) {
					break;
				}
				List<ConsumerRecord<K, V>> records = topics.get(topic).records(partition);
				result.addAll(records);
				if (records.size() == 0) {
					break;
				}
			}
			consumer.commit(true);
		}

		return UnmodifiableList.decorate(result);
	}

	@Override
	public <T> StreamSink<T> getProducerSink(
			String topic,
			KeyedSerializationSchema<T> serSchema,
			Properties props,
			FlinkKafkaPartitioner<T> partitioner) {
		FlinkKafkaProducer08<T> prod = new FlinkKafkaProducer08<>(
				topic,
				serSchema,
				props,
				partitioner);
		prod.setFlushOnCheckpoint(true);
		return new StreamSink<>(prod);
	}

	@Override
	public <T> DataStreamSink<T> produceIntoKafka(DataStream<T> stream, String topic, KeyedSerializationSchema<T> serSchema, Properties props, FlinkKafkaPartitioner<T> partitioner) {
		FlinkKafkaProducer08<T> prod = new FlinkKafkaProducer08<>(topic, serSchema, props, partitioner);
		prod.setFlushOnCheckpoint(true);
		return stream.addSink(prod);
	}

	@Override
	public KafkaOffsetHandler createOffsetHandler() {
		return new KafkaOffsetHandlerImpl();
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
	public boolean isSecureRunSupported() {
		return false;
	}

	@Override
	public void prepare(Config config) throws Exception {
		this.config = config;
		File tempDir = new File(System.getProperty("java.io.tmpdir"));

		tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
		Files.createDirectories(tmpZkDir.toPath());

		tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir" + (UUID.randomUUID().toString()));
		Files.createDirectories(tmpKafkaParent.toPath());

		tmpKafkaDirs = new ArrayList<>(config.getKafkaServersNumber());
		for (int i = 0; i < config.getKafkaServersNumber(); i++) {
			File tmpDir = new File(tmpKafkaParent, "server-" + i);
			assertTrue("cannot create kafka temp dir", tmpDir.mkdir());
			tmpKafkaDirs.add(tmpDir);
		}

		zookeeper = null;
		brokers = null;

		LOG.info("Starting Zookeeper");
		zookeeper = new TestingServer(-1, tmpZkDir);
		zookeeperConnectionString = zookeeper.getConnectString();

		LOG.info("Starting KafkaServer");
		brokers = new ArrayList<>(config.getKafkaServersNumber());

		for (int i = 0; i < config.getKafkaServersNumber(); i++) {
			brokers.add(getKafkaServer(i, tmpKafkaDirs.get(i)));
			SocketServer socketServer = brokers.get(i).socketServer();

			String host = socketServer.host() == null ? "localhost" : socketServer.host();
			brokerConnectionString += hostAndPortToUrlString(host, socketServer.port()) + ",";
		}

		LOG.info("ZK and KafkaServer started.");

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
	public void shutdown() throws Exception {
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
				zookeeper.close();
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
		super.shutdown();
	}

	@Override
	public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor, Properties topicConfig) {
		// create topic with one client
		LOG.info("Creating topic {}", topic);

		ZkClient creator = createZkClient();

		AdminUtils.createTopic(creator, topic, numberOfPartitions, replicationFactor, topicConfig);
		creator.close();

		List<String> topicList = Collections.singletonList(topic);

		// create a partition discoverer, to make sure that partitions for the test topic are created
		Kafka08PartitionDiscoverer partitionDiscoverer =
			new Kafka08PartitionDiscoverer(new KafkaTopicsDescriptor(topicList, null), 0, 1, standardProps);
		try {
			partitionDiscoverer.open();
		} catch (Exception e) {
			throw new RuntimeException("Exception while opening partition discoverer.", e);
		}

		// validate that the topic has been created
		final long deadline = System.nanoTime() + 30_000_000_000L;
		do {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				// restore interrupted state
			}
			List<KafkaTopicPartitionLeader> partitions = partitionDiscoverer.getPartitionLeadersForTopics(Collections.singletonList(topic));
			if (partitions != null && partitions.size() > 0) {
				return;
			}
		}
		while (System.nanoTime() < deadline);
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
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed).
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
		if (config.getKafkaServerProperties() != null) {
			kafkaProperties.putAll(config.getKafkaServerProperties());
		}

		final int numTries = 5;

		for (int i = 1; i <= numTries; i++) {
			int kafkaPort = NetUtils.getAvailablePort();
			kafkaProperties.put("port", Integer.toString(kafkaPort));

			if (config.isHideKafkaBehindProxy()) {
				NetworkFailuresProxy proxy = createProxy(KAFKA_HOST, kafkaPort);
				kafkaProperties.put("advertised.port", Integer.toString(proxy.getLocalPort()));
			}

			KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

			try {
				KafkaServer server = new KafkaServer(kafkaConfig, SystemTime$.MODULE$);
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

	private class KafkaOffsetHandlerImpl implements KafkaOffsetHandler {

		private final CuratorFramework offsetClient;
		private final String groupId;

		public KafkaOffsetHandlerImpl() {
			offsetClient = createCuratorClient();
			groupId = standardProps.getProperty("group.id");
		}

		@Override
		public Long getCommittedOffset(String topicName, int partition) {
			try {
				return ZookeeperOffsetHandler.getOffsetFromZooKeeper(offsetClient, groupId, topicName, partition);
			} catch (Exception e) {
				throw new RuntimeException("Exception when getting offsets from Zookeeper", e);
			}
		}

		@Override
		public void setCommittedOffset(String topicName, int partition, long offset) {
			try {
				ZookeeperOffsetHandler.setOffsetInZooKeeper(offsetClient, groupId, topicName, partition, offset);
			} catch (Exception e) {
				throw new RuntimeException("Exception when writing offsets to Zookeeper", e);
			}
		}

		@Override
		public void close() {
			offsetClient.close();
		}
	}

}
