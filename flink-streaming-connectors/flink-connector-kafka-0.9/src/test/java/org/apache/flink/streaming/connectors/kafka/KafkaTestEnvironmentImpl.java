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
import kafka.api.PartitionMetadata;
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.testutils.ZooKeeperStringSerializer;
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
	private Properties additionalServerProperties;
	private boolean secureMode = false;
	// 6 seconds is default. Seems to be too small for travis. 30 seconds
	private String zkTimeout = "30000";

	public String getBrokerConnectionString() {
		return brokerConnectionString;
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
	public <T> DataStreamSink<T> produceIntoKafka(DataStream<T> stream, String topic, KeyedSerializationSchema<T> serSchema, Properties props, KafkaPartitioner<T> partitioner) {
		FlinkKafkaProducer09<T> prod = new FlinkKafkaProducer09<>(topic, serSchema, props, partitioner);
		prod.setFlushOnCheckpoint(true);
		return stream.addSink(prod);
	}

	@Override
	public void restartBroker(int leaderId) throws Exception {
		brokers.set(leaderId, getKafkaServer(leaderId, tmpKafkaDirs.get(leaderId)));
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
	public boolean isSecureRunSupported() {
		return true;
	}

	@Override
	public void prepare(int numKafkaServers, Properties additionalServerProperties, boolean secureMode) {

		//increase the timeout since in Travis ZK connection takes long time for secure connection.
		if(secureMode) {
			//run only one kafka server to avoid multiple ZK connections from many instances - Travis timeout
			numKafkaServers = 1;
			zkTimeout = String.valueOf(Integer.parseInt(zkTimeout) * 15);
		}

		this.additionalServerProperties = additionalServerProperties;
		this.secureMode = secureMode;
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
			LOG.info("zookeeperConnectionString: {}", zookeeperConnectionString);

			LOG.info("Starting KafkaServer");
			brokers = new ArrayList<>(numKafkaServers);

			for (int i = 0; i < numKafkaServers; i++) {
				brokers.add(getKafkaServer(i, tmpKafkaDirs.get(i)));

				SocketServer socketServer = brokers.get(i).socketServer();
				if(secureMode) {
					brokerConnectionString += hostAndPortToUrlString(KafkaTestEnvironment.KAFKA_HOST, brokers.get(i).socketServer().boundPort(SecurityProtocol.SASL_PLAINTEXT)) + ",";
				} else {
					brokerConnectionString += hostAndPortToUrlString(KafkaTestEnvironment.KAFKA_HOST, brokers.get(i).socketServer().boundPort(SecurityProtocol.PLAINTEXT)) + ",";
				}
			}

			LOG.info("ZK and KafkaServer started.");
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail("Test setup failed: " + t.getMessage());
		}

		LOG.info("brokerConnectionString --> {}", brokerConnectionString);

		standardProps = new Properties();
		standardProps.setProperty("zookeeper.connect", zookeeperConnectionString);
		standardProps.setProperty("bootstrap.servers", brokerConnectionString);
		standardProps.setProperty("group.id", "flink-tests");
		standardProps.setProperty("auto.commit.enable", "false");
		standardProps.setProperty("zookeeper.session.timeout.ms", zkTimeout);
		standardProps.setProperty("zookeeper.connection.timeout.ms", zkTimeout);
		standardProps.setProperty("auto.offset.reset", "earliest"); // read from the beginning. (earliest is kafka 0.9 value)
		standardProps.setProperty("fetch.message.max.bytes", "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)

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
	}

	public ZkUtils getZkUtils() {
		LOG.info("In getZKUtils:: zookeeperConnectionString = {}", zookeeperConnectionString);
		ZkClient creator = new ZkClient(zookeeperConnectionString, Integer.valueOf(standardProps.getProperty("zookeeper.session.timeout.ms")),
				Integer.valueOf(standardProps.getProperty("zookeeper.connection.timeout.ms")), new ZooKeeperStringSerializer());
		return ZkUtils.apply(creator, false);
	}

	@Override
	public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor, Properties topicConfig) {
		// create topic with one client
		LOG.info("Creating topic {}", topic);

		ZkUtils zkUtils = getZkUtils();
		try {
			AdminUtils.createTopic(zkUtils, topic, numberOfPartitions, replicationFactor, topicConfig);
		} finally {
			zkUtils.close();
		}

		LOG.info("Topic {} create request is successfully posted", topic);

		// validate that the topic has been created
		final long deadline = System.currentTimeMillis() + Integer.parseInt(zkTimeout);
		do {
			try {
				if(secureMode) {
					//increase wait time since in Travis ZK timeout occurs frequently
					int wait = Integer.parseInt(zkTimeout) / 100;
					LOG.info("waiting for {} msecs before the topic {} can be checked", wait, topic);
					Thread.sleep(wait);
				} else {
					Thread.sleep(100);
				}

			} catch (InterruptedException e) {
				// restore interrupted state
			}
			// we could use AdminUtils.topicExists(zkUtils, topic) here, but it's results are
			// not always correct.

			LOG.info("Validating if the topic {} has been created or not", topic);

			// create a new ZK utils connection
			ZkUtils checkZKConn = getZkUtils();
			if(AdminUtils.topicExists(checkZKConn, topic)) {
				LOG.info("topic {} has been created successfully", topic);
				checkZKConn.close();
				return;
			}
			LOG.info("topic {} has not been created yet. Will check again...", topic);
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

			ZkClient zk = new ZkClient(zookeeperConnectionString, Integer.valueOf(standardProps.getProperty("zookeeper.session.timeout.ms")),
				Integer.valueOf(standardProps.getProperty("zookeeper.connection.timeout.ms")), new ZooKeeperStringSerializer());

			AdminUtils.deleteTopic(zkUtils, topic);

			zk.close();
		} finally {
			zkUtils.close();
		}
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	protected KafkaServer getKafkaServer(int brokerId, File tmpFolder) throws Exception {
		Properties kafkaProperties = new Properties();

		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", KAFKA_HOST);
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpFolder.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024));
		kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));

		// for CI stability, increase zookeeper session timeout
		kafkaProperties.put("zookeeper.session.timeout.ms", zkTimeout);
		kafkaProperties.put("zookeeper.connection.timeout.ms", zkTimeout);
		if(additionalServerProperties != null) {
			kafkaProperties.putAll(additionalServerProperties);
		}

		final int numTries = 5;

		for (int i = 1; i <= numTries; i++) {
			int kafkaPort = NetUtils.getAvailablePort();
			kafkaProperties.put("port", Integer.toString(kafkaPort));

			//to support secure kafka cluster
			if(secureMode) {
				LOG.info("Adding Kafka secure configurations");
				kafkaProperties.put("listeners", "SASL_PLAINTEXT://" + KAFKA_HOST + ":" + kafkaPort);
				kafkaProperties.put("advertised.listeners", "SASL_PLAINTEXT://" + KAFKA_HOST + ":" + kafkaPort);
				kafkaProperties.putAll(getSecureProperties());
			}

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

	public Properties getSecureProperties() {
		Properties prop = new Properties();
		if(secureMode) {
			prop.put("security.inter.broker.protocol", "SASL_PLAINTEXT");
			prop.put("security.protocol", "SASL_PLAINTEXT");
			prop.put("sasl.kerberos.service.name", "kafka");

			//add special timeout for Travis
			prop.setProperty("zookeeper.session.timeout.ms", zkTimeout);
			prop.setProperty("zookeeper.connection.timeout.ms", zkTimeout);
			prop.setProperty("metadata.fetch.timeout.ms","120000");
		}
		return prop;
	}

}
