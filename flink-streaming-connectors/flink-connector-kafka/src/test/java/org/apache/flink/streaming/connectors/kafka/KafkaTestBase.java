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
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.I0Itec.zkclient.ZkClient;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionLeader;
import org.apache.flink.streaming.connectors.kafka.internals.ZooKeeperStringSerializer;
import org.apache.flink.streaming.connectors.kafka.testutils.SuccessException;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.common.PartitionInfo;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The base for the Kafka tests. It brings up:
 * <ul>
 *     <li>A ZooKeeper mini cluster</li>
 *     <li>Three Kafka Brokers (mini clusters)</li>
 *     <li>A Flink mini cluster</li>
 * </ul>
 * 
 * <p>Code in this test is based on the following GitHub repository:
 * <a href="https://github.com/sakserv/hadoop-mini-clusters">
 *   https://github.com/sakserv/hadoop-mini-clusters</a> (ASL licensed),
 * as per commit <i>bc6b2b2d5f6424d5f377aa6c0871e82a956462ef</i></p>
 */
@SuppressWarnings("serial")
public abstract class KafkaTestBase extends TestLogger {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);
	
	protected static final int NUMBER_OF_KAFKA_SERVERS = 3;

	protected static String zookeeperConnectionString;

	protected static File tmpZkDir;

	protected static File tmpKafkaParent;

	protected static TestingServer zookeeper;
	protected static List<KafkaServer> brokers;
	protected static String brokerConnectionStrings = "";

	protected static ConsumerConfig standardCC;
	protected static Properties standardProps;
	
	protected static ForkableFlinkMiniCluster flink;

	protected static int flinkPort;

	protected static FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

	protected static List<File> tmpKafkaDirs;

	protected static String kafkaHost = "localhost";

	// ------------------------------------------------------------------------
	//  Setup and teardown of the mini clusters
	// ------------------------------------------------------------------------
	
	@BeforeClass
	public static void prepare() throws IOException {
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting KafkaITCase ");
		LOG.info("-------------------------------------------------------------------------");
		
		LOG.info("Starting KafkaITCase.prepare()");
		
		File tempDir = new File(System.getProperty("java.io.tmpdir"));
		
		tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
		assertTrue("cannot create zookeeper temp dir", tmpZkDir.mkdirs());

		tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir*" + (UUID.randomUUID().toString()));
		assertTrue("cannot create kafka temp dir", tmpKafkaParent.mkdirs());

		tmpKafkaDirs = new ArrayList<>(NUMBER_OF_KAFKA_SERVERS);
		for (int i = 0; i < NUMBER_OF_KAFKA_SERVERS; i++) {
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
			brokers = new ArrayList<>(NUMBER_OF_KAFKA_SERVERS);
			
			for (int i = 0; i < NUMBER_OF_KAFKA_SERVERS; i++) {
				brokers.add(getKafkaServer(i, tmpKafkaDirs.get(i), kafkaHost, zookeeperConnectionString));
				SocketServer socketServer = brokers.get(i).socketServer();
				
				String host = socketServer.host() == null ? "localhost" : socketServer.host();
				brokerConnectionStrings += hostAndPortToUrlString(host, socketServer.port()) + ",";
			}

			LOG.info("ZK and KafkaServer started.");
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail("Test setup failed: " + t.getMessage());
		}

		standardProps = new Properties();

		standardProps.setProperty("zookeeper.connect", zookeeperConnectionString);
		standardProps.setProperty("bootstrap.servers", brokerConnectionStrings);
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
		
		// start also a re-usable Flink mini cluster
		
		Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 16);
		flinkConfig.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 s");

		flink = new ForkableFlinkMiniCluster(flinkConfig, false);
		flink.start();

		flinkPort = flink.getLeaderRPCPort();
	}

	@AfterClass
	public static void shutDownServices() {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Shut down KafkaITCase ");
		LOG.info("-------------------------------------------------------------------------");

		flinkPort = -1;
		if (flink != null) {
			flink.shutdown();
		}
		
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

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    KafkaITCase finished"); 
		LOG.info("-------------------------------------------------------------------------");
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

	// ------------------------------------------------------------------------
	//  Execution utilities
	// ------------------------------------------------------------------------
	
	protected ZkClient createZookeeperClient() {
		return new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(),
				standardCC.zkConnectionTimeoutMs(), new ZooKeeperStringSerializer());
	}
	
	protected static void tryExecute(StreamExecutionEnvironment see, String name) throws Exception {
		try {
			see.execute(name);
		}
		catch (ProgramInvocationException | JobExecutionException root) {
			Throwable cause = root.getCause();
			
			// search for nested SuccessExceptions
			int depth = 0;
			while (!(cause instanceof SuccessException)) {
				if (cause == null || depth++ == 20) {
					root.printStackTrace();
					fail("Test failed: " + root.getMessage());
				}
				else {
					cause = cause.getCause();
				}
			}
		}
	}

	protected static void tryExecutePropagateExceptions(StreamExecutionEnvironment see, String name) throws Exception {
		try {
			see.execute(name);
		}
		catch (ProgramInvocationException | JobExecutionException root) {
			Throwable cause = root.getCause();

			// search for nested SuccessExceptions
			int depth = 0;
			while (!(cause instanceof SuccessException)) {
				if (cause == null || depth++ == 20) {
					throw root;
				}
				else {
					cause = cause.getCause();
				}
			}
		}
	}
	
	

	protected static void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
		
		// create topic with one client
		Properties topicConfig = new Properties();
		LOG.info("Creating topic {}", topic);

		ZkClient creator = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(),
				standardCC.zkConnectionTimeoutMs(), new ZooKeeperStringSerializer());
		
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
			List<KafkaTopicPartitionLeader> partitions = FlinkKafkaConsumer.getPartitionsForTopic(Collections.singletonList(topic), standardProps);
			if (partitions != null && partitions.size() > 0) {
				return;
			}
		}
		while (System.currentTimeMillis() < deadline);
		fail ("Test topic could not be created");
	}
	
	protected static void deleteTestTopic(String topic) {
		LOG.info("Deleting topic {}", topic);

		ZkClient zk = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(),
				standardCC.zkConnectionTimeoutMs(), new ZooKeeperStringSerializer());

		AdminUtils.deleteTopic(zk, topic);
		
		zk.close();
	}
}
