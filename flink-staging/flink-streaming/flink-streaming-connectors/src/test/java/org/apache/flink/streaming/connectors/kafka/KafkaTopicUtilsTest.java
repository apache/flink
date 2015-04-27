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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.test.TestingServer;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.util.KafkaLocalSystemTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.PartitionMetadata;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

public class KafkaTopicUtilsTest {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicUtilsTest.class);
	private static final int NUMBER_OF_BROKERS = 2;
	private static final String TOPIC = "myTopic";

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void test() {
		int zkPort;
		String kafkaHost;
		String zookeeperConnectionString;

		File tmpZkDir;
		List<File> tmpKafkaDirs;
		Map<String, KafkaServer> kafkaServers = null;
		TestingServer zookeeper = null;

		try {
			tmpZkDir = tempFolder.newFolder();

			tmpKafkaDirs = new ArrayList<File>(NUMBER_OF_BROKERS);
			for (int i = 0; i < NUMBER_OF_BROKERS; i++) {
				tmpKafkaDirs.add(tempFolder.newFolder());
			}

			zkPort = NetUtils.getAvailablePort();
			kafkaHost = InetAddress.getLocalHost().getHostName();
			zookeeperConnectionString = "localhost:" + zkPort;

			// init zookeeper
			zookeeper = new TestingServer(zkPort, tmpZkDir);

			// init kafka kafkaServers
			kafkaServers = new HashMap<String, KafkaServer>();

			for (int i = 0; i < NUMBER_OF_BROKERS; i++) {
				KafkaServer kafkaServer = getKafkaServer(kafkaHost, zookeeperConnectionString, i, tmpKafkaDirs.get(i));
				kafkaServers.put(kafkaServer.config().advertisedHostName() + ":" + kafkaServer.config().advertisedPort(), kafkaServer);
			}

			// create Kafka topic
			final KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperConnectionString);
			kafkaTopicUtils.createTopic(TOPIC, 1, 2);

			// check whether topic exists
			assertTrue(kafkaTopicUtils.topicExists(TOPIC));

			// check number of partitions
			assertEquals(1, kafkaTopicUtils.getNumberOfPartitions(TOPIC));

			// get partition metadata without error
			PartitionMetadata partitionMetadata = kafkaTopicUtils.waitAndGetPartitionMetadata(TOPIC, 0);
			assertEquals(0, partitionMetadata.errorCode());

			// get broker list
			assertEquals(new HashSet<String>(kafkaServers.keySet()), kafkaTopicUtils.getBrokerAddresses(TOPIC));
		} catch (IOException e) {
			fail(e.toString());
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			LOG.info("Shutting down all services");
			for (KafkaServer broker : kafkaServers.values()) {
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
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	private static KafkaServer getKafkaServer(String kafkaHost, String zookeeperConnectionString, int brokerId, File tmpFolder) throws UnknownHostException {
		Properties kafkaProperties = new Properties();

		int kafkaPort = NetUtils.getAvailablePort();

		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", kafkaHost);
		kafkaProperties.put("port", Integer.toString(kafkaPort));
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpFolder.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

		KafkaServer server = new KafkaServer(kafkaConfig, new KafkaLocalSystemTime());
		server.startup();
		return server;
	}

}
