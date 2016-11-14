/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

public class KafkaConsumer08Test {

	@Test
	public void testValidateZooKeeperConfig() {
		try {
			// empty
			Properties emptyProperties = new Properties();
			try {
				FlinkKafkaConsumer08.validateZooKeeperConfig(emptyProperties);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// expected
			}

			// no connect string (only group string)
			Properties noConnect = new Properties();
			noConnect.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-test-group");
			try {
				FlinkKafkaConsumer08.validateZooKeeperConfig(noConnect);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// expected
			}

			// no group string (only connect string)
			Properties noGroup = new Properties();
			noGroup.put("zookeeper.connect", "localhost:47574");
			try {
				FlinkKafkaConsumer08.validateZooKeeperConfig(noGroup);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCreateSourceWithoutCluster() {
		try {
			Properties props = new Properties();
			props.setProperty("zookeeper.connect", "localhost:56794");
			props.setProperty("bootstrap.servers", "localhost:11111, localhost:22222");
			props.setProperty("group.id", "non-existent-group");

			FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>(Collections.singletonList("no op topic"), new SimpleStringSchema(), props);
			consumer.open(new Configuration());
			fail();
		}
		catch (Exception e) {
			assertTrue(e.getMessage().contains("Unable to retrieve any partitions"));
		}
	}

	@Test
	public void testAllBoostrapServerHostsAreInvalid() {
		try {
			String zookeeperConnect = "localhost:56794";
			String bootstrapServers = "indexistentHost:11111";
			String groupId = "non-existent-group";
			Properties props = createKafkaProps(zookeeperConnect, bootstrapServers, groupId);
			FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>(Collections.singletonList("no op topic"),
					new SimpleStringSchema(), props);
			consumer.open(new Configuration());
			fail();
		} catch (Exception e) {
			assertTrue("Exception should be thrown containing 'all bootstrap servers invalid' message!",
					e.getMessage().contains("All the servers provided in: '" + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
							+ "' config are invalid"));
		}
	}

	@Test
	public void testAtLeastOneBootstrapServerHostIsValid() {
		try {
			String zookeeperConnect = "localhost:56794";
			// we declare one valid boostrap server, namely the one with
			// 'localhost'
			String bootstrapServers = "indexistentHost:11111, localhost:22222";
			String groupId = "non-existent-group";
			Properties props = createKafkaProps(zookeeperConnect, bootstrapServers, groupId);
			FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>(Collections.singletonList("no op topic"),
					new SimpleStringSchema(), props);
			consumer.open(new Configuration());
			fail();
		} catch (Exception e) {
			// test is not failing because we have one valid boostrap server
			assertTrue("The cause of the exception should not be 'all boostrap server are invalid'!",
					!e.getMessage().contains("All the hosts provided in: " + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
							+ " config are invalid"));
		}
	}
	
	private Properties createKafkaProps(String zookeeperConnect, String bootstrapServers, String groupId) {
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnect);
		props.setProperty("bootstrap.servers", bootstrapServers);
		props.setProperty("group.id", groupId);
		return props;
	}
}
