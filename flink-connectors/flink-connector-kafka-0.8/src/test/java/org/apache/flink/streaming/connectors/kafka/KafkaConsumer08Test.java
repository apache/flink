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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.Kafka08PartitionDiscoverer;
import org.apache.flink.util.NetUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the {@link FlinkKafkaConsumer08}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Kafka08PartitionDiscoverer.class)
@PowerMockIgnore("javax.management.*")
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
			props.setProperty(FlinkKafkaConsumer08.GET_PARTITIONS_RETRIES_KEY, "1");

			FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>(
				Collections.singletonList("no op topic"), new SimpleStringSchema(), props);
			StreamingRuntimeContext mockRuntimeContext = mock(StreamingRuntimeContext.class);
			Mockito.when(mockRuntimeContext.isCheckpointingEnabled()).thenReturn(true);
			consumer.setRuntimeContext(mockRuntimeContext);

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
			String unknownHost = "foobar:11111";

			URL unknownHostURL = NetUtils.getCorrectHostnamePort(unknownHost);

			PowerMockito.mockStatic(InetAddress.class);
			when(InetAddress.getByName(Matchers.eq(unknownHostURL.getHost()))).thenThrow(new UnknownHostException("Test exception"));

			String zookeeperConnect = "localhost:56794";
			String groupId = "non-existent-group";
			Properties props = createKafkaProps(zookeeperConnect, unknownHost, groupId);

			FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>(
				Collections.singletonList("no op topic"), new SimpleStringSchema(), props);
			StreamingRuntimeContext mockRuntimeContext = mock(StreamingRuntimeContext.class);
			Mockito.when(mockRuntimeContext.isCheckpointingEnabled()).thenReturn(true);
			consumer.setRuntimeContext(mockRuntimeContext);

			consumer.open(new Configuration());

			fail();
		} catch (Exception expected) {
			assertTrue("Exception should be thrown containing 'all bootstrap servers invalid' message!",
					expected.getMessage().contains("All the servers provided in: '" + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
							+ "' config are invalid"));
		}
	}

	@Test
	public void testAtLeastOneBootstrapServerHostIsValid() throws Exception {
		try {
			String zookeeperConnect = "localhost:56794";
			String unknownHost = "foobar:11111";
			// we declare one valid bootstrap server, namely the one with 'localhost'
			String bootstrapServers = unknownHost + ", localhost:22222";

			URL unknownHostURL = NetUtils.getCorrectHostnamePort(unknownHost);

			PowerMockito.mockStatic(InetAddress.class);
			when(InetAddress.getByName(Matchers.eq(unknownHostURL.getHost()))).thenThrow(new UnknownHostException("Test exception"));

			String groupId = "non-existent-group";
			Properties props = createKafkaProps(zookeeperConnect, bootstrapServers, groupId);
			DummyFlinkKafkaConsumer consumer = new DummyFlinkKafkaConsumer(
				"no op topic",
				new SimpleStringSchema(),
				props);
			consumer.open(new Configuration());

			// no exception should be thrown, because we have one valid bootstrap server; test passes if we reach here
		} catch (Exception e) {
			assertFalse("No exception should be thrown containing 'all bootstrap servers invalid' message!",
				e.getMessage().contains("All the servers provided in: '" + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
					+ "' config are invalid"));
		}
	}

	private Properties createKafkaProps(String zookeeperConnect, String bootstrapServers, String groupId) {
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnect);
		props.setProperty("bootstrap.servers", bootstrapServers);
		props.setProperty("group.id", groupId);
		props.setProperty("socket.timeout.ms", "100");
		props.setProperty(FlinkKafkaConsumer08.GET_PARTITIONS_RETRIES_KEY, "1");
		return props;
	}

	private static class DummyFlinkKafkaConsumer extends FlinkKafkaConsumer08<String> {

		private static final long serialVersionUID = -3939402845009972810L;

		public DummyFlinkKafkaConsumer(String topic, DeserializationSchema<String> schema, Properties props) {
			super(Collections.singletonList(topic), schema, props);
		}

		@Override
		public RuntimeContext getRuntimeContext() {
			RuntimeContext mockRuntimeContext = mock(RuntimeContext.class);
			when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(0);
			when(mockRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(1);
			return mockRuntimeContext;
		}
	}
}
