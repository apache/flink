/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent.service;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import kafka.server.KafkaConfig$;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance, 1 Kafka broker, and 1
 * Confluent Schema Registry instance.
 */
public class SingleNodeKafkaClusterEmbedded extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(SingleNodeKafkaClusterEmbedded.class);
	private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
	private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
	private static final String AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.NONE.name;

	private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "60000";
	private static final String KAFKASTORE_DEBUG = "true";
	private static final String KAFKASTORE_INIT_TIMEOUT = "90000";

	private ZooKeeperEmbedded zookeeper;
	private KafkaEmbedded broker;
	private RestApp schemaRegistry;
	private final Properties brokerConfig;
	private boolean running;

	/**
	 * Creates and starts the cluster.
	 */
	public SingleNodeKafkaClusterEmbedded() {
		this(new Properties());
	}

	/**
	 * Creates and starts the cluster.
	 *
	 * @param brokerConfig Additional broker configuration settings.
	 */
	public SingleNodeKafkaClusterEmbedded(final Properties brokerConfig) {
		this.brokerConfig = new Properties();
		this.brokerConfig.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
		this.brokerConfig.putAll(brokerConfig);
	}

	/**
	 * Creates and starts the cluster.
	 */
	public void start() throws Exception {
		LOG.debug("Initiating embedded Kafka cluster startup");
		LOG.debug("Starting a ZooKeeper instance...");
		zookeeper = new ZooKeeperEmbedded();
		LOG.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

		final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
		LOG.debug("Starting a Kafka instance on port {} ...",
				effectiveBrokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
		broker = new KafkaEmbedded(effectiveBrokerConfig);
		LOG.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
				broker.brokerList(), broker.zookeeperConnect());

		final Properties schemaRegistryProps = new Properties();

		schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
		schemaRegistryProps.put(SchemaRegistryConfig.DEBUG_CONFIG, KAFKASTORE_DEBUG);
		schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG, KAFKASTORE_INIT_TIMEOUT);

		schemaRegistry = new RestApp(0, zookeeperConnect(), KAFKA_SCHEMAS_TOPIC, AVRO_COMPATIBILITY_TYPE, schemaRegistryProps);
		schemaRegistry.start();
		running = true;
	}

	private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final ZooKeeperEmbedded zookeeper) {
		final Properties effectiveConfig = new Properties();
		effectiveConfig.putAll(brokerConfig);
		effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
		effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
		effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
		effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
		effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
		effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
		effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
		effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
		effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
		effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
		return effectiveConfig;
	}

	@Override
	protected void before() throws Exception {
		start();
	}

	@Override
	protected void after() {
		stop();
	}

	/**
	 * Stops the cluster.
	 */
	public void stop() {
		LOG.info("Stopping Confluent");
		try {
			try {
				if (schemaRegistry != null) {
					schemaRegistry.stop();
				}
			} catch (final Exception fatal) {
				throw new RuntimeException(fatal);
			}
			if (broker != null) {
				broker.stop();
			}
			try {
				if (zookeeper != null) {
					zookeeper.stop();
				}
			} catch (final IOException fatal) {
				throw new RuntimeException(fatal);
			}
		} finally {
			running = false;
		}
		LOG.info("Confluent Stopped");
	}

	/**
	 * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
	 * Example: `127.0.0.1:2181`.
	 *
	 * <p>You can use this to e.g. tell Kafka consumers (old consumer API) how to connect to this
	 * cluster.
	 */
	public String zookeeperConnect() {
		return zookeeper.connectString();
	}

	/**
	 * The "schema.registry.url" setting of the schema registry instance.
	 */
	public String schemaRegistryUrl() {
		return schemaRegistry.restConnect;
	}
}
