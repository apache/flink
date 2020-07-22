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

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import scala.collection.mutable.ArraySeq;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 *
 * <p>Requires a running ZooKeeper instance to connect to.  By default,
 * it expects a ZooKeeper instance running at `127.0.0.1:2181`.
 * You can specify a different ZooKeeper instance by setting the
 * `zookeeper.connect` parameter in the broker's configuration.
 */
public class KafkaEmbedded {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaEmbedded.class);

	private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";

	private final Properties effectiveConfig;
	private final File logDir;
	private final TemporaryFolder tmpFolder;
	private final KafkaServer kafka;

	/**
	 * Creates and starts an embedded Kafka broker.
	 *
	 * @param config Broker configuration settings.  Used to modify, for example, on which port the
	 *               broker should listen to.  Note that you cannot change some settings such as
	 *               `log.dirs`, `port`.
	 */
	public KafkaEmbedded(final Properties config) throws IOException {
		tmpFolder = new TemporaryFolder();
		tmpFolder.create();
		logDir = tmpFolder.newFolder();
		effectiveConfig = effectiveConfigFrom(config);
		final boolean loggingEnabled = true;

		final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
		LOG.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
				logDir, zookeeperConnect());
		kafka = createServer(kafkaConfig);
		LOG.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
				brokerList(), zookeeperConnect());
	}

	/**
	 * Create a kafka server instance with appropriate test settings.
	 *
	 * @param config The configuration of the server
	 */
	private KafkaServer createServer(KafkaConfig config) {
		scala.Option<String> stringNone = scala.Option.apply(null);
		KafkaServer server = new KafkaServer(
				config,
				Time.SYSTEM,
				stringNone,
				new ArraySeq<KafkaMetricsReporter>(0));
		server.startup();
		return server;
	}

	private Properties effectiveConfigFrom(final Properties initialConfig) throws IOException {
		final Properties effectiveConfig = new Properties();
		effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
		effectiveConfig.put(KafkaConfig$.MODULE$.HostNameProp(), "127.0.0.1");
		effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), "9092");
		effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
		effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
		effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
		effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);

		effectiveConfig.putAll(initialConfig);
		effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());
		return effectiveConfig;
	}

	/**
	 * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
	 *
	 * <p>You can use this to tell Kafka producers and consumers how to connect to this instance.
	 */
	public String brokerList() {
		return String.join(
				":",
				kafka.config().hostName(),
				Integer.toString(
						kafka.boundPort(
								ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))));
	}

	/**
	 * The ZooKeeper connection string aka `zookeeper.connect`.
	 */
	public String zookeeperConnect() {
		return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
	}

	/**
	 * Stop the broker.
	 */
	public void stop() {
		LOG.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
				brokerList(), zookeeperConnect());
		kafka.shutdown();
		kafka.awaitShutdown();
		LOG.debug("Removing temp folder {} with logs.dir at {} ...", tmpFolder, logDir);
		tmpFolder.delete();
		LOG.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
				brokerList(), zookeeperConnect());
	}

}
