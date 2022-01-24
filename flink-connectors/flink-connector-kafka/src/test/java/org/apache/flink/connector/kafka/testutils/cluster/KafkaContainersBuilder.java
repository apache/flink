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

package org.apache.flink.connector.kafka.testutils.cluster;

import org.apache.flink.util.DockerImageVersions;

import org.apache.kafka.common.config.LogLevelConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Builder class of {@link KafkaContainers}. */
public class KafkaContainersBuilder {
    private static final String ZOOKEEPER_HOSTNAME = "zookeeper";
    private static final String ZOOKEEPER_PORT = "2181";

    private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();
    private final Map<String, String> envVars = new HashMap<>();
    private final Properties brokerProps = new Properties();

    private String networkAliasPrefix = "kafka";
    private String dockerImageVersion = DockerImageVersions.KAFKA;
    private int numBrokers = 1;
    private Network network = Network.newNetwork();
    @Nullable private Logger logger = LoggerFactory.getLogger(KafkaContainers.class);
    private String logLevel = LogLevelConfig.INFO_LOG_LEVEL;

    /** Set number of brokers. Will start 1 broker by default. */
    public KafkaContainersBuilder setNumBrokers(int numBrokers) {
        this.numBrokers = numBrokers;
        return this;
    }

    /** Set container network of the cluster. */
    public KafkaContainersBuilder setNetwork(Network network) {
        this.network = network;
        return this;
    }

    /**
     * Set network alias prefix of Kafka brokers. The actual network alias will be
     * "{prefix}-{brokerID}".
     */
    public KafkaContainersBuilder setNetworkAliasPrefix(String networkAliasPrefix) {
        this.networkAliasPrefix = networkAliasPrefix;
        return this;
    }

    /** Set a property to brokers. */
    public KafkaContainersBuilder setBrokerProperty(String key, String value) {
        this.brokerProps.setProperty(key, value);
        return this;
    }

    /** Set properties to brokers. */
    public KafkaContainersBuilder setBrokerProperties(Properties brokerProps) {
        this.brokerProps.putAll(brokerProps);
        return this;
    }

    /** Set docker image version of Kafka. Will use {@link DockerImageVersions#KAFKA} as default. */
    public KafkaContainersBuilder setDockerImageVersion(String dockerImageVersion) {
        this.dockerImageVersion = dockerImageVersion;
        return this;
    }

    /** Sets environment variable to containers. */
    public KafkaContainersBuilder setEnvironmentVariable(String key, String value) {
        this.envVars.put(key, value);
        return this;
    }

    /** Set dependent container. */
    public KafkaContainersBuilder dependsOn(GenericContainer<?> container) {
        container.withNetwork(this.network);
        this.dependentContainers.add(container);
        return this;
    }

    /**
     * Sets a logger to the cluster in order to consume STDOUT of containers to the logger. If the
     * logger is not specified, the container
     */
    public KafkaContainersBuilder setLogger(Logger logger) {
        this.logger = logger;
        if (logger == null) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logLevel = LogLevelConfig.TRACE_LOG_LEVEL;
        } else if (logger.isDebugEnabled()) {
            logLevel = LogLevelConfig.DEBUG_LOG_LEVEL;
        } else if (logger.isInfoEnabled()) {
            logLevel = LogLevelConfig.INFO_LOG_LEVEL;
        } else if (logger.isWarnEnabled()) {
            logLevel = LogLevelConfig.WARN_LOG_LEVEL;
        } else if (logger.isErrorEnabled()) {
            logLevel = LogLevelConfig.ERROR_LOG_LEVEL;
        } else {
            logLevel = LogLevelConfig.FATAL_LOG_LEVEL;
        }
        return this;
    }

    public KafkaContainers build() {
        configureBrokers();
        Map<Integer, KafkaContainer> kafkaContainers = new HashMap<>();
        GenericContainer<?> zookeeper = createZookeeperIfNecessary();
        for (int brokerID = 0; brokerID < numBrokers; brokerID++) {
            KafkaContainer kafkaContainer =
                    configureContainer(
                            new KafkaContainer(DockerImageName.parse(dockerImageVersion)),
                            networkAliasPrefix + "-" + brokerID,
                            "Kafka-" + brokerID);
            kafkaContainer.withEnv("KAFKA_BROKER_ID", String.valueOf(brokerID));
            if (zookeeper != null) {
                kafkaContainer.dependsOn(zookeeper);
                kafkaContainer.withExternalZookeeper(ZOOKEEPER_HOSTNAME + ":" + ZOOKEEPER_PORT);
            } else {
                kafkaContainer.withEmbeddedZookeeper();
            }
            kafkaContainers.put(brokerID, kafkaContainer);
        }
        return new KafkaContainers(kafkaContainers, zookeeper);
    }

    private void configureBrokers() {
        brokerProps.setProperty("transaction.state.log.replication.factor", "1");
        brokerProps.setProperty("transaction.state.log.min.isr", "1");
        brokerProps.setProperty(
                "transaction.max.timeout.ms", String.valueOf(Duration.ofHours(2).toMillis()));
        brokerProps.setProperty("confluent.support.metrics.enable", "false");
        if (logger != null) {
            brokerProps.setProperty("log4j.root.loglevel", logLevel);
            brokerProps.setProperty("log4j.loggers", "state.change.logger=" + logLevel);
            brokerProps.setProperty("log4j.tools.root.loglevel", logLevel);
        }
        // Rules of converting Kafka properties to environment variables:
        // https://docs.confluent.io/platform/current/installation/docker/config-reference.html#confluent-ak-configuration
        brokerProps.forEach(
                (key, value) -> {
                    String envKey =
                            "KAFKA_"
                                    + ((String) key)
                                            .replace("_", "___")
                                            .replace("-", "__")
                                            .replace(".", "_");
                    envVars.put(envKey, (String) value);
                });
    }

    private GenericContainer<?> createZookeeperIfNecessary() {
        if (numBrokers == 1) {
            return null;
        }
        GenericContainer<?> zookeeper =
                configureContainer(
                        new GenericContainer<>(
                                DockerImageName.parse("zookeeper").withTag("3.4.14")),
                        ZOOKEEPER_HOSTNAME,
                        "Zookeeper");
        zookeeper.withEnv("ZOOKEEPER_CLIENT_PORT", ZOOKEEPER_PORT);
        return zookeeper;
    }

    private <T extends GenericContainer<?>> T configureContainer(
            T container, String networkAlias, String logPrefix) {
        // Set dependent containers
        for (GenericContainer<?> dependentContainer : dependentContainers) {
            container.dependsOn(dependentContainer);
        }
        // Setup network
        container.withNetwork(network);
        container.withNetworkAliases(networkAlias);
        // Setup logger
        if (logger != null) {
            container.withLogConsumer(new Slf4jLogConsumer(logger).withPrefix(logPrefix));
        }
        // Add environment variables
        container.withEnv(envVars);
        return container;
    }
}
