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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.DockerImageVersions;

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/** An implementation of the KafkaServerProvider. */
public class KafkaTestEnvironmentImpl extends KafkaTestEnvironment {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestEnvironmentImpl.class);

    private static final String ZOOKEEPER_HOSTNAME = "zookeeper";
    private static final int ZOOKEEPER_PORT = 2181;

    private final Map<Integer, KafkaContainer> brokers = new HashMap<>();
    private final Set<Integer> pausedBroker = new HashSet<>();
    private @Nullable GenericContainer<?> zookeeper;
    private String brokerConnectionString = "";
    private Properties standardProps;
    private FlinkKafkaProducer.Semantic producerSemantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
    // 6 seconds is default. Seems to be too small for travis. 30 seconds
    private int zkTimeout = 30000;
    private Config config;
    private static final int REQUEST_TIMEOUT_SECONDS = 30;

    public void setProducerSemantic(FlinkKafkaProducer.Semantic producerSemantic) {
        this.producerSemantic = producerSemantic;
    }

    @Override
    public void prepare(Config config) throws Exception {
        // increase the timeout since in Travis ZK connection takes long time for secure connection.
        if (config.isSecureMode()) {
            // run only one kafka server to avoid multiple ZK connections from many instances -
            // Travis timeout
            config.setKafkaServersNumber(1);
            zkTimeout = zkTimeout * 15;
        }
        this.config = config;
        brokers.clear();

        LOG.info("Starting KafkaServer");
        startKafkaContainerCluster(config.getKafkaServersNumber());
        LOG.info("KafkaServer started.");

        standardProps = new Properties();
        standardProps.setProperty("bootstrap.servers", brokerConnectionString);
        standardProps.setProperty("group.id", "flink-tests");
        standardProps.setProperty("enable.auto.commit", "false");
        standardProps.setProperty("zookeeper.session.timeout.ms", String.valueOf(zkTimeout));
        standardProps.setProperty("zookeeper.connection.timeout.ms", String.valueOf(zkTimeout));
        standardProps.setProperty("auto.offset.reset", "earliest"); // read from the beginning.
        standardProps.setProperty(
                "max.partition.fetch.bytes",
                "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)
    }

    @Override
    public void deleteTestTopic(String topic) {
        LOG.info("Deleting topic {}", topic);
        Properties props = getSecureProperties();
        props.putAll(getStandardProperties());
        String clientId = Long.toString(new Random().nextLong());
        props.put("client.id", clientId);
        AdminClient adminClient = AdminClient.create(props);
        // We do not use a try-catch clause here so we can apply a timeout to the admin client
        // closure.
        try {
            tryDelete(adminClient, topic);
        } catch (Exception e) {
            e.printStackTrace();
            fail(String.format("Delete test topic : %s failed, %s", topic, e.getMessage()));
        } finally {
            adminClient.close(Duration.ofMillis(5000L));
            maybePrintDanglingThreadStacktrace(clientId);
        }
    }

    private void tryDelete(AdminClient adminClient, String topic) throws Exception {
        try {
            adminClient
                    .deleteTopics(Collections.singleton(topic))
                    .all()
                    .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            return adminClient.listTopics().listings()
                                    .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS).stream()
                                    .map(TopicListing::name)
                                    .noneMatch((name) -> name.equals(topic));
                        } catch (Exception e) {
                            LOG.warn("Exception caught when listing Kafka topics", e);
                            return false;
                        }
                    },
                    Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS),
                    String.format("Topic \"%s\" was not deleted within timeout", topic));
        } catch (TimeoutException e) {
            LOG.info(
                    "Did not receive delete topic response within {} seconds. Checking if it succeeded",
                    REQUEST_TIMEOUT_SECONDS);
            if (adminClient
                    .listTopics()
                    .names()
                    .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .contains(topic)) {
                throw new Exception("Topic still exists after timeout", e);
            }
        }
    }

    @Override
    public void createTestTopic(
            String topic, int numberOfPartitions, int replicationFactor, Properties properties) {
        LOG.info("Creating topic {}", topic);
        try (AdminClient adminClient = AdminClient.create(getStandardProperties())) {
            NewTopic topicObj = new NewTopic(topic, numberOfPartitions, (short) replicationFactor);
            adminClient.createTopics(Collections.singleton(topicObj)).all().get();
            CommonTestUtils.waitUtil(
                    () -> {
                        Map<String, TopicDescription> topicDescriptions;
                        try {
                            topicDescriptions =
                                    adminClient
                                            .describeTopics(Collections.singleton(topic))
                                            .all()
                                            .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            LOG.warn("Exception caught when describing Kafka topics", e);
                            return false;
                        }
                        if (topicDescriptions == null || !topicDescriptions.containsKey(topic)) {
                            return false;
                        }
                        TopicDescription topicDescription = topicDescriptions.get(topic);
                        return topicDescription.partitions().size() == numberOfPartitions;
                    },
                    Duration.ofSeconds(30),
                    String.format("New topic \"%s\" is not ready within timeout", topicObj));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Create test topic : " + topic + " failed, " + e.getMessage());
        }
    }

    @Override
    public Properties getStandardProperties() {
        return standardProps;
    }

    @Override
    public Properties getSecureProperties() {
        Properties prop = new Properties();
        if (config.isSecureMode()) {
            prop.put("security.inter.broker.protocol", "SASL_PLAINTEXT");
            prop.put("security.protocol", "SASL_PLAINTEXT");
            prop.put("sasl.kerberos.service.name", "kafka");

            // add special timeout for Travis
            prop.setProperty("zookeeper.session.timeout.ms", String.valueOf(zkTimeout));
            prop.setProperty("zookeeper.connection.timeout.ms", String.valueOf(zkTimeout));
            prop.setProperty("metadata.fetch.timeout.ms", "120000");
        }
        return prop;
    }

    @Override
    public String getBrokerConnectionString() {
        return brokerConnectionString;
    }

    @Override
    public String getVersion() {
        return DockerImageVersions.KAFKA;
    }

    @Override
    public <T> FlinkKafkaConsumerBase<T> getConsumer(
            List<String> topics, KafkaDeserializationSchema<T> readSchema, Properties props) {
        return new FlinkKafkaConsumer<T>(topics, readSchema, props);
    }

    @Override
    public <T> KafkaSourceBuilder<T> getSourceBuilder(
            List<String> topics, KafkaDeserializationSchema<T> schema, Properties props) {
        return KafkaSource.<T>builder()
                .setTopics(topics)
                .setDeserializer(KafkaRecordDeserializationSchema.of(schema))
                .setProperties(props);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Collection<ConsumerRecord<K, V>> getAllRecordsFromTopic(
            Properties properties, String topic) {
        return UnmodifiableList.decorate(KafkaUtil.drainAllRecordsFromTopic(topic, properties));
    }

    @Override
    public <T> StreamSink<T> getProducerSink(
            String topic,
            SerializationSchema<T> serSchema,
            Properties props,
            FlinkKafkaPartitioner<T> partitioner) {
        return new StreamSink<>(
                new FlinkKafkaProducer<>(
                        topic,
                        serSchema,
                        props,
                        partitioner,
                        producerSemantic,
                        FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE));
    }

    @Override
    public <T> DataStreamSink<T> produceIntoKafka(
            DataStream<T> stream,
            String topic,
            KeyedSerializationSchema<T> serSchema,
            Properties props,
            FlinkKafkaPartitioner<T> partitioner) {
        return stream.addSink(
                new FlinkKafkaProducer<T>(
                        topic,
                        serSchema,
                        props,
                        Optional.ofNullable(partitioner),
                        producerSemantic,
                        FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE));
    }

    @Override
    public <T> DataStreamSink<T> produceIntoKafka(
            DataStream<T> stream,
            String topic,
            SerializationSchema<T> serSchema,
            Properties props,
            FlinkKafkaPartitioner<T> partitioner) {
        return stream.addSink(
                new FlinkKafkaProducer<T>(
                        topic,
                        serSchema,
                        props,
                        partitioner,
                        producerSemantic,
                        FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE));
    }

    @Override
    public <T> DataStreamSink<T> produceIntoKafka(
            DataStream<T> stream,
            String topic,
            KafkaSerializationSchema<T> serSchema,
            Properties props) {
        return stream.addSink(new FlinkKafkaProducer<T>(topic, serSchema, props, producerSemantic));
    }

    @Override
    public KafkaOffsetHandler createOffsetHandler() {
        return new KafkaOffsetHandlerImpl();
    }

    @Override
    public void restartBroker(int leaderId) throws Exception {
        unpause(leaderId);
    }

    @Override
    public void stopBroker(int brokerId) throws Exception {
        pause(brokerId);
    }

    @Override
    public int getLeaderToShutDown(String topic) throws Exception {
        try (final AdminClient client = AdminClient.create(getStandardProperties())) {
            TopicDescription result =
                    client.describeTopics(Collections.singleton(topic)).all().get().get(topic);
            return result.partitions().get(0).leader().id();
        }
    }

    @Override
    public boolean isSecureRunSupported() {
        return true;
    }

    @Override
    public void shutdown() throws Exception {
        brokers.values().forEach(GenericContainer::stop);
        brokers.clear();

        if (zookeeper != null) {
            zookeeper.stop();
        }
    }

    private class KafkaOffsetHandlerImpl implements KafkaOffsetHandler {

        private final KafkaConsumer<byte[], byte[]> offsetClient;

        public KafkaOffsetHandlerImpl() {
            Properties props = new Properties();
            props.putAll(standardProps);
            props.setProperty(
                    "key.deserializer",
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(
                    "value.deserializer",
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");

            offsetClient = new KafkaConsumer<>(props);
        }

        @Override
        public Long getCommittedOffset(String topicName, int partition) {
            OffsetAndMetadata committed =
                    offsetClient.committed(new TopicPartition(topicName, partition));
            return (committed != null) ? committed.offset() : null;
        }

        @Override
        public void setCommittedOffset(String topicName, int partition, long offset) {
            Map<TopicPartition, OffsetAndMetadata> partitionAndOffset = new HashMap<>();
            partitionAndOffset.put(
                    new TopicPartition(topicName, partition), new OffsetAndMetadata(offset));
            offsetClient.commitSync(partitionAndOffset);
        }

        @Override
        public void close() {
            offsetClient.close();
        }
    }

    private void startKafkaContainerCluster(int numBrokers) {
        Network network = Network.newNetwork();
        if (numBrokers > 1) {
            zookeeper = createZookeeperContainer(network);
            zookeeper.start();
            LOG.info("Zookeeper container started");
        }
        for (int brokerID = 0; brokerID < numBrokers; brokerID++) {
            KafkaContainer broker = createKafkaContainer(network, brokerID, zookeeper);
            brokers.put(brokerID, broker);
        }
        new ArrayList<>(brokers.values()).parallelStream().forEach(GenericContainer::start);
        LOG.info("{} brokers started", numBrokers);
        brokerConnectionString =
                brokers.values().stream()
                        .map(KafkaContainer::getBootstrapServers)
                        // Here we have URL like "PLAINTEXT://127.0.0.1:15213", and we only keep the
                        // "127.0.0.1:15213" part in broker connection string
                        .map(server -> server.split("://")[1])
                        .collect(Collectors.joining(","));
    }

    private GenericContainer<?> createZookeeperContainer(Network network) {
        return new GenericContainer<>(DockerImageName.parse(DockerImageVersions.ZOOKEEPER))
                .withNetwork(network)
                .withNetworkAliases(ZOOKEEPER_HOSTNAME)
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT));
    }

    private KafkaContainer createKafkaContainer(
            Network network, int brokerID, @Nullable GenericContainer<?> zookeeper) {
        String brokerName = String.format("Kafka-%d", brokerID);
        KafkaContainer broker =
                KafkaUtil.createKafkaContainer(DockerImageVersions.KAFKA, LOG, brokerName)
                        .withNetwork(network)
                        .withNetworkAliases(brokerName)
                        .withEnv("KAFKA_BROKER_ID", String.valueOf(brokerID))
                        .withEnv("KAFKA_MESSAGE_MAX_BYTES", String.valueOf(50 * 1024 * 1024))
                        .withEnv("KAFKA_REPLICA_FETCH_MAX_BYTES", String.valueOf(50 * 1024 * 1024))
                        .withEnv(
                                "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                                Integer.toString(1000 * 60 * 60 * 2))
                        // Disable log deletion to prevent records from being deleted during test
                        // run
                        .withEnv("KAFKA_LOG_RETENTION_MS", "-1")
                        .withEnv("KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS", String.valueOf(zkTimeout))
                        .withEnv(
                                "KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS", String.valueOf(zkTimeout));

        if (zookeeper != null) {
            broker.dependsOn(zookeeper)
                    .withExternalZookeeper(
                            String.format("%s:%d", ZOOKEEPER_HOSTNAME, ZOOKEEPER_PORT));
        } else {
            broker.withEmbeddedZookeeper();
        }
        return broker;
    }

    private void pause(int brokerId) {
        if (pausedBroker.contains(brokerId)) {
            LOG.warn("Broker {} is already paused. Skipping pause operation", brokerId);
            return;
        }
        DockerClientFactory.instance()
                .client()
                .pauseContainerCmd(brokers.get(brokerId).getContainerId())
                .exec();
        pausedBroker.add(brokerId);
        LOG.info("Broker {} is paused", brokerId);
    }

    private void unpause(int brokerId) throws Exception {
        if (!pausedBroker.contains(brokerId)) {
            LOG.warn("Broker {} is already running. Skipping unpause operation", brokerId);
            return;
        }
        DockerClientFactory.instance()
                .client()
                .unpauseContainerCmd(brokers.get(brokerId).getContainerId())
                .exec();
        try (AdminClient adminClient = AdminClient.create(getStandardProperties())) {
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            return adminClient.describeCluster().nodes().get().stream()
                                    .anyMatch((node) -> node.id() == brokerId);
                        } catch (Exception e) {
                            return false;
                        }
                    },
                    Duration.ofSeconds(30),
                    String.format(
                            "The paused broker %d is not recovered within timeout", brokerId));
        }
        pausedBroker.remove(brokerId);
        LOG.info("Broker {} is resumed", brokerId);
    }
}
