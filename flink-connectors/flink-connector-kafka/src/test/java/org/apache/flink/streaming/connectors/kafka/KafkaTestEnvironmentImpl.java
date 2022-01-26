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
import org.apache.flink.connector.kafka.testutils.cluster.KafkaContainers;
import org.apache.flink.connector.kafka.testutils.extension.KafkaExtension;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

import java.time.Duration;
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

    public KafkaExtension kafkaExtension;

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestEnvironmentImpl.class);
    private final Set<Integer> pausedBroker = new HashSet<>();
    private FlinkKafkaProducer.Semantic producerSemantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
    // 6 seconds is default. Seems to be too small for travis. 30 seconds
    private int zkTimeout = 30000;
    private Config config;
    private static final int DELETE_TIMEOUT_SECONDS = 30;

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
        kafkaExtension =
                new KafkaExtension(
                        KafkaContainers.builder()
                                .setNumBrokers(config.getKafkaServersNumber())
                                .setBrokerProperty(
                                        "message.max.bytes", String.valueOf(50 * 1024 * 1024))
                                .setBrokerProperty(
                                        "replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024))
                                .setBrokerProperty(
                                        "transaction.max.timeout.ms",
                                        Integer.toString(1000 * 60 * 60 * 2))
                                // Disable log deletion to prevent records from being deleted during
                                // test run
                                .setBrokerProperty("log.retention.ms", "-1")
                                // for CI stability, increase zookeeper session timeout
                                .setBrokerProperty(
                                        "zookeeper.session.timeout.ms", String.valueOf(zkTimeout))
                                .setBrokerProperty(
                                        "zookeeper.connection.timeout.ms",
                                        String.valueOf(zkTimeout))
                                .setLogger(null)
                                .build());

        kafkaExtension.startKafkaCluster();

        LOG.info("Kafka cluster started.");
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
                    .get(DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.info(
                    "Did not receive delete topic response within {} seconds. Checking if it succeeded",
                    DELETE_TIMEOUT_SECONDS);
            if (adminClient
                    .listTopics()
                    .names()
                    .get(DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .contains(topic)) {
                throw new Exception("Topic still exists after timeout");
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
            try (KafkaConsumer<Void, Void> consumer =
                    kafkaExtension
                            .createKafkaClientKit()
                            .createConsumer(VoidDeserializer.class, VoidDeserializer.class)) {
                CommonTestUtils.waitUtil(
                        () -> {
                            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                            return partitionInfos.size() == numberOfPartitions;
                        },
                        Duration.ofSeconds(30),
                        String.format("New topic \"%s\" is not ready with timeout", topicObj));
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Create test topic : " + topic + " failed, " + e.getMessage());
        }
    }

    @Override
    public Properties getStandardProperties() {
        Properties standardProps = new Properties();
        standardProps.setProperty("bootstrap.servers", getBrokerConnectionString());
        standardProps.setProperty("group.id", "flink-tests");
        standardProps.setProperty("enable.auto.commit", "false");
        standardProps.setProperty("zookeeper.session.timeout.ms", String.valueOf(zkTimeout));
        standardProps.setProperty("zookeeper.connection.timeout.ms", String.valueOf(zkTimeout));
        standardProps.setProperty("auto.offset.reset", "earliest"); // read from the beginning.
        standardProps.setProperty(
                "max.partition.fetch.bytes",
                "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)
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
        return kafkaExtension.getKafkaContainers().getKafkaContainers().values().stream()
                .map(container -> container.getBootstrapServers().split("://")[1])
                .collect(Collectors.joining(","));
    }

    @Override
    public String getVersion() {
        return "2.0";
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
        kafkaExtension.stopKafkaCluster();
    }

    private void pause(int brokerId) throws Exception {
        if (pausedBroker.contains(brokerId)) {
            return;
        }
        DockerClientFactory.instance()
                .client()
                .pauseContainerCmd(
                        kafkaExtension
                                .getKafkaContainers()
                                .getKafkaContainer(brokerId)
                                .getContainerId())
                .exec();
        pausedBroker.add(brokerId);
    }

    private void unpause(int brokerId) throws Exception {
        if (!pausedBroker.contains(brokerId)) {
            throw new IllegalStateException(
                    String.format("Broker %d is already running", brokerId));
        }
        DockerClientFactory.instance()
                .client()
                .unpauseContainerCmd(
                        kafkaExtension
                                .getKafkaContainers()
                                .getKafkaContainer(brokerId)
                                .getContainerId())
                .exec();
        CommonTestUtils.waitUtil(
                () -> {
                    AdminClient adminClient =
                            kafkaExtension.createKafkaClientKit().getAdminClient();
                    try {
                        return adminClient.describeCluster().nodes().get().stream()
                                .anyMatch((node) -> node.id() == brokerId);
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofSeconds(30),
                String.format("The paused broker %d is not recovered within timeout", brokerId));
        pausedBroker.remove(brokerId);
    }

    private class KafkaOffsetHandlerImpl implements KafkaOffsetHandler {

        private final KafkaConsumer<byte[], byte[]> offsetClient;

        public KafkaOffsetHandlerImpl() {
            Properties props = new Properties();
            props.putAll(getStandardProperties());
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
}
