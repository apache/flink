package org.apache.flink.connectors.test.kafka.external;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Single topic. */
public class KafkaSingleTopicExternalContext implements ExternalContext<String> {

    protected String bootstrapServers;
    private static final String TOPIC_NAME_PREFIX = "kafka-single-topic";

    private final String topicName;

    private static final int DEFAULT_TIMEOUT = 10;

    private final Map<Integer, SourceSplitDataWriter<String>> partitionToSplitWriter =
            new HashMap<>();

    private int numSplits = 0;

    protected final AdminClient kafkaAdminClient;

    public KafkaSingleTopicExternalContext(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.topicName =
                TOPIC_NAME_PREFIX + "-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        kafkaAdminClient = createAdminClient();
    }

    protected void createTopic(String topicName, int numPartitions, short replicationFactor) {
        // Make sure Kafka container is running
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        try {
            kafkaAdminClient
                    .createTopics(Collections.singletonList(newTopic))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Cannot create topic '%s'", topicName), e);
        }
    }

    protected void deleteTopic(String topicName) {
        try {
            kafkaAdminClient
                    .deleteTopics(Collections.singletonList(topicName))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof UnknownTopicOrPartitionException) {
                throw new RuntimeException(String.format("Cannot delete topic '%s'", topicName), e);
            }
        }
    }

    private AdminClient createAdminClient() {
        Properties config = new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(config);
    }

    @Override
    public Source<String, ?, ?> createSource() {
        return KafkaSource.<String>builder()
                .setUnbounded(OffsetsInitializer.latest())
                .setGroupId("flink-kafka-test")
                .setDeserializer(KafkaRecordDeserializer.valueOnly(StringDeserializer.class))
                .setTopics(topicName)
                .setBootstrapServers(bootstrapServers)
                .build();
    }

    @Override
    public Sink<String, ?, ?, ?> createSink() {
        throw new UnsupportedOperationException(
                "Kafka connector didn't implement the new sink interface.");
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplit() {
        if (numSplits == 0) {
            createTopic(topicName, 1, (short) 1);
            numSplits++;
        } else {
            kafkaAdminClient.createPartitions(
                    Collections.singletonMap(topicName, NewPartitions.increaseTo(++numSplits)));
        }
        KafkaSourceSplitDataWriter splitWriter =
                new KafkaSourceSplitDataWriter(
                        getKafkaProducerProperties(numSplits - 1),
                        new TopicPartition(topicName, numSplits - 1),
                        null); // TODO: Here just use null temporarily
        partitionToSplitWriter.put(numSplits - 1, splitWriter);
        return splitWriter;
    }

    @Override
    public Collection<String> generateTestRecords() {
        List<String> randomStringRecords = new ArrayList<>();
        int recordNum = ThreadLocalRandom.current().nextInt(100, 200);
        for (int i = 0; i < recordNum; i++) {
            int stringLength = ThreadLocalRandom.current().nextInt(100, 200);
            randomStringRecords.add(generateRandomString(stringLength));
        }
        return randomStringRecords;
    }

    private String generateRandomString(int length) {
        String alphaNumericString =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; ++i) {
            sb.append(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
        }
        return sb.toString();
    }

    protected Properties getKafkaProducerProperties(int producerId) {
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProducerProperties.setProperty(
                ProducerConfig.CLIENT_ID_CONFIG, "flink-kafka-split-writer-" + producerId);
        kafkaProducerProperties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProducerProperties;
    }

    @Override
    public void close() throws Exception {
        deleteTopic(topicName);
        partitionToSplitWriter.forEach(
                (partitionId, splitWriter) -> {
                    try {
                        splitWriter.close();
                    } catch (Exception e) {
                        kafkaAdminClient.close();
                        throw new RuntimeException("Cannot close split writer", e);
                    }
                });
        partitionToSplitWriter.clear();
        kafkaAdminClient.close();
    }
}
