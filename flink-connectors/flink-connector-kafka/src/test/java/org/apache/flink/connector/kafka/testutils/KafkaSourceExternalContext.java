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

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/** External context for testing {@link KafkaSource}. */
public class KafkaSourceExternalContext implements DataStreamSourceExternalContext<String> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceExternalContext.class);
    private static final String TOPIC_NAME_PREFIX = "kafka-test-topic-";
    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile(TOPIC_NAME_PREFIX + ".*");
    private static final String GROUP_ID_PREFIX = "kafka-source-external-context-";
    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    private final List<URL> connectorJarPaths;
    private final String bootstrapServers;
    private final String topicName;
    private final SplitMappingMode splitMappingMode;
    private final AdminClient adminClient;
    private final List<KafkaPartitionDataWriter> writers = new ArrayList<>();

    protected KafkaSourceExternalContext(
            String bootstrapServers,
            SplitMappingMode splitMappingMode,
            List<URL> connectorJarPaths) {
        this.connectorJarPaths = connectorJarPaths;
        this.bootstrapServers = bootstrapServers;
        this.topicName = randomize(TOPIC_NAME_PREFIX);
        this.splitMappingMode = splitMappingMode;
        this.adminClient = createAdminClient();
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return this.connectorJarPaths;
    }

    @Override
    public Source<String, ?, ?> createSource(TestingSourceSettings sourceSettings) {
        final KafkaSourceBuilder<String> builder = KafkaSource.builder();

        builder.setBootstrapServers(bootstrapServers)
                .setTopicPattern(TOPIC_NAME_PATTERN)
                .setGroupId(randomize(GROUP_ID_PREFIX))
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));

        if (sourceSettings.getBoundedness().equals(Boundedness.BOUNDED)) {
            builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.build();
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        KafkaPartitionDataWriter writer;
        try {
            switch (splitMappingMode) {
                case TOPIC:
                    writer = createSinglePartitionTopic(writers.size());
                    break;
                case PARTITION:
                    writer = scaleOutTopic(this.topicName);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Split mode should be either TOPIC or PARTITION");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create new splits", e);
        }
        writers.add(writer);
        return writer;
    }

    @Override
    public List<String> generateTestData(
            TestingSourceSettings sourceSettings, int splitIndex, long seed) {
        Random random = new Random(seed);
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        List<String> records = new ArrayList<>(recordNum);

        for (int i = 0; i < recordNum; i++) {
            int stringLength = random.nextInt(50) + 1;
            records.add(splitIndex + "-" + randomAlphanumeric(stringLength));
        }

        return records;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public void close() throws Exception {
        final List<String> topics = new ArrayList<>();
        writers.forEach(
                writer -> {
                    topics.add(writer.getTopicPartition().topic());
                    writer.close();
                });
        adminClient.deleteTopics(topics).all().get();
    }

    @Override
    public String toString() {
        return "KafkaSource-" + splitMappingMode.toString();
    }

    private String randomize(String prefix) {
        return prefix + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
    }

    private AdminClient createAdminClient() {
        Properties config = new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(config);
    }

    private KafkaPartitionDataWriter createSinglePartitionTopic(int topicIndex) throws Exception {
        String newTopicName = topicName + "-" + topicIndex;
        LOG.info("Creating topic '{}'", newTopicName);
        adminClient
                .createTopics(Collections.singletonList(new NewTopic(newTopicName, 1, (short) 1)))
                .all()
                .get();
        return new KafkaPartitionDataWriter(
                getKafkaProducerProperties(topicIndex), new TopicPartition(newTopicName, 0));
    }

    private KafkaPartitionDataWriter scaleOutTopic(String topicName) throws Exception {
        final Set<String> topics = adminClient.listTopics().names().get();
        if (topics.contains(topicName)) {
            final Map<String, TopicDescription> topicDescriptions =
                    adminClient.describeTopics(Collections.singletonList(topicName)).all().get();
            final int numPartitions = topicDescriptions.get(topicName).partitions().size();
            LOG.info("Creating partition {} for topic '{}'", numPartitions + 1, topicName);
            adminClient
                    .createPartitions(
                            Collections.singletonMap(
                                    topicName, NewPartitions.increaseTo(numPartitions + 1)))
                    .all()
                    .get();
            return new KafkaPartitionDataWriter(
                    getKafkaProducerProperties(numPartitions),
                    new TopicPartition(topicName, numPartitions));
        } else {
            LOG.info("Creating topic '{}'", topicName);
            adminClient.createTopics(
                    Collections.singletonList(new NewTopic(topicName, 1, (short) 1)));
            return new KafkaPartitionDataWriter(
                    getKafkaProducerProperties(0), new TopicPartition(topicName, 0));
        }
    }

    private Properties getKafkaProducerProperties(int producerId) {
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProducerProperties.setProperty(
                ProducerConfig.CLIENT_ID_CONFIG,
                String.join(
                        "-",
                        "flink-kafka-split-writer",
                        Integer.toString(producerId),
                        Long.toString(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))));
        kafkaProducerProperties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProducerProperties;
    }

    /** Mode of mapping split to Kafka components. */
    public enum SplitMappingMode {
        /** Use a single-partitioned topic as a split. */
        TOPIC,

        /** Use a partition in topic as a split. */
        PARTITION
    }
}
