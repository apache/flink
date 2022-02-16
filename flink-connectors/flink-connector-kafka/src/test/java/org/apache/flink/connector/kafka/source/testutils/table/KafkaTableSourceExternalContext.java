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

package org.apache.flink.connector.kafka.source.testutils.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContext;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.TableSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.formats.csv.CsvFormatFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode.EARLIEST_OFFSET;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC_PATTERN;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory.IDENTIFIER;

/** Kafka table source external context. */
public class KafkaTableSourceExternalContext extends KafkaSourceExternalContext
        implements TableSourceExternalContext {
    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaTableSourceExternalContext.class);
    private final List<KafkaTableDataWriter> writers = new ArrayList<>();

    public KafkaTableSourceExternalContext(
            String bootstrapServers,
            SplitMappingMode splitMappingMode,
            List<URL> connectorJarPaths) {
        super(bootstrapServers, splitMappingMode, connectorJarPaths);
    }

    @Override
    public Map<String, String> getSourceTableOptions(TestingSourceSettings sourceSettings) {
        Configuration conf = new Configuration();
        conf.set(FactoryUtil.CONNECTOR, IDENTIFIER);
        conf.set(PROPS_GROUP_ID, randomize(GROUP_ID_PREFIX));
        conf.set(PROPS_BOOTSTRAP_SERVERS, bootstrapServers);
        conf.set(FactoryUtil.FORMAT, new CsvFormatFactory().factoryIdentifier());
        conf.set(TOPIC_PATTERN, TOPIC_NAME_PREFIX + ".*");
        conf.set(SCAN_STARTUP_MODE, EARLIEST_OFFSET);
        return conf.toMap();
    }

    @Override
    public ExternalSystemSplitDataWriter<RowData> createSplitRowDataWriter(
            TestingSourceSettings sourceOptions, DataType physicalDataType) {
        KafkaTableDataWriter writer;
        try {
            switch (splitMappingMode) {
                case TOPIC:
                    writer = createSinglePartitionTopic(writers.size(), physicalDataType);
                    break;
                case PARTITION:
                    writer = scaleOutTopic(this.topicName, physicalDataType);
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
    public void close() throws Exception {
        final List<String> topics = new ArrayList<>();
        writers.forEach(
                writer -> {
                    topics.add(writer.getTopicPartition().topic());
                    writer.close();
                });
        adminClient.deleteTopics(topics).all().get();
    }

    private KafkaTableDataWriter createSinglePartitionTopic(
            int topicIndex, DataType physicalDataType) throws Exception {
        String newTopicName = topicName + "-" + topicIndex;
        LOG.info("Creating topic '{}'", newTopicName);
        adminClient
                .createTopics(Collections.singletonList(new NewTopic(newTopicName, 1, (short) 1)))
                .all()
                .get();
        return new KafkaTableDataWriter(
                getKafkaProducerProperties(topicIndex),
                new TopicPartition(newTopicName, 0),
                physicalDataType);
    }

    private KafkaTableDataWriter scaleOutTopic(String topicName, DataType physicalDataType)
            throws Exception {
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
            return new KafkaTableDataWriter(
                    getKafkaProducerProperties(numPartitions),
                    new TopicPartition(topicName, numPartitions),
                    physicalDataType);
        } else {
            LOG.info("Creating topic '{}'", topicName);
            adminClient.createTopics(
                    Collections.singletonList(new NewTopic(topicName, 1, (short) 1)));
            return new KafkaTableDataWriter(
                    getKafkaProducerProperties(0),
                    new TopicPartition(topicName, 0),
                    physicalDataType);
        }
    }
}
