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

package org.apache.flink.connector.kafka.sink.testutils.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kafka.sink.testutils.KafkaSinkExternalContext;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.TableSinkExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;
import org.apache.flink.formats.csv.CsvFormatFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Kafka table sink external context. */
public class KafkaTableSinkExternalContext extends KafkaSinkExternalContext
        implements TableSinkExternalContext {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTableSinkExternalContext.class);

    public KafkaTableSinkExternalContext(String bootstrapServers, List<URL> connectorJarPaths) {
        super(bootstrapServers, connectorJarPaths);
    }

    @Override
    public Map<String, String> getSinkTableOptions(TestingSinkSettings sinkSettings) {
        if (numSplits == 0) {
            createTopic(topicName, 1, (short) 1);
            numSplits++;
        } else {
            LOG.debug("Creating new partition for topic {}", topicName);
            kafkaAdminClient.createPartitions(
                    Collections.singletonMap(topicName, NewPartitions.increaseTo(++numSplits)));
        }

        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CONNECTOR.key(), IDENTIFIER);
        tableOptions.put(KafkaConnectorOptions.TOPIC.key(), topicName);
        tableOptions.put(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS.key(), bootstrapServers);
        tableOptions.put(KafkaConnectorOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");
        tableOptions.put(KafkaConnectorOptions.PROPS_GROUP_ID.key(), "flink-kafka-test");
        tableOptions.put(FactoryUtil.FORMAT.key(), new CsvFormatFactory().factoryIdentifier());
        return tableOptions;
    }

    @Override
    public ExternalSystemDataReader<RowData> createSinkRowDataReader(
            TestingSinkSettings sinkSettings, DataType physicalDataType) {
        LOG.info("Fetching descriptions for topic: {}", topicName);
        final Map<String, TopicDescription> topicMetadata =
                getTopicMetadata(Arrays.asList(topicName));

        Set<TopicPartition> subscribedPartitions = new HashSet<>();
        for (TopicDescription topic : topicMetadata.values()) {
            for (TopicPartitionInfo partition : topic.partitions()) {
                subscribedPartitions.add(new TopicPartition(topic.name(), partition.partition()));
            }
        }

        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                "flink-kafka-test" + subscribedPartitions.hashCode());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getCanonicalName());
        if (CheckpointingMode.EXACTLY_ONCE.equals(sinkSettings.getCheckpointingMode())) {
            // default is read_uncommitted
            properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaTableDataReader(
                properties,
                subscribedPartitions,
                physicalDataType,
                getTableDataTypeInformation(physicalDataType));
    }

    private TypeInformation<RowData> getTableDataTypeInformation(DataType physicalDataType) {
        return new GenericTypeInfo<>(RowData.class);
    }
}
