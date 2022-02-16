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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.formats.csv.CsvFormatFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Properties;

/** Kafka table data writer. */
public class KafkaTableDataWriter implements ExternalSystemSplitDataWriter<RowData> {

    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final TopicPartition topicPartition;
    private final DataType physicalDataType;
    private final SerializationSchema<RowData> serializer;

    public KafkaTableDataWriter(
            Properties producerProperties,
            TopicPartition topicPartition,
            DataType physicalDataType) {
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topicPartition = topicPartition;
        this.physicalDataType = physicalDataType;
        this.serializer =
                new CsvFormatFactory()
                        .createEncodingFormat(null, new Configuration())
                        .createRuntimeEncoder(null, physicalDataType);
    }

    @Override
    public void writeRecords(List<RowData> records) {
        for (RowData record : records) {
            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            null,
                            serializer.serialize(record));
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.flush();
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
