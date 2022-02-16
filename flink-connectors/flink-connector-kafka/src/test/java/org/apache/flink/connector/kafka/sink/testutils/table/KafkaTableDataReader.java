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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.formats.csv.CsvRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** Kafka table data reader. */
public class KafkaTableDataReader implements ExternalSystemDataReader<RowData> {
    private final KafkaConsumer<String, byte[]> consumer;
    private final DataType physicalDataType;
    private final DeserializationSchema<RowData> deserializer;

    public KafkaTableDataReader(
            Properties properties,
            Set<TopicPartition> partitions,
            DataType physicalDataType,
            TypeInformation<RowData> typeInformation) {
        this.consumer = new KafkaConsumer<>(properties);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        this.physicalDataType = physicalDataType;

        final RowType rowType = (RowType) physicalDataType.getLogicalType();
        final CsvRowDataDeserializationSchema.Builder schemaBuilder =
                new CsvRowDataDeserializationSchema.Builder(rowType, typeInformation);
        this.deserializer = schemaBuilder.build();
    }

    @Override
    public List<RowData> poll(Duration timeout) {
        List<RowData> result = new LinkedList<>();
        ConsumerRecords<String, byte[]> consumerRecords;
        try {
            consumerRecords = consumer.poll(timeout);
        } catch (WakeupException we) {
            return Collections.emptyList();
        }
        Iterator<ConsumerRecord<String, byte[]>> iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            try {
                result.add(deserializer.deserialize(iterator.next().value()));
            } catch (IOException e) {
                throw new IllegalArgumentException("Error when deserializing from kafka.", e);
            }
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }
}
