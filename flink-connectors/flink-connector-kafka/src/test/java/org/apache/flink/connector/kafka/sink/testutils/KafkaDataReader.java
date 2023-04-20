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

package org.apache.flink.connector.kafka.sink.testutils;

import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/** Kafka dataStream data reader. */
public class KafkaDataReader implements ExternalSystemDataReader<String> {
    private final KafkaConsumer<String, String> consumer;

    public KafkaDataReader(Properties properties, Collection<TopicPartition> partitions) {
        this.consumer = new KafkaConsumer<>(properties);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    @Override
    public List<String> poll(Duration timeout) {
        List<String> result = new LinkedList<>();
        ConsumerRecords<String, String> consumerRecords;
        try {
            consumerRecords = consumer.poll(timeout);
        } catch (WakeupException we) {
            return Collections.emptyList();
        }
        Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            result.add(iterator.next().value());
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
    }
}
