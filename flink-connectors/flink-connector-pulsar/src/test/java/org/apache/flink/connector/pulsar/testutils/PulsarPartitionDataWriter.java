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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.Collection;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;

/** Source split data writer for writing test data into a Pulsar topic partition. */
public class PulsarPartitionDataWriter implements SourceSplitDataWriter<String> {

    private final Producer<String> producer;

    public PulsarPartitionDataWriter(PulsarClient client, TopicPartition partition) {
        try {
            this.producer =
                    client.newProducer(Schema.STRING).topic(partition.getFullTopicName()).create();
        } catch (PulsarClientException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void writeRecords(Collection<String> records) {
        for (String record : records) {
            sneakyClient(() -> producer.newMessage().value(record).send());
        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
