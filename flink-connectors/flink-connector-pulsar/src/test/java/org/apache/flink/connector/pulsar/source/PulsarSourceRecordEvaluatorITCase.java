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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expre
 * ss or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.testutils.PulsarSourceTestRecordEvaluator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test class for {@link PulsarSource} with {@link
 * org.apache.flink.connector.base.source.reader.RecordEvaluator}.
 */
public class PulsarSourceRecordEvaluatorITCase {

    @Test
    void testRecordEvaluator() throws Exception {
        String topicName = "recordEvaluatorTopic";
        int maxRecords = 10;
        PulsarTestEnvironment pulsar = new PulsarTestEnvironment(PulsarRuntime.mock());
        pulsar.startUp();
        pulsar.operator().createTopic(topicName, 1);
        PulsarSourceBuilder<Integer> builder =
                new PulsarSourceBuilder<Integer>()
                        .setTopics(topicName)
                        .setAdminUrl(pulsar.operator().adminUrl())
                        .setServiceUrl(pulsar.operator().serviceUrl())
                        .setSubscriptionName("subscription-name")
                        .setDeserializationSchema(pulsarSchema(Schema.INT32))
                        .setEofRecordEvaluator(new PulsarSourceTestRecordEvaluator<>(maxRecords));
        PulsarSource<Integer> source = builder.build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CloseableIterator<Integer> result =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "testRecordEvaluatorSource")
                        .executeAndCollect();

        int sumToCheck = 0;
        for (int i = 0; i < maxRecords * 2; i++) {
            pulsar.operator()
                    .sendMessage(topicNameWithPartition(topicName, 0), Schema.INT32, i * 10);
            if (i < maxRecords) {
                sumToCheck += i * 10;
            }
        }
        AtomicInteger actualCount = new AtomicInteger();
        AtomicInteger actualSum = new AtomicInteger();
        result.forEachRemaining(
                value -> {
                    actualCount.getAndIncrement();
                    actualSum.addAndGet(value);
                });

        assertThat(actualCount.get()).isEqualTo(maxRecords);
        assertThat(actualSum.get()).isEqualTo(sumToCheck);
    }
}
