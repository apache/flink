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

package org.apache.flink.connector.pulsar.testutils.cases;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.testutils.PulsarPartitionDataWriter;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.apache.pulsar.client.api.SubscriptionType.Exclusive;

/**
 * A Pulsar external context that will create only one topic and use partitions in that topic as
 * source splits.
 */
public class SingleTopicConsumingContext extends PulsarTestContext<String> {
    private static final long serialVersionUID = 2754642285356345741L;

    private static final String TOPIC_NAME_PREFIX = "pulsar-single-topic";
    private final String topicName;
    private final Map<Integer, SourceSplitDataWriter<String>> partitionToSplitWriter =
            new HashMap<>();

    private int numSplits = 0;

    public SingleTopicConsumingContext(PulsarTestEnvironment environment) {
        super(environment);
        this.topicName =
                TOPIC_NAME_PREFIX + "-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    }

    @Override
    protected String displayName() {
        return "consuming message on single topic";
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        PulsarSourceBuilder<String> builder =
                PulsarSource.builder()
                        .setDeserializationSchema(pulsarSchema(STRING))
                        .setServiceUrl(operator.serviceUrl())
                        .setAdminUrl(operator.adminUrl())
                        .setTopics(topicName)
                        .setSubscriptionType(Exclusive)
                        .setSubscriptionName("pulsar-single-topic");
        if (boundedness == Boundedness.BOUNDED) {
            // Using latest stop cursor for making sure the source could be stopped.
            // This is required for SourceTestSuiteBase.
            builder.setBoundedStopCursor(StopCursor.latest());
        }

        return builder.build();
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter() {
        if (numSplits == 0) {
            // Create the topic first.
            operator.createTopic(topicName, 1);
            numSplits++;
        } else {
            numSplits++;
            operator.increaseTopicPartitions(topicName, numSplits);
        }

        String partitionName = TopicNameUtils.topicNameWithPartition(topicName, numSplits - 1);
        PulsarPartitionDataWriter writer = new PulsarPartitionDataWriter(operator, partitionName);
        partitionToSplitWriter.put(numSplits - 1, writer);

        return writer;
    }

    @Override
    public List<String> generateTestData(int splitIndex, long seed) {
        return generateStringTestData(splitIndex, seed);
    }

    @Override
    public void close() throws Exception {
        // Close writer.
        for (SourceSplitDataWriter<String> writer : partitionToSplitWriter.values()) {
            writer.close();
        }

        partitionToSplitWriter.clear();
    }
}
