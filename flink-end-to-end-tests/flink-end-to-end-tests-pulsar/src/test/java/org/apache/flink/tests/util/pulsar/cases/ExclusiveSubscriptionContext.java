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

package org.apache.flink.tests.util.pulsar.cases;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.testutils.PulsarPartitionDataWriter;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_SERVICE_URL;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.apache.pulsar.client.api.SubscriptionType.Exclusive;

/**
 * Pulsar context that will create multi topics as source splits. We would consume these splits by
 * using {@link SubscriptionType#Exclusive} subscription.
 */
public class ExclusiveSubscriptionContext extends PulsarTestContext<String> {
    private static final long serialVersionUID = -3855336888090886528L;

    private static final String SUBSCRIPTION_NAME = "flink-pulsar-multiple-topic-test";

    protected SubscriptionType subscriptionType = Exclusive;

    private int numTopics = 0;
    private final String topicPattern;
    private final Map<String, SourceSplitDataWriter<String>> topicNameToSplitWriters =
            new HashMap<>();

    public ExclusiveSubscriptionContext(PulsarTestEnvironment environment) {
        super(environment);
        this.topicPattern =
                "pulsar-multiple-topic-[0-9]+-"
                        + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    }

    @Override
    protected String displayName() {
        return "consuming message on multiple topic with subscriptionType " + subscriptionType;
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        PulsarSourceBuilder<String> builder =
                PulsarSource.builder()
                        .setDeserializationSchema(pulsarSchema(STRING))
                        .setServiceUrl(PULSAR_SERVICE_URL)
                        .setAdminUrl(PULSAR_ADMIN_URL)
                        .setTopicPattern(topicPattern, RegexSubscriptionMode.AllTopics)
                        .setSubscriptionType(subscriptionType)
                        .setSubscriptionName(SUBSCRIPTION_NAME);
        if (boundedness == Boundedness.BOUNDED) {
            // Using the latest stop cursor for making sure the source could be stopped.
            // This is required for SourceTestSuiteBase.
            builder.setBoundedStopCursor(StopCursor.latest());
        }

        return builder.build();
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter() {
        String topicName = topicPattern.replace("[0-9]+", String.valueOf(numTopics));
        operator.createTopic(topicName, 1);

        String partitionName = TopicNameUtils.topicNameWithPartition(topicName, 0);
        TopicPartition partition = new TopicPartition(partitionName, 0, createFullRange());
        PulsarPartitionDataWriter writer =
                new PulsarPartitionDataWriter(operator.client(), partition);

        topicNameToSplitWriters.put(partitionName, writer);
        numTopics++;

        return writer;
    }

    @Override
    public Collection<String> generateTestData(int splitIndex, long seed) {
        return generateStringTestData(splitIndex, seed);
    }

    @Override
    public void close() throws Exception {
        for (SourceSplitDataWriter<String> writer : topicNameToSplitWriters.values()) {
            writer.close();
        }

        topicNameToSplitWriters.clear();
    }
}
