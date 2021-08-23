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

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.pulsar.client.api.Schema.STRING;

/**
 * Pulsar external context template that will create multiple topics with only one partitions as
 * source splits.
 */
public abstract class MultipleTopicTemplateContext extends PulsarTestContext<String> {
    private static final long serialVersionUID = 7333807392445848344L;

    private int numTopics = 0;

    private final String topicPattern = "pulsar-multiple-topic-[0-9]+-" + randomAlphabetic(8);

    private final Map<String, SourceSplitDataWriter<String>> topicNameToSplitWriters =
            new HashMap<>();

    public MultipleTopicTemplateContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        PulsarSourceBuilder<String> builder =
                PulsarSource.builder()
                        .setDeserializationSchema(pulsarSchema(STRING))
                        .setServiceUrl(serviceUrl())
                        .setAdminUrl(adminUrl())
                        .setTopicPattern(topicPattern, RegexSubscriptionMode.AllTopics)
                        .setSubscriptionType(subscriptionType())
                        .setSubscriptionName(subscriptionName());
        if (boundedness == Boundedness.BOUNDED) {
            // Using latest stop cursor for making sure the source could be stopped.
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
        PulsarPartitionDataWriter writer = new PulsarPartitionDataWriter(operator, partitionName);

        topicNameToSplitWriters.put(partitionName, writer);
        numTopics++;

        return writer;
    }

    @Override
    public List<String> generateTestData(int splitIndex, long seed) {
        return generateStringTestData(splitIndex, seed);
    }

    @Override
    public void close() throws Exception {
        for (SourceSplitDataWriter<String> writer : topicNameToSplitWriters.values()) {
            writer.close();
        }

        topicNameToSplitWriters.clear();
    }

    protected abstract String subscriptionName();

    protected abstract SubscriptionType subscriptionType();

    protected String serviceUrl() {
        return operator.serviceUrl();
    }

    protected String adminUrl() {
        return operator.adminUrl();
    }
}
