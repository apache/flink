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
import org.apache.flink.connector.pulsar.testutils.PulsarPartitionDataWriter;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_SERVICE_URL;
import static org.apache.pulsar.client.api.Schema.STRING;

/** We would consuming from test splits by using {@link SubscriptionType#Shared} subscription. */
public class SharedSubscriptionContext extends PulsarTestContext<String> {
    private static final long serialVersionUID = -2798707923661295245L;

    private int index = 0;

    private final List<PulsarPartitionDataWriter> writers = new ArrayList<>();

    public SharedSubscriptionContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected String displayName() {
        return "consuming message by Shared";
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        PulsarSourceBuilder<String> builder =
                PulsarSource.builder()
                        .setDeserializationSchema(pulsarSchema(STRING))
                        .setServiceUrl(PULSAR_SERVICE_URL)
                        .setAdminUrl(PULSAR_ADMIN_URL)
                        .setTopicPattern("pulsar-[0-9]+-shared", RegexSubscriptionMode.AllTopics)
                        .setSubscriptionType(SubscriptionType.Shared)
                        .setSubscriptionName("pulsar-shared");
        if (boundedness == Boundedness.BOUNDED) {
            // Using latest stop cursor for making sure the source could be stopped.
            builder.setBoundedStopCursor(StopCursor.latest());
        }

        return builder.build();
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter() {
        String topicName = "pulsar-" + index + "-shared";
        operator.createTopic(topicName, 1);
        index++;

        String partitionName = TopicNameUtils.topicNameWithPartition(topicName, 0);
        PulsarPartitionDataWriter writer = new PulsarPartitionDataWriter(operator, partitionName);
        writers.add(writer);

        return writer;
    }

    @Override
    public List<String> generateTestData(int splitIndex, long seed) {
        return generateStringTestData(splitIndex, seed);
    }

    @Override
    public void close() {
        for (PulsarPartitionDataWriter writer : writers) {
            writer.close();
        }
        writers.clear();
    }
}
