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

package org.apache.flink.connector.pulsar.testutils.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.AllTopics;

/**
 * Common source test context for pulsar based test. We use the string text as the basic send
 * content.
 */
public abstract class PulsarSourceTestContext extends PulsarTestContext<String>
        implements DataStreamSourceExternalContext<String> {

    private static final long DISCOVERY_INTERVAL = 1000L;
    private static final int BATCH_DATA_SIZE = 300;

    protected PulsarSourceTestContext(PulsarTestEnvironment environment) {
        super(environment, Schema.STRING);
    }

    @Override
    public Source<String, ?, ?> createSource(TestingSourceSettings sourceSettings) {
        PulsarSourceBuilder<String> builder =
                PulsarSource.builder()
                        .setDeserializationSchema(pulsarSchema(schema))
                        .setServiceUrl(operator.serviceUrl())
                        .setAdminUrl(operator.adminUrl())
                        .setTopicPattern(topicPattern(), AllTopics)
                        .setSubscriptionType(subscriptionType())
                        .setSubscriptionName(subscriptionName())
                        .setConfig(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, DISCOVERY_INTERVAL);

        // Set extra configuration for source builder.
        setSourceBuilder(builder);

        if (sourceSettings.getBoundedness() == Boundedness.BOUNDED) {
            // Using the latest stop cursor for making sure the source could be stopped.
            // This is required for SourceTestSuiteBase.
            builder.setBoundedStopCursor(StopCursor.latest());
        }

        return builder.build();
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        String partitionName = generatePartitionName();
        return new PulsarPartitionDataWriter<>(operator, partitionName, schema);
    }

    @Override
    public List<String> generateTestData(
            TestingSourceSettings sourceSettings, int splitIndex, long seed) {
        Random random = new Random(seed);
        return IntStream.range(0, BATCH_DATA_SIZE)
                .boxed()
                .map(
                        index -> {
                            int length = random.nextInt(20) + 1;
                            return "split:"
                                    + splitIndex
                                    + "-index:"
                                    + index
                                    + "-content:"
                                    + randomAlphanumeric(length);
                        })
                .collect(toList());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }

    /** Override this method for creating builder. */
    protected void setSourceBuilder(PulsarSourceBuilder<String> builder) {
        // Nothing to do by default.
    }

    /**
     * The topic pattern which is used in Pulsar topic auto discovery. It was discovered every
     * {@link #DISCOVERY_INTERVAL} ms;
     */
    protected abstract String topicPattern();

    /** The subscription name used in Pulsar consumer. */
    protected abstract String subscriptionName();

    /** The subscription type used in Pulsar consumer. */
    protected abstract SubscriptionType subscriptionType();

    /**
     * Dynamic generate a partition related topic in Pulsar. This topic should be pre-created in
     * Pulsar. Everytime we call this method, we may get a new partition name.
     */
    protected abstract String generatePartitionName();
}
