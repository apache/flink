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
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FixedRangeGenerator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;
import org.apache.flink.tests.util.pulsar.common.KeyedPulsarPartitionDataWriter;

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.Murmur3_32Hash;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.RANGE_SIZE;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_SERVICE_URL;
import static org.apache.pulsar.client.api.Schema.STRING;

/**
 * We would consuming from test splits by using {@link SubscriptionType#Key_Shared} subscription.
 */
public class KeySharedSubscriptionContext extends PulsarTestContext<String> {
    private static final long serialVersionUID = 3246516520107893983L;

    private int index = 0;

    private final List<KeyedPulsarPartitionDataWriter> writers = new ArrayList<>();

    // Message keys.
    private final String key1;
    private final String key2;

    public KeySharedSubscriptionContext(PulsarTestEnvironment environment) {
        super(environment);

        // Init message keys.
        this.key1 = randomAlphabetic(8);
        String newKey2;
        do {
            newKey2 = randomAlphabetic(8);
        } while (keyHash(key1) == keyHash(newKey2));
        this.key2 = newKey2;
    }

    @Override
    protected String displayName() {
        return "consuming message by Key_Shared";
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        int keyHash = keyHash(key1);
        TopicRange range = new TopicRange(keyHash, keyHash);

        PulsarSourceBuilder<String> builder =
                PulsarSource.builder()
                        .setDeserializationSchema(pulsarSchema(STRING))
                        .setServiceUrl(PULSAR_SERVICE_URL)
                        .setAdminUrl(PULSAR_ADMIN_URL)
                        .setTopicPattern(
                                "pulsar-[0-9]+-key-shared", RegexSubscriptionMode.AllTopics)
                        .setSubscriptionType(SubscriptionType.Key_Shared)
                        .setSubscriptionName("pulsar-key-shared")
                        .setRangeGenerator(new FixedRangeGenerator(singletonList(range)));
        if (boundedness == Boundedness.BOUNDED) {
            // Using latest stop cursor for making sure the source could be stopped.
            builder.setBoundedStopCursor(StopCursor.latest());
        }

        return builder.build();
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter() {
        String topicName = "pulsar-" + index + "-key-shared";
        operator.createTopic(topicName, 1);
        index++;

        String partitionName = TopicNameUtils.topicNameWithPartition(topicName, 0);
        KeyedPulsarPartitionDataWriter writer =
                new KeyedPulsarPartitionDataWriter(operator, partitionName, key1, key2);
        writers.add(writer);

        return writer;
    }

    @Override
    public List<String> generateTestData(int splitIndex, long seed) {
        return generateStringTestData(splitIndex, seed);
    }

    @Override
    public void close() {
        for (KeyedPulsarPartitionDataWriter writer : writers) {
            writer.close();
        }
        writers.clear();
    }

    private int keyHash(String key) {
        return Murmur3_32Hash.getInstance().makeHash(key.getBytes()) % RANGE_SIZE;
    }
}
