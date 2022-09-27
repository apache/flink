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

package org.apache.flink.tests.util.pulsar.source;

import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FixedRangeGenerator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.source.cases.MultipleTopicConsumingContext;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.tests.util.pulsar.common.KeyedPulsarPartitionDataWriter;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.Murmur3_32Hash;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.RANGE_SIZE;
import static org.apache.pulsar.client.api.SubscriptionType.Key_Shared;

/** We would consume from test splits by using {@link SubscriptionType#Key_Shared} subscription. */
public class KeySharedSubscriptionContext extends MultipleTopicConsumingContext {

    private final String keyToRead;
    private final String keyToExclude;

    public KeySharedSubscriptionContext(PulsarTestEnvironment environment) {
        super(environment, Key_Shared);

        this.keyToRead = randomAlphabetic(8);

        // Make sure they have different hash code.
        int readHash = keyHash(keyToRead);
        String randomKey;
        do {
            randomKey = randomAlphabetic(8);
        } while (keyHash(randomKey) == readHash);
        this.keyToExclude = randomKey;
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        String partitionName = generatePartitionName();
        return new KeyedPulsarPartitionDataWriter(operator, partitionName, keyToRead, keyToExclude);
    }

    @Override
    protected String displayName() {
        return "consume message by Key_Shared";
    }

    @Override
    protected void setSourceBuilder(PulsarSourceBuilder<String> builder) {
        int keyHash = keyHash(keyToRead);
        TopicRange range = new TopicRange(keyHash, keyHash);

        builder.setRangeGenerator(new FixedRangeGenerator(singletonList(range)));
    }

    @Override
    protected String subscriptionName() {
        return "pulsar-key-shared-subscription";
    }

    // This method is copied from Pulsar for calculating message key hash.
    private int keyHash(String key) {
        return Murmur3_32Hash.getInstance().makeHash(key.getBytes()) % RANGE_SIZE;
    }
}
