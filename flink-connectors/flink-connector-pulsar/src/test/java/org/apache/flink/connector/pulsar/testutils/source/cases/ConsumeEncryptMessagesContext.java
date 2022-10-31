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

package org.apache.flink.connector.pulsar.testutils.source.cases;

import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader;
import org.apache.flink.connector.pulsar.testutils.source.writer.PulsarEncryptDataWriter;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.SubscriptionType;

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CRYPTO_FAILURE_ACTION;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.DEFAULT_KEY;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.DEFAULT_PRIVKEY;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.DEFAULT_PUBKEY;
import static org.apache.pulsar.client.api.ConsumerCryptoFailureAction.FAIL;

/** We will use this context for producing messages with encryption support. */
public class ConsumeEncryptMessagesContext extends MultipleTopicConsumingContext {

    private final CryptoKeyReader cryptoKeyReader =
            new PulsarTestKeyReader(DEFAULT_KEY, DEFAULT_PUBKEY, DEFAULT_PRIVKEY);

    public ConsumeEncryptMessagesContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected void setSourceBuilder(PulsarSourceBuilder<String> builder) {
        super.setSourceBuilder(builder);

        // Set CryptoKeyReader for the Pulsar source.
        builder.setCryptoKeyReader(cryptoKeyReader);
        builder.setConfig(PULSAR_CRYPTO_FAILURE_ACTION, FAIL);
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        String partitionName = generatePartitionName();
        return new PulsarEncryptDataWriter<>(
                operator, partitionName, schema, DEFAULT_KEY, cryptoKeyReader);
    }

    @Override
    protected String displayName() {
        return "consume messages by End-to-end encryption";
    }

    @Override
    protected String subscriptionName() {
        return "pulsar-encryption-subscription";
    }

    @Override
    protected SubscriptionType subscriptionType() {
        return SubscriptionType.Exclusive;
    }
}
