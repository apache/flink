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

package org.apache.flink.connector.pulsar.testutils.sink.cases;

import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader;
import org.apache.flink.connector.pulsar.testutils.sink.reader.PulsarEncryptDataReader;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;

import org.apache.pulsar.client.api.CryptoKeyReader;

import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_PRODUCER_CRYPTO_FAILURE_ACTION;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.DEFAULT_KEY;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.DEFAULT_PRIVKEY;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.DEFAULT_PUBKEY;
import static org.apache.pulsar.client.api.ProducerCryptoFailureAction.FAIL;
import static org.apache.pulsar.client.api.Schema.STRING;

/** The sink context for supporting producing messages which are encrypted. */
public class PulsarEncryptSinkContext extends PulsarSinkTestContext {

    private final CryptoKeyReader cryptoKeyReader =
            new PulsarTestKeyReader(DEFAULT_KEY, DEFAULT_PUBKEY, DEFAULT_PRIVKEY);

    public PulsarEncryptSinkContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected void setSinkBuilder(PulsarSinkBuilder<String> builder) {
        super.setSinkBuilder(builder);

        builder.setEncryptionKeys(DEFAULT_KEY);
        builder.setCryptoKeyReader(cryptoKeyReader);
        builder.setConfig(PULSAR_PRODUCER_CRYPTO_FAILURE_ACTION, FAIL);
    }

    @Override
    public ExternalSystemDataReader<String> createSinkDataReader(TestingSinkSettings sinkSettings) {
        PulsarEncryptDataReader<String> reader =
                new PulsarEncryptDataReader<>(operator, topicName, STRING, cryptoKeyReader);
        closer.register(reader);

        return reader;
    }

    @Override
    protected String displayName() {
        return "write messages into one topic by End-to-end encryption";
    }
}
