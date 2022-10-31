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

package org.apache.flink.connector.pulsar.testutils.source.writer;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.Schema;

import java.util.List;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.pulsar.client.api.ProducerAccessMode.Shared;

/** Encrypt the messages with the given public key and send the message to Pulsar. */
public class PulsarEncryptDataWriter<T> implements ExternalSystemSplitDataWriter<T> {

    private final Producer<T> producer;

    public PulsarEncryptDataWriter(
            PulsarRuntimeOperator operator,
            String fullTopicName,
            Schema<T> schema,
            String encryptKey,
            CryptoKeyReader cryptoKeyReader) {
        ProducerBuilder<T> builder =
                operator.client()
                        .newProducer(schema)
                        .topic(fullTopicName)
                        .enableBatching(false)
                        .enableMultiSchema(true)
                        .accessMode(Shared)
                        .addEncryptionKey(encryptKey)
                        .cryptoFailureAction(ProducerCryptoFailureAction.FAIL)
                        .cryptoKeyReader(cryptoKeyReader);
        this.producer = sneakyClient(builder::create);
    }

    @Override
    public void writeRecords(List<T> records) {
        for (T record : records) {
            sneakyClient(() -> producer.newMessage().value(record).send());
        }
        sneakyClient(producer::flush);
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
