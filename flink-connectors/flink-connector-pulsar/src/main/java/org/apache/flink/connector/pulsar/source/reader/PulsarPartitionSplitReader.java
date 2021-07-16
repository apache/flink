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

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.source.config.ConfigurationDataCustomizer;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;

import java.io.Closeable;
import java.io.IOException;

public class PulsarPartitionSplitReader<T>
        implements SplitReader<PulsarMessage<T>, PulsarPartitionSplit>, Closeable {

    public <IN> PulsarPartitionSplitReader(
            Configuration configuration,
            ConfigurationDataCustomizer<ClientConfigurationData> clientConfigurationCustomizer,
            ConfigurationDataCustomizer<ConsumerConfigurationData<IN>>
                    consumerConfigurationCustomizer,
            PulsarDeserializationSchema<IN, T> deserializationSchema,
            ExecutorProvider listenerExecutor) {}

    @Override
    public RecordsWithSplitIds<PulsarMessage<T>> fetch() throws IOException {
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PulsarPartitionSplit> splitsChanges) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() {}
}
