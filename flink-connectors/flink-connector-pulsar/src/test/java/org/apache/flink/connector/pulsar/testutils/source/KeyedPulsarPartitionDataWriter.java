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

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;

import org.apache.pulsar.client.api.Schema;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Source split data writer for writing test data into a Pulsar topic partition. It will write the
 * message with two keys.
 */
public class KeyedPulsarPartitionDataWriter implements ExternalSystemSplitDataWriter<String> {

    private final PulsarRuntimeOperator operator;
    private final String fullTopicName;
    private final String keyToRead;
    private final String keyToExclude;

    public KeyedPulsarPartitionDataWriter(
            PulsarRuntimeOperator operator,
            String fullTopicName,
            String keyToRead,
            String keyToExclude) {
        this.operator = operator;
        this.fullTopicName = fullTopicName;
        this.keyToRead = keyToRead;
        this.keyToExclude = keyToExclude;
    }

    @Override
    public void writeRecords(List<String> records) {
        // Send messages with the key we don't need.
        List<String> newRecords = records.stream().map(a -> a + keyToRead).collect(toList());
        operator.sendMessages(fullTopicName, Schema.STRING, keyToExclude, newRecords);

        // Send messages with the given key.
        operator.sendMessages(fullTopicName, Schema.STRING, keyToRead, records);
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
