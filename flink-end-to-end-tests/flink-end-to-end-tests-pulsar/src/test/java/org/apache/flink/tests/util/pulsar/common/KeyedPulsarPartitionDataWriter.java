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

package org.apache.flink.tests.util.pulsar.common;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.pulsar.client.api.Schema;

import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Source split data writer for writing test data into a Pulsar topic partition. It will write the
 * message with two keys.
 */
public class KeyedPulsarPartitionDataWriter implements SourceSplitDataWriter<String> {

    private final PulsarRuntimeOperator operator;
    private final String fullTopicName;
    private final String key1;
    private final String key2;

    public KeyedPulsarPartitionDataWriter(
            PulsarRuntimeOperator operator, String fullTopicName, String key1, String key2) {
        this.operator = operator;
        this.fullTopicName = fullTopicName;
        this.key1 = key1;
        this.key2 = key2;
    }

    @Override
    public void writeRecords(Collection<String> records) {
        operator.sendMessages(fullTopicName, Schema.STRING, key1, records);

        List<String> newRecords = records.stream().map(a -> a + key1).collect(toList());
        operator.sendMessages(fullTopicName, Schema.STRING, key2, newRecords);
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
