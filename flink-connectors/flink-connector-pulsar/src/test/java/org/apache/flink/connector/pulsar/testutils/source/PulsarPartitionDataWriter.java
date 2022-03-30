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

/**
 * Source split data writer for writing test data into a Pulsar topic partition. This writer doesn't
 * need to be closed.
 */
public class PulsarPartitionDataWriter<T> implements ExternalSystemSplitDataWriter<T> {

    private final PulsarRuntimeOperator operator;
    private final String fullTopicName;
    private final Schema<T> schema;

    public PulsarPartitionDataWriter(
            PulsarRuntimeOperator operator, String fullTopicName, Schema<T> schema) {
        this.operator = operator;
        this.fullTopicName = fullTopicName;
        this.schema = schema;
    }

    @Override
    public void writeRecords(List<T> records) {
        operator.sendMessages(fullTopicName, schema, records);
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
