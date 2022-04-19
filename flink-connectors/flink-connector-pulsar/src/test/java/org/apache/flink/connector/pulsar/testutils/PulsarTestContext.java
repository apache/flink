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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/** Common test context for pulsar based test. */
public abstract class PulsarTestContext<T> implements DataStreamSourceExternalContext<T> {

    protected final PulsarRuntimeOperator operator;
    protected final List<URL> connectorJarPaths;

    protected PulsarTestContext(PulsarTestEnvironment environment, List<URL> connectorJarPaths) {
        this.operator = environment.operator();
        this.connectorJarPaths = connectorJarPaths;
    }

    // Helper methods for generating data.

    protected List<String> generateStringTestData(int splitIndex, long seed) {
        int recordNum = 300;
        List<String> records = new ArrayList<>(recordNum);
        for (int i = 0; i < recordNum; i++) {
            records.add(splitIndex + "-" + i);
        }

        return records;
    }

    protected abstract String displayName();

    @Override
    public String toString() {
        return displayName();
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return connectorJarPaths;
    }
}
