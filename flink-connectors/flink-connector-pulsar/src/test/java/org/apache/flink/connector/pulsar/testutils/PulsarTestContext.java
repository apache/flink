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
import org.apache.flink.connector.testframe.external.ExternalContext;

import org.apache.pulsar.client.api.Schema;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * The implementation for Flink connector test tools. Providing the common test case writing
 * constraint for both source, sink and table API.
 */
public abstract class PulsarTestContext<T> implements ExternalContext {

    protected final PulsarRuntimeOperator operator;
    // The schema used for consuming and producing messages between Pulsar and tests.
    protected final Schema<T> schema;

    protected PulsarTestContext(PulsarTestEnvironment environment, Schema<T> schema) {
        this.operator = environment.operator();
        this.schema = schema;
    }

    /** Implement this method for providing a more friendly test name in IDE. */
    protected abstract String displayName();

    @Override
    public String toString() {
        return displayName();
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        // We don't need any test jars definition. They are provided in docker-related environments.
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // All the topics would be deleted in the PulsarRuntime. No need to manually close them.
    }
}
