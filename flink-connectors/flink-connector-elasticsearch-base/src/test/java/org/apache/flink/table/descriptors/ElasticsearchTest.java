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

package org.apache.flink.table.descriptors;

import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_HOSTS;

/** Tests for the {@link Elasticsearch} descriptor. */
public class ElasticsearchTest extends DescriptorTestBase {

    @Test
    public void testMissingIndex() {
        assertThrows(ValidationException.class, () -> {
                    removePropertyAndVerify(descriptors().get(0), "connector.index");
        });
    }

    @Test
    public void testInvalidFailureHandler() {
        assertThrows(ValidationException.class, () -> {
                    addPropertyAndVerify(descriptors().get(0), "connector.failure-handler", "invalid handler");
        });
    }

    @Test
    public void testInvalidMemorySize() {
        assertThrows(ValidationException.class, () -> {
                    addPropertyAndVerify(descriptors().get(1), "connector.bulk-flush.max-size", "12 bytes");
        });
    }

    @Test
    public void testInvalidProtocolInHosts() {
        assertThrows(ValidationException.class, () -> {
                    final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putString(CONNECTOR_HOSTS, "localhost:90");
        ElasticsearchValidator.validateAndParseHostsString(descriptorProperties);
        });
    }

    @Test
    public void testInvalidHostNameInHosts() {
        assertThrows(ValidationException.class, () -> {
                    final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putString(CONNECTOR_HOSTS, "http://:90");
        ElasticsearchValidator.validateAndParseHostsString(descriptorProperties);
        });
    }

    @Test
    public void testInvalidHostPortInHosts() {
        assertThrows(ValidationException.class, () -> {
                    final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putString(CONNECTOR_HOSTS, "http://localhost");
        ElasticsearchValidator.validateAndParseHostsString(descriptorProperties);
        });
    }

    @Override
    public List<Descriptor> descriptors() {
        final Descriptor minimumDesc =
                new Elasticsearch()
                        .version("6")
                        .host("localhost", 1234, "http")
                        .index("MyIndex")
                        .documentType("MyType");

        final Descriptor maximumDesc =
                new Elasticsearch()
                        .version("6")
                        .host("host1", 1234, "https")
                        .host("host2", 1234, "https")
                        .index("MyIndex")
                        .documentType("MyType")
                        .keyDelimiter("#")
                        .keyNullLiteral("")
                        .bulkFlushBackoffExponential()
                        .bulkFlushBackoffDelay(123L)
                        .bulkFlushBackoffMaxRetries(3)
                        .bulkFlushInterval(100L)
                        .bulkFlushMaxActions(1000)
                        .bulkFlushMaxSize("12 MB")
                        .failureHandlerRetryRejected()
                        .connectionMaxRetryTimeout(100)
                        .connectionPathPrefix("/myapp");

        final Descriptor customDesc =
                new Elasticsearch()
                        .version("6")
                        .host("localhost", 1234, "http")
                        .index("MyIndex")
                        .documentType("MyType")
                        .disableFlushOnCheckpoint()
                        .failureHandlerCustom(NoOpFailureHandler.class);

        return Arrays.asList(minimumDesc, maximumDesc, customDesc);
    }

    @Override
    public List<Map<String, String>> properties() {
        final Map<String, String> minimumDesc = new HashMap<>();
        minimumDesc.put("connector.property-version", "1");
        minimumDesc.put("connector.type", "elasticsearch");
        minimumDesc.put("connector.version", "6");
        minimumDesc.put("connector.hosts", "http://localhost:1234");
        minimumDesc.put("connector.index", "MyIndex");
        minimumDesc.put("connector.document-type", "MyType");

        final Map<String, String> maximumDesc = new HashMap<>();
        maximumDesc.put("connector.property-version", "1");
        maximumDesc.put("connector.type", "elasticsearch");
        maximumDesc.put("connector.version", "6");
        maximumDesc.put("connector.hosts", "https://host1:1234;https://host2:1234");
        maximumDesc.put("connector.index", "MyIndex");
        maximumDesc.put("connector.document-type", "MyType");
        maximumDesc.put("connector.key-delimiter", "#");
        maximumDesc.put("connector.key-null-literal", "");
        maximumDesc.put("connector.bulk-flush.backoff.type", "exponential");
        maximumDesc.put("connector.bulk-flush.backoff.delay", "123");
        maximumDesc.put("connector.bulk-flush.backoff.max-retries", "3");
        maximumDesc.put("connector.bulk-flush.interval", "100");
        maximumDesc.put("connector.bulk-flush.max-actions", "1000");
        maximumDesc.put("connector.bulk-flush.max-size", "12 mb");
        maximumDesc.put("connector.failure-handler", "retry-rejected");
        maximumDesc.put("connector.connection-max-retry-timeout", "100");
        maximumDesc.put("connector.connection-path-prefix", "/myapp");

        final Map<String, String> customDesc = new HashMap<>();
        customDesc.put("connector.property-version", "1");
        customDesc.put("connector.type", "elasticsearch");
        customDesc.put("connector.version", "6");
        customDesc.put("connector.hosts", "http://localhost:1234");
        customDesc.put("connector.index", "MyIndex");
        customDesc.put("connector.document-type", "MyType");
        customDesc.put("connector.flush-on-checkpoint", "false");
        customDesc.put("connector.failure-handler", "custom");
        customDesc.put("connector.failure-handler-class", NoOpFailureHandler.class.getName());

        return Arrays.asList(minimumDesc, maximumDesc, customDesc);
    }

    @Override
    public DescriptorValidator validator() {
        return new ElasticsearchValidator();
    }
}
