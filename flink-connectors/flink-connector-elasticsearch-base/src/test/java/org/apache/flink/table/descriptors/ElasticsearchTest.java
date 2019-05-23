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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link Elasticsearch} descriptor.
 */
public class ElasticsearchTest extends DescriptorTestBase {

	@Test(expected = ValidationException.class)
	public void testMissingIndex() {
		removePropertyAndVerify(descriptors().get(0), "connector.index");
	}

	@Test(expected = ValidationException.class)
	public void testInvalidFailureHandler() {
		addPropertyAndVerify(descriptors().get(0), "connector.failure-handler", "invalid handler");
	}

	@Test(expected = ValidationException.class)
	public void testInvalidMemorySize() {
		addPropertyAndVerify(descriptors().get(1), "connector.bulk-flush.max-size", "12 bytes");
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
		minimumDesc.put("connector.hosts.0.hostname", "localhost");
		minimumDesc.put("connector.hosts.0.port", "1234");
		minimumDesc.put("connector.hosts.0.protocol", "http");
		minimumDesc.put("connector.index", "MyIndex");
		minimumDesc.put("connector.document-type", "MyType");

		final Map<String, String> maximumDesc = new HashMap<>();
		maximumDesc.put("connector.property-version", "1");
		maximumDesc.put("connector.type", "elasticsearch");
		maximumDesc.put("connector.version", "6");
		maximumDesc.put("connector.hosts.0.hostname", "host1");
		maximumDesc.put("connector.hosts.0.port", "1234");
		maximumDesc.put("connector.hosts.0.protocol", "https");
		maximumDesc.put("connector.hosts.1.hostname", "host2");
		maximumDesc.put("connector.hosts.1.port", "1234");
		maximumDesc.put("connector.hosts.1.protocol", "https");
		maximumDesc.put("connector.index", "MyIndex");
		maximumDesc.put("connector.document-type", "MyType");
		maximumDesc.put("connector.key-delimiter", "#");
		maximumDesc.put("connector.key-null-literal", "");
		maximumDesc.put("connector.bulk-flush.backoff.type", "exponential");
		maximumDesc.put("connector.bulk-flush.backoff.delay", "123");
		maximumDesc.put("connector.bulk-flush.backoff.max-retries", "3");
		maximumDesc.put("connector.bulk-flush.interval", "100");
		maximumDesc.put("connector.bulk-flush.max-actions", "1000");
		maximumDesc.put("connector.bulk-flush.max-size", "12582912 bytes");
		maximumDesc.put("connector.failure-handler", "retry-rejected");
		maximumDesc.put("connector.connection-max-retry-timeout", "100");
		maximumDesc.put("connector.connection-path-prefix", "/myapp");

		final Map<String, String> customDesc = new HashMap<>();
		customDesc.put("connector.property-version", "1");
		customDesc.put("connector.type", "elasticsearch");
		customDesc.put("connector.version", "6");
		customDesc.put("connector.hosts.0.hostname", "localhost");
		customDesc.put("connector.hosts.0.port", "1234");
		customDesc.put("connector.hosts.0.protocol", "http");
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
