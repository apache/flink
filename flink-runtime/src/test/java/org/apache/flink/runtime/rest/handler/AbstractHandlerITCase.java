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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.util.TestMessageHeaders;
import org.apache.flink.runtime.rest.util.TestRestHandler;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests to cover functionality provided by {@link AbstractHandler}.
 */
public class AbstractHandlerITCase extends TestLogger {

	private static final RestfulGateway mockRestfulGateway = new TestingDispatcherGateway.Builder().build();

	private static final GatewayRetriever<RestfulGateway> mockGatewayRetriever = () -> CompletableFuture.completedFuture(mockRestfulGateway);

	private static final Configuration REST_BASE_CONFIG;

	static {
		final String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();

		final Configuration config = new Configuration();
		config.setString(RestOptions.BIND_PORT, "0");
		config.setString(RestOptions.BIND_ADDRESS, loopbackAddress);
		config.setString(RestOptions.ADDRESS, loopbackAddress);

		REST_BASE_CONFIG = config;
	}

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private RestClient createRestClient(int serverPort) throws ConfigurationException {
		Configuration config = new Configuration(REST_BASE_CONFIG);
		config.setInteger(RestOptions.PORT, serverPort);

		return new RestClient(
				RestClientConfiguration.fromConfiguration(config),
				Executors.directExecutor());
	}

	@Test
	public void testOOMErrorMessageEnrichment() throws Exception {
		final TestMessageHeaders<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> messageHeaders =
				TestMessageHeaders.emptyBuilder()
						.setTargetRestEndpointURL("/test-handler")
						.build();

		final TestRestHandler<RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> testRestHandler = new TestRestHandler<>(
				mockGatewayRetriever,
				messageHeaders,
				FutureUtils.completedExceptionally(new OutOfMemoryError("Metaspace"))
		);

		try (final TestRestServerEndpoint server = TestRestServerEndpoint.builder(RestServerEndpointConfiguration.fromConfiguration(REST_BASE_CONFIG))
				.withHandler(messageHeaders, testRestHandler)
				.buildAndStart();
			final RestClient restClient = createRestClient(server.getServerAddress().getPort())
		) {
			CompletableFuture<EmptyResponseBody> response = restClient.sendRequest(
					server.getServerAddress().getHostName(),
					server.getServerAddress().getPort(),
					messageHeaders,
					EmptyMessageParameters.getInstance(),
					EmptyRequestBody.getInstance());
			try {
				response.get();
				fail("An ExecutionException was expected here being caused by the OutOfMemoryError.");
			} catch (ExecutionException e) {
				assertThat(e.getMessage(), StringContains.containsString("Metaspace. The metaspace out-of-memory error has occurred. "));
			}
		}
	}
}
