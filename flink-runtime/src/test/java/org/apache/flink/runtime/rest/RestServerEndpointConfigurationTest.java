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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.SSLHandshakeException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RestServerEndpointConfiguration}.
 */
public class RestServerEndpointConfigurationTest extends TestLogger {

	private static final String ADDRESS = "123.123.123.123";
	private static final String BIND_ADDRESS = "023.023.023.023";
	private static final String BIND_PORT = "7282";
	private static final int CONTENT_LENGTH = 1234;
	private static final Time timeout = Time.seconds(10L);

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testBasicMapping() throws ConfigurationException {
		Configuration originalConfig = new Configuration();
		originalConfig.setString(RestOptions.ADDRESS, ADDRESS);
		originalConfig.setString(RestOptions.BIND_ADDRESS, BIND_ADDRESS);
		originalConfig.setString(RestOptions.BIND_PORT, BIND_PORT);
		originalConfig.setInteger(RestOptions.SERVER_MAX_CONTENT_LENGTH, CONTENT_LENGTH);
		originalConfig.setString(WebOptions.TMP_DIR, temporaryFolder.getRoot().getAbsolutePath());

		final RestServerEndpointConfiguration result = RestServerEndpointConfiguration.fromConfiguration(originalConfig);
		Assert.assertEquals(ADDRESS, result.getRestAddress());
		Assert.assertEquals(BIND_ADDRESS, result.getRestBindAddress());
		Assert.assertEquals(BIND_PORT, result.getRestBindPort());
		Assert.assertEquals(CONTENT_LENGTH, result.getMaxContentLength());
		Assert.assertThat(
			result.getUploadDir().toAbsolutePath().toString(),
			containsString(temporaryFolder.getRoot().getAbsolutePath()));
	}

	@Test
	public void testDefaultRestServerBindPort() throws Exception {
		testRestServerBindPort(null, "8081");
	}

	@Test
	public void testRestServerSpecificBindPort() throws Exception {
		testRestServerBindPort("12345", "12345");
	}

	@Test
	public void testRestServerSpecificBindPortRange() throws Exception {
		testRestServerBindPort("12345,12346", "12345");
	}

	@Test
	public void testRestServerBindPortRange() throws Exception {
		testRestServerBindPort("12345-12350", "12345");
	}

	@Test
	public void testRestServerBindPortAndConnectionPortSuccess() throws Exception {
		testRestServerBindPortAndConnectionPort("12345,12346", 12345);
	}

	@Test
	public void testRestServerBindPortAndConnectionPortFailed() throws Exception {
		try {
			testRestServerBindPortAndConnectionPort("12345-12346", 12347);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Connection refused")
				&& e.getMessage().contains("12347"));
		}
	}

	@Test
	public void testRestServerBindPortConflict() throws Exception {
		RestServerEndpoint serverEndpoint1 = null;
		RestServerEndpoint serverEndpoint2 = null;
		RestClient restClient = null;

		try {
			final Configuration config = new Configuration();
			config.setString(RestOptions.ADDRESS, "localhost");
			config.setString(RestOptions.BIND_PORT, "12345,12346");

			RestServerEndpointITCase.TestHandler testHandler;

			RestServerEndpointConfiguration serverConfig = RestServerEndpointConfiguration.fromConfiguration(config);
			RestClientConfiguration clientConfig = RestClientConfiguration.fromConfiguration(config);

			final String restAddress = "http://localhost:1234";
			RestfulGateway mockRestfulGateway = mock(RestfulGateway.class);
			when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(restAddress));

			final GatewayRetriever<RestfulGateway> mockGatewayRetriever = () ->
				CompletableFuture.completedFuture(mockRestfulGateway);

			testHandler = new RestServerEndpointITCase.TestHandler(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT);

			RestServerEndpointITCase.TestVersionHandler testVersionHandler = new RestServerEndpointITCase.TestVersionHandler(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT);

			final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = Arrays.asList(
				Tuple2.of(new RestServerEndpointITCase.TestHeaders(), testHandler),
				Tuple2.of(testVersionHandler.getMessageHeaders(), testVersionHandler));

			serverEndpoint1 = new RestServerEndpointITCase.TestRestServerEndpoint(serverConfig, handlers);
			serverEndpoint2 = new RestServerEndpointITCase.TestRestServerEndpoint(serverConfig, handlers);
			restClient = new RestServerEndpointITCase.TestRestClient(clientConfig);

			//start two rest server instances, the first one pick port : 12345
			//the second one pick port : 12345 firstly, but will conflict with the first instance,
			//then the expected behavior is that it should choose port : 12346
			serverEndpoint1.start();
			serverEndpoint2.start();

			CompletableFuture<EmptyResponseBody> response = restClient.sendRequest(
				config.getString(RestOptions.ADDRESS),
				12346,		//connect to the second port
				RestServerEndpointITCase.TestVersionHeaders.INSTANCE,
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance(),
				Collections.emptyList()
			);

			response.get(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			throw e;
		} finally {
			if (restClient != null) {
				restClient.shutdown(timeout);
			}

			if (serverEndpoint1 != null) {
				serverEndpoint1.closeAsync().get(timeout.getSize(), timeout.getUnit());
			}

			if (serverEndpoint2 != null) {
				serverEndpoint2.closeAsync().get(timeout.getSize(), timeout.getUnit());
			}
		}
	}

	private void testRestServerBindPortAndConnectionPort(String bindPort, int connectPort) throws Exception {
		RestServerEndpoint serverEndpoint = null;
		RestClient restClient = null;

		try {
			final Configuration config = new Configuration();
			config.setString(RestOptions.ADDRESS, "localhost");
			config.setString(RestOptions.BIND_PORT, bindPort);

			RestServerEndpointITCase.TestHandler testHandler;

			RestServerEndpointConfiguration serverConfig = RestServerEndpointConfiguration.fromConfiguration(config);
			RestClientConfiguration clientConfig = RestClientConfiguration.fromConfiguration(config);

			final String restAddress = "http://localhost:1234";
			RestfulGateway mockRestfulGateway = mock(RestfulGateway.class);
			when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(restAddress));

			final GatewayRetriever<RestfulGateway> mockGatewayRetriever = () ->
				CompletableFuture.completedFuture(mockRestfulGateway);

			testHandler = new RestServerEndpointITCase.TestHandler(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT);

			RestServerEndpointITCase.TestVersionHandler testVersionHandler = new RestServerEndpointITCase.TestVersionHandler(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT);

			final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = Arrays.asList(
				Tuple2.of(new RestServerEndpointITCase.TestHeaders(), testHandler),
				Tuple2.of(testVersionHandler.getMessageHeaders(), testVersionHandler));

			serverEndpoint = new RestServerEndpointITCase.TestRestServerEndpoint(serverConfig, handlers);
			restClient = new RestServerEndpointITCase.TestRestClient(clientConfig);

			serverEndpoint.start();

			CompletableFuture<EmptyResponseBody> response = restClient.sendRequest(
				config.getString(RestOptions.ADDRESS),
				connectPort,
				RestServerEndpointITCase.TestVersionHeaders.INSTANCE,
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance(),
				Collections.emptyList()
			);

			response.get(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			throw e;
		} finally {
			if (restClient != null) {
				restClient.shutdown(timeout);
			}

			if (serverEndpoint != null) {
				serverEndpoint.closeAsync().get(timeout.getSize(), timeout.getUnit());
			}
		}
	}

	private void testRestServerBindPort(String restBindPort, String expectedBindPort) throws Exception {
		RestServerEndpoint serverEndpoint = null;

		try {
			final Configuration baseConfig = new Configuration();
			baseConfig.setString(RestOptions.ADDRESS, "localhost");

			if (restBindPort != null) {
				baseConfig.setString(RestOptions.BIND_PORT, restBindPort);
			}

			Configuration serverConfig = new Configuration(baseConfig);

			RestServerEndpointConfiguration restServerConfig = RestServerEndpointConfiguration.fromConfiguration(serverConfig);

			RestfulGateway restfulGateway = TestingRestfulGateway.newBuilder().build();
			RestServerEndpointITCase.TestVersionHandler testVersionHandler = new RestServerEndpointITCase.TestVersionHandler(
				CompletableFuture.completedFuture("http://localhost:1234"),
				() -> CompletableFuture.completedFuture(restfulGateway),
				RpcUtils.INF_TIMEOUT);

			serverEndpoint = new RestServerEndpointITCase.TestRestServerEndpoint(
				restServerConfig,
				Arrays.asList(Tuple2.of(testVersionHandler.getMessageHeaders(), testVersionHandler)));
			serverEndpoint.start();

			assertEquals("http://localhost:" + expectedBindPort, serverEndpoint.getRestBaseUrl());
		} catch (ExecutionException exception) {
			// that is what we want
			assertTrue(ExceptionUtils.findThrowable(exception, SSLHandshakeException.class).isPresent());
		} finally {
			if (serverEndpoint != null) {
				serverEndpoint.close();
			}
		}
	}

}
