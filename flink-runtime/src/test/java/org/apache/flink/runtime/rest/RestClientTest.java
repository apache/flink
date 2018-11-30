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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.netty4.io.netty.channel.ConnectTimeoutException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link RestClient}.
 */
public class RestClientTest extends TestLogger {

	private static final String unroutableIp = "10.255.255.1";

	private static final long TIMEOUT = 10L;

	@Test
	public void testConnectionTimeout() throws Exception {
		final Configuration config = new Configuration();
		config.setLong(RestOptions.CONNECTION_TIMEOUT, 1);
		try (final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(config), Executors.directExecutor())) {
			restClient.sendRequest(
				unroutableIp,
				80,
				new TestMessageHeaders(),
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance())
				.get(60, TimeUnit.SECONDS);
		} catch (final ExecutionException e) {
			final Throwable throwable = ExceptionUtils.stripExecutionException(e);
			assertThat(throwable, instanceOf(ConnectTimeoutException.class));
			assertThat(throwable.getMessage(), containsString(unroutableIp));
		}
	}

	@Test
	public void testInvalidVersionRejection() throws Exception {
		try (final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(new Configuration()), Executors.directExecutor())) {
			CompletableFuture<EmptyResponseBody> invalidVersionResponse = restClient.sendRequest(
				unroutableIp,
				80,
				new TestMessageHeaders(),
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance(),
				Collections.emptyList(),
				RestAPIVersion.V0
			);
			Assert.fail("The request should have been rejected due to a version mismatch.");
		} catch (IllegalArgumentException e) {
			// expected
		}
	}

	/**
	 * Tests that we fail the operation if the remote connection closes.
	 */
	@Test
	public void testConnectionClosedHandling() throws Exception {
		final Configuration config = new Configuration();
		config.setLong(RestOptions.IDLENESS_TIMEOUT, 5000L);
		try (final ServerSocket serverSocket = new ServerSocket(0);
			final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(config), TestingUtils.defaultExecutor())) {

			final String targetAddress = "localhost";
			final int targetPort = serverSocket.getLocalPort();

			// start server
			final CompletableFuture<Socket> socketCompletableFuture = CompletableFuture.supplyAsync(CheckedSupplier.unchecked(serverSocket::accept));

			final CompletableFuture<EmptyResponseBody> responseFuture = restClient.sendRequest(
				targetAddress,
				targetPort,
				new TestMessageHeaders(),
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance(),
				Collections.emptyList());

			Socket connectionSocket = null;

			try {
				connectionSocket = socketCompletableFuture.get(TIMEOUT, TimeUnit.SECONDS);
			} catch (TimeoutException ignored) {
				// could not establish a server connection --> see that the response failed
				socketCompletableFuture.cancel(true);
			}

			if (connectionSocket != null) {
				// close connection
				connectionSocket.close();
			}

			try {
				responseFuture.get();
			} catch (ExecutionException ee) {
				if (!ExceptionUtils.findThrowable(ee, IOException.class).isPresent()) {
					throw ee;
				}
			}
		}
	}

	/**
	 * Tests that we fail the operation if the client closes.
	 */
	@Test
	public void testRestClientClosedHandling() throws Exception {
		final Configuration config = new Configuration();
		config.setLong(RestOptions.IDLENESS_TIMEOUT, 5000L);

		Socket connectionSocket = null;

		try (final ServerSocket serverSocket = new ServerSocket(0);
			final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(config), TestingUtils.defaultExecutor())) {

			final String targetAddress = "localhost";
			final int targetPort = serverSocket.getLocalPort();

			// start server
			final CompletableFuture<Socket> socketCompletableFuture = CompletableFuture.supplyAsync(CheckedSupplier.unchecked(serverSocket::accept));

			final CompletableFuture<EmptyResponseBody> responseFuture = restClient.sendRequest(
				targetAddress,
				targetPort,
				new TestMessageHeaders(),
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance(),
				Collections.emptyList());

			try {
				connectionSocket = socketCompletableFuture.get(TIMEOUT, TimeUnit.SECONDS);
			} catch (TimeoutException ignored) {
				// could not establish a server connection --> see that the response failed
				socketCompletableFuture.cancel(true);
			}

			restClient.close();

			try {
				responseFuture.get();
			} catch (ExecutionException ee) {
				if (!ExceptionUtils.findThrowable(ee, IOException.class).isPresent()) {
					throw ee;
				}
			}
		} finally {
			if (connectionSocket != null) {
				connectionSocket.close();
			}
		}
	}

	private static class TestMessageHeaders implements MessageHeaders<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

		@Override
		public Class<EmptyRequestBody> getRequestClass() {
			return EmptyRequestBody.class;
		}

		@Override
		public Class<EmptyResponseBody> getResponseClass() {
			return EmptyResponseBody.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public String getDescription() {
			return "";
		}

		@Override
		public EmptyMessageParameters getUnresolvedMessageParameters() {
			return EmptyMessageParameters.getInstance();
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/";
		}

	}

}
