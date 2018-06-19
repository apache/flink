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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ConfigurationException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for the multipart functionality of the {@link RestClient}.
 */
public class RestClientMultipartTest extends MultipartUploadTestBase {

	private static RestClient restClient;

	@BeforeClass
	public static void setupClient() throws ConfigurationException {
		restClient = new RestClient(RestClientConfiguration.fromConfiguration(new Configuration()), TestingUtils.defaultExecutor());
	}

	@AfterClass
	public static void teardownClient() {
		if (restClient != null) {
			restClient.shutdown(Time.seconds(10));
		}
	}

	@Test
	public void testMixedMultipart() throws Exception {
		Collection<FileUpload> files = Arrays.asList(new FileUpload(file1.toPath(), "application/octet-stream"), new FileUpload(file2.toPath(), "application/octet-stream"));

		TestRequestBody json = new TestRequestBody();
		CompletableFuture<EmptyResponseBody> responseFuture = restClient.sendRequest(
			serverSocketAddress.getHostName(),
			serverSocketAddress.getPort(),
			mixedHandler.getMessageHeaders(),
			EmptyMessageParameters.getInstance(),
			json,
			files
		);

		responseFuture.get();
		Assert.assertEquals(json, mixedHandler.lastReceivedRequest);
	}

	@Test
	public void testJsonMultipart() throws Exception {
		TestRequestBody json = new TestRequestBody();
		CompletableFuture<EmptyResponseBody> responseFuture = restClient.sendRequest(
			serverSocketAddress.getHostName(),
			serverSocketAddress.getPort(),
			jsonHandler.getMessageHeaders(),
			EmptyMessageParameters.getInstance(),
			json,
			Collections.emptyList()
		);

		responseFuture.get();
		Assert.assertEquals(json, jsonHandler.lastReceivedRequest);
	}

	@Test
	public void testFileMultipart() throws Exception {
		Collection<FileUpload> files = Arrays.asList(new FileUpload(file1.toPath(), "application/octet-stream"), new FileUpload(file2.toPath(), "application/octet-stream"));

		CompletableFuture<EmptyResponseBody> responseFuture = restClient.sendRequest(
			serverSocketAddress.getHostName(),
			serverSocketAddress.getPort(),
			fileHandler.getMessageHeaders(),
			EmptyMessageParameters.getInstance(),
			EmptyRequestBody.getInstance(),
			files
		);

		responseFuture.get();
	}
}
