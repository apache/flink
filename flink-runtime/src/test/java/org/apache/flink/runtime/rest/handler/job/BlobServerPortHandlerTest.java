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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.BlobServerPortResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link BlobServerPortHandler}.
 */
@Category(New.class)
public class BlobServerPortHandlerTest extends TestLogger {
	private static final int PORT = 64;

	@Test
	public void testPortRetrieval() throws Exception {
		DispatcherGateway mockGateway = mock(DispatcherGateway.class);
		when(mockGateway.getBlobServerPort(any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(PORT));
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		BlobServerPortHandler handler = new BlobServerPortHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap());

		BlobServerPortResponseBody portResponse = handler
			.handleRequest(new HandlerRequest<>(EmptyRequestBody.getInstance(), EmptyMessageParameters.getInstance()), mockGateway)
			.get();

		Assert.assertEquals(PORT, portResponse.port);
	}

	@Test
	public void testPortRetrievalFailureHandling() throws Exception {
		DispatcherGateway mockGateway = mock(DispatcherGateway.class);
		when(mockGateway.getBlobServerPort(any(Time.class)))
			.thenReturn(FutureUtils.completedExceptionally(new TestException()));
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		BlobServerPortHandler handler = new BlobServerPortHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap());

		try {
			handler
				.handleRequest(new HandlerRequest<>(EmptyRequestBody.getInstance(), EmptyMessageParameters.getInstance()), mockGateway)
				.get();
			Assert.fail();
		} catch (ExecutionException ee) {
			RestHandlerException rhe = (RestHandlerException) ee.getCause();

			Assert.assertEquals(TestException.class, rhe.getCause().getClass());
			Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, rhe.getHttpResponseStatus());
		}
	}

	private static class TestException extends Exception {
		private static final long serialVersionUID = -7064446788277853899L;
	}
}
