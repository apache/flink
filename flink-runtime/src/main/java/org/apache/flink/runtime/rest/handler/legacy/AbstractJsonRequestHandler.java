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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base class for most request handlers. The handlers must produce a JSON response.
 */
public abstract class AbstractJsonRequestHandler implements RequestHandler {

	private static final Charset ENCODING = Charset.forName("UTF-8");

	protected final Executor executor;

	protected AbstractJsonRequestHandler(Executor executor) {
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Override
	public CompletableFuture<FullHttpResponse> handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		CompletableFuture<String> resultFuture = handleJsonRequest(pathParams, queryParams, jobManagerGateway);

		return resultFuture.thenApplyAsync(
			(String result) -> {
				byte[] bytes = result.getBytes(ENCODING);

				DefaultFullHttpResponse response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));

				response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ENCODING.name());
				response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

				return response;
			});
	}

	/**
	 * Core method that handles the request and generates the response. The method needs to
	 * respond with a valid JSON string. Exceptions may be thrown and will be handled.
	 *
	 * @param pathParams The map of REST path parameters, decoded by the router.
	 * @param queryParams The map of query parameters.
	 * @param jobManagerGateway to communicate with the JobManager.
	 *
	 * @return The JSON string that is the HTTP response.
	 *
	 * @throws Exception Handlers may forward exceptions. Exceptions of type
	 *         {@link NotFoundException} will cause a HTTP 404
	 *         response with the exception message, other exceptions will cause a HTTP 500 response
	 *         with the exception stack trace.
	 */
	public abstract CompletableFuture<String> handleJsonRequest(
			Map<String, String> pathParams,
			Map<String, String> queryParams,
			JobManagerGateway jobManagerGateway);

}
