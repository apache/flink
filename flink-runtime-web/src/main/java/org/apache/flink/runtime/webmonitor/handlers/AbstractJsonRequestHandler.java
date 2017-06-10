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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.instance.ActorGateway;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Base class for most request handlers. The handlers must produce a JSON response.
 */
public abstract class AbstractJsonRequestHandler implements RequestHandler {

	private static final Charset ENCODING = Charset.forName("UTF-8");

	@Override
	public FullHttpResponse handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		String result = handleJsonRequest(pathParams, queryParams, jobManager);
		byte[] bytes = result.getBytes(ENCODING);

		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		return response;
	}

	/**
	 * Core method that handles the request and generates the response. The method needs to
	 * respond with a valid JSON string. Exceptions may be thrown and will be handled.
	 *
	 * @param pathParams The map of REST path parameters, decoded by the router.
	 * @param queryParams The map of query parameters.
	 * @param jobManager The JobManager actor.
	 *
	 * @return The JSON string that is the HTTP response.
	 *
	 * @throws Exception Handlers may forward exceptions. Exceptions of type
	 *         {@link org.apache.flink.runtime.webmonitor.NotFoundException} will cause a HTTP 404
	 *         response with the exception message, other exceptions will cause a HTTP 500 response
	 *         with the exception stack trace.
	 */
	public abstract String handleJsonRequest(
			Map<String, String> pathParams,
			Map<String, String> queryParams,
			ActorGateway jobManager) throws Exception;

}
