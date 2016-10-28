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

import io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.runtime.instance.ActorGateway;

import java.util.Map;

/**
 * Base interface for all request handlers.
 *
 * <p>Most handlers will want to use the {@link AbstractJsonRequestHandler}
 * as a starting point, which produces a valid HTTP response.
 */
public interface RequestHandler {

	/**
	 * Core method that handles the request and generates the response. The method needs to
	 * respond with a full http response, including content-type, content-length, etc.
	 *
	 * <p>Exceptions may be throws and will be handled.
	 * 
	 * @param pathParams The map of REST path parameters, decoded by the router.
	 * @param queryParams The map of query parameters.
	 * @param jobManager The JobManager actor.
	 *
	 * @return The full http response.
	 * 
	 * @throws Exception Handlers may forward exceptions. Exceptions of type
	 *         {@link org.apache.flink.runtime.webmonitor.NotFoundException} will cause a HTTP 404
	 *         response with the exception message, other exceptions will cause a HTTP 500 response
	 *         with the exception stack trace.
	 */
	FullHttpResponse handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception;
}
