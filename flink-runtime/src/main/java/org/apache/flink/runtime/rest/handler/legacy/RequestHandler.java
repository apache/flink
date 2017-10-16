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

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
	 * @param jobManagerGateway to talk to the JobManager.
	 *
	 * @return The full http response.
	 */
	CompletableFuture<FullHttpResponse> handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway);

	/**
	 * Returns an array of REST URL's under which this handler can be registered.
	 *
	 * @return array containing REST URL's under which this handler can be registered.
	 */
	String[] getPaths();
}
