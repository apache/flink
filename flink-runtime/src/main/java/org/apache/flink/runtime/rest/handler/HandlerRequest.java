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

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * Simple container for the request to a handler, that contains the {@link RequestBody} and path/query parameters.
 */
public class HandlerRequest<R extends RequestBody> {
	private final R requestBody;
	private final Map<String, String> pathParameters;
	private final Map<String, List<String>> queryParameters;

	public HandlerRequest(R requestBody, Map<String, String> pathParameters, Map<String, List<String>> queryParameters) {
		this.requestBody = Preconditions.checkNotNull(requestBody);
		this.pathParameters = Preconditions.checkNotNull(pathParameters);
		this.queryParameters = Preconditions.checkNotNull(queryParameters);
	}

	/**
	 * Returns the request body.
	 *
	 * @return request body
	 */
	public R getRequestBody() {
		return requestBody;
	}

	/**
	 * Returns a map containing all query parameters.
	 *
	 * @return query parameters
	 */
	public Map<String, List<String>> getQueryParameters() {
		return queryParameters;
	}

	/**
	 * Returns a map containing all path parameters.
	 *
	 * @return path parameters
	 */
	public Map<String, String> getPathParameters() {
		return pathParameters;
	}
}
