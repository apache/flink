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

import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Simple container for the request to a handler, that contains the {@link RequestBody} and path/query parameters.
 *
 * @param <R> type of the contained request body
 * @param <M> type of the contained message parameters
 */
public class HandlerRequest<R extends RequestBody, M extends MessageParameters> {

	private final R requestBody;
	private final Map<Class<? extends MessagePathParameter>, MessagePathParameter<?>> pathParameters = new HashMap<>();
	private final Map<Class<? extends MessageQueryParameter>, MessageQueryParameter<?>> queryParameters = new HashMap<>();

	public HandlerRequest(R requestBody, M messageParameters, Map<String, String> pathParameters, Map<String, List<String>> queryParameters) {
		this.requestBody = Preconditions.checkNotNull(requestBody);
		Preconditions.checkNotNull(messageParameters);
		Preconditions.checkNotNull(queryParameters);
		Preconditions.checkNotNull(pathParameters);

		for (MessagePathParameter<?> pathParameter : messageParameters.getPathParameters()) {
			String value = pathParameters.get(pathParameter.getKey());
			if (value != null) {
				pathParameter.resolveFromString(value);
				this.pathParameters.put(pathParameter.getClass(), pathParameter);
			}
		}

		for (MessageQueryParameter<?> queryParameter : messageParameters.getQueryParameters()) {
			List<String> values = queryParameters.get(queryParameter.getKey());
			if (values != null && values.size() > 0) {
				StringJoiner joiner = new StringJoiner(",");
				values.forEach(joiner::add);
				queryParameter.resolveFromString(joiner.toString());
				this.queryParameters.put(queryParameter.getClass(), queryParameter);
			}

		}
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
	 * Returns the {@link MessagePathParameter} for the given class.
	 *
	 * @param parameterClass class of the parameter
	 * @param <X>            the value type that the parameter contains
	 * @param <PP>           type of the path parameter
	 * @return path parameter for the given class, or null if no parameter value exists for the given class
	 */
	@SuppressWarnings("unchecked")
	public <X, PP extends MessagePathParameter<X>> PP getPathParameter(Class<PP> parameterClass) {
		return (PP) pathParameters.get(parameterClass);
	}

	/**
	 * Returns the {@link MessageQueryParameter} for the given class.
	 *
	 * @param parameterClass class of the parameter
	 * @param <X>            the value type that the parameter contains
	 * @param <QP>           type of the query parameter
	 * @return query parameter for the given class, or null if no parameter value exists for the given class
	 */
	@SuppressWarnings("unchecked")
	public <X, QP extends MessageQueryParameter<X>> QP getQueryParameter(Class<QP> parameterClass) {
		return (QP) queryParameters.get(parameterClass);
	}
}
