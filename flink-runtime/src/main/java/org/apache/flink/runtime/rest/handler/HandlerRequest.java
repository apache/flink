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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Simple container for the request to a handler, that contains the {@link RequestBody} and path/query parameters.
 *
 * @param <R> type of the contained request body
 * @param <M> type of the contained message parameters
 */
public class HandlerRequest<R extends RequestBody, M extends MessageParameters> {

	private final R requestBody;
	private final Map<Class<? extends MessagePathParameter<?>>, MessagePathParameter<?>> pathParameters;
	private final Map<Class<? extends MessageQueryParameter<?>>, MessageQueryParameter<?>> queryParameters;

	public HandlerRequest(R requestBody, M messageParameters) throws HandlerRequestException {
		this(requestBody, messageParameters, Collections.emptyMap(), Collections.emptyMap());
	}

	@SuppressWarnings("unchecked")
	public HandlerRequest(R requestBody, M messageParameters, Map<String, String> receivedPathParameters, Map<String, List<String>> receivedQueryParameters) throws HandlerRequestException {
		this.requestBody = Preconditions.checkNotNull(requestBody);
		Preconditions.checkNotNull(messageParameters);
		Preconditions.checkNotNull(receivedQueryParameters);
		Preconditions.checkNotNull(receivedPathParameters);

		try {
			messageParameters.resolveParameters(receivedPathParameters, receivedQueryParameters);
		}
		catch (IllegalArgumentException e) {
			throw new HandlerRequestException("Unable to resolve the request parameters: " + e.getMessage());
		}

		pathParameters = messageParameters.getPathParameters().stream().collect(Collectors.toMap(p -> (Class<? extends MessagePathParameter<?>>) p.getClass(), Function.identity()));
		queryParameters = messageParameters.getQueryParameters().stream().collect(Collectors.toMap(p -> (Class<? extends MessageQueryParameter<?>>) p.getClass(), Function.identity()));
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
	 * Returns the value of the {@link MessagePathParameter} for the given class.
	 *
	 * @param parameterClass class of the parameter
	 * @param <X>            the value type that the parameter contains
	 * @param <PP>           type of the path parameter
	 * @return path parameter value for the given class
	 * @throws IllegalStateException if no value is defined for the given parameter class
	 */
	public <X, PP extends MessagePathParameter<X>> X getPathParameter(Class<PP> parameterClass) {
		@SuppressWarnings("unchecked")
		PP pathParameter = (PP) pathParameters.get(parameterClass);
		Preconditions.checkState(pathParameter != null, "No parameter could be found for the given class.");
		return pathParameter.getValue();
	}

	/**
	 * Returns the value of the {@link MessageQueryParameter} for the given class.
	 *
	 * @param parameterClass class of the parameter
	 * @param <X>            the value type that the parameter contains
	 * @param <QP>           type of the query parameter
	 * @return query parameter value for the given class, or an empty list if no parameter value exists for the given class
	 */
	public <X, QP extends MessageQueryParameter<X>> List<X> getQueryParameter(Class<QP> parameterClass) {
		@SuppressWarnings("unchecked")
		QP queryParameter = (QP) queryParameters.get(parameterClass);
		if (queryParameter == null) {
			return Collections.emptyList();
		} else {
			return queryParameter.getValue();
		}
	}
}
