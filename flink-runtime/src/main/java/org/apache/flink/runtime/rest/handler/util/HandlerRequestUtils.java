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

package org.apache.flink.runtime.rest.handler.util;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;

import java.util.List;

/**
 * Utils for getting query parameters from {@link HandlerRequest}.
 */
public class HandlerRequestUtils {

	/**
	 * Returns the value of a query parameter, or {@code null} if the query parameter is not set.
	 * @throws RestHandlerException If the query parameter is repeated.
	 */
	public static <X, P extends MessageQueryParameter<X>, R extends RequestBody, M extends MessageParameters> X getQueryParameter(
			final HandlerRequest<R, M> request,
			final Class<P> queryParameterClass) throws RestHandlerException {

		return getQueryParameter(request, queryParameterClass, null);
	}

	public static <X, P extends MessageQueryParameter<X>, R extends RequestBody, M extends MessageParameters> X getQueryParameter(
			final HandlerRequest<R, M> request,
			final Class<P> queryParameterClass,
			final X defaultValue) throws RestHandlerException {

		final List<X> values = request.getQueryParameter(queryParameterClass);
		final X value;
		if (values.size() > 1) {
			throw new RestHandlerException(
				String.format("Expected only one value %s.", values),
				HttpResponseStatus.BAD_REQUEST);
		} else if (values.size() == 1) {
			value = values.get(0);
		} else {
			value = defaultValue;
		}
		return value;
	}

	/**
	 * Returns {@code requestValue} if it is not null, otherwise returns the query parameter value
	 * if it is not null, otherwise returns the default value.
	 */
	public static <T> T fromRequestBodyOrQueryParameter(
			T requestValue,
			SupplierWithException<T, RestHandlerException> queryParameterExtractor,
			T defaultValue,
			Logger log) throws RestHandlerException {
		if (requestValue != null) {
			return requestValue;
		} else {
			T queryParameterValue = queryParameterExtractor.get();
			if (queryParameterValue != null) {
				log.warn("Configuring the job submission via query parameters is deprecated." +
					" Please migrate to submitting a JSON request instead.");
				return queryParameterValue;
			} else {
				return defaultValue;
			}
		}
	}
}
