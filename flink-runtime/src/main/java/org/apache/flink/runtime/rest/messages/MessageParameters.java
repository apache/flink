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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * This class defines the path/query {@link MessageParameter}s that can be used for a request.
 */
public abstract class MessageParameters {

	/**
	 * Returns the collection of {@link MessageParameter} that the request supports. The collection should not be
	 * modifiable.
	 *
	 * @return collection of all supported message parameters
	 */
	public abstract Collection<MessageParameter> getParameters();

	/**
	 * Returns whether all mandatory parameters have been resolved.
	 *
	 * @return true, if all mandatory parameters have been resolved, false otherwise
	 */
	public final boolean isResolved() {
		return getParameters().stream().allMatch(parameter -> parameter.isMandatory() && parameter.isResolved());
	}

	/**
	 * Resolves the given URL (e.g "jobs/:jobid") using the given path/query parameters.
	 *
	 * <p>This method will fail with an {@link IllegalStateException} if any mandatory parameter was not resolved.
	 *
	 * <p>Unresolved optional parameters will be ignored.
	 *
	 * @param genericUrl URL to resolve
	 * @param parameters message parameters parameters
	 * @return resolved url, e.g "/jobs/1234?state=running"
	 * @throws IllegalStateException if any mandatory parameter was not resolved
	 */
	public static String resolveUrl(String genericUrl, MessageParameters parameters) {
		Preconditions.checkState(parameters.isResolved(), "Not all mandatory message parameters were resolved.");
		StringBuilder path = new StringBuilder(genericUrl);
		StringBuilder queryParameters = new StringBuilder();

		boolean isFirstQueryParameter = true;
		for (MessageParameter parameter : parameters.getParameters()) {
			if (parameter.isResolved()) {
				switch (parameter.getParameterType()) {
					case PATH:
						int start = path.indexOf(":" + parameter.getKey());
						path.replace(start, start + parameter.getKey().length() + 1, parameter.getValue());
						break;
					case QUERY:
						if (isFirstQueryParameter) {
							queryParameters.append("?");
							isFirstQueryParameter = false;
						} else {
							queryParameters.append("&");
						}
						queryParameters.append(parameter.getKey());
						queryParameters.append("=");
						queryParameters.append(parameter.getValue());
						break;
				}
			}
		}
		path.append(queryParameters);

		return path.toString();
	}
}
