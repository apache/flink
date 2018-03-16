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
	 * Returns the collection of {@link MessagePathParameter} that the request supports. The collection should not be
	 * modifiable.
	 *
	 * @return collection of all supported message path parameters
	 */
	public abstract Collection<MessagePathParameter<?>> getPathParameters();

	/**
	 * Returns the collection of {@link MessageQueryParameter} that the request supports. The collection should not be
	 * modifiable.
	 *
	 * @return collection of all supported message query parameters
	 */
	public abstract Collection<MessageQueryParameter<?>> getQueryParameters();

	/**
	 * Returns whether all mandatory parameters have been resolved.
	 *
	 * @return true, if all mandatory parameters have been resolved, false otherwise
	 */
	public final boolean isResolved() {
		return getPathParameters().stream().filter(MessageParameter::isMandatory).allMatch(MessageParameter::isResolved)
			&& getQueryParameters().stream().filter(MessageParameter::isMandatory).allMatch(MessageParameter::isResolved);
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

		for (MessageParameter<?> pathParameter : parameters.getPathParameters()) {
			if (pathParameter.isResolved()) {
				int start = path.indexOf(':' + pathParameter.getKey());

				final String pathValue = Preconditions.checkNotNull(pathParameter.getValueAsString());

				// only replace path parameters if they are present
				if (start != -1) {
					path.replace(start, start + pathParameter.getKey().length() + 1, pathValue);
				}
			}
		}
		boolean isFirstQueryParameter = true;
		for (MessageQueryParameter<?> queryParameter : parameters.getQueryParameters()) {
			if (queryParameter.isResolved()) {
				if (isFirstQueryParameter) {
					queryParameters.append('?');
					isFirstQueryParameter = false;
				} else {
					queryParameters.append('&');
				}
				queryParameters.append(queryParameter.getKey());
				queryParameters.append('=');
				queryParameters.append(queryParameter.getValueAsString());
			}
		}
		path.append(queryParameters);

		return path.toString();
	}
}
