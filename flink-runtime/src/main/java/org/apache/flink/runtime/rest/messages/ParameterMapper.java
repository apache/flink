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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to map query/path {@link Parameter}s to their actual value.
 */
public abstract class ParameterMapper {

	/**
	 * Maps the given query {@link Parameter}s to their actual value.
	 *
	 * @param queryParameters parameters to map
	 * @return map containing the parameters and their associated value
	 */
	public Map<Parameter, String> mapQueryParameters(Set<Parameter> queryParameters) {
		return Collections.emptyMap();
	}

	/**
	 * Maps the given path {@link Parameter}s to their actual value.
	 *
	 * @param pathParameters parameters to map
	 * @return map containing the parameters and their associated value
	 */
	public Map<Parameter, String> mapPathParameters(Set<Parameter> pathParameters) {
		return Collections.emptyMap();
	}

	/**
	 * Resolves the given URL (e.g "jobs/:jobid") using the given path/query parameters.
	 *
	 * @param genericUrl      URL to resolve
	 * @param pathParameters  path parameters
	 * @param queryParameters query parameters
	 * @return resolved url, e.g "/jobs/1234?state=running"
	 */
	public static String resolveUrl(String genericUrl, Map<Parameter, String> pathParameters, Map<Parameter, String> queryParameters) {
		StringBuilder sb = new StringBuilder(genericUrl);

		pathParameters.forEach((parameter, value) -> {
			int start = sb.indexOf(":" + parameter.getKey());
			sb.replace(start, start + parameter.getKey().length() + 1, value);
		});

		Iterator<Map.Entry<Parameter, String>> iterator = queryParameters.entrySet().iterator();
		if (iterator.hasNext()) {
			sb.append("?");
			Map.Entry<Parameter, String> entry = iterator.next();
			sb.append(entry.getKey().getKey());
			sb.append("=");
			sb.append(entry.getValue());
			iterator.forEachRemaining((loopEntry) -> {
				sb.append("&");
				sb.append(loopEntry.getKey().getKey());
				sb.append("=");
				sb.append(loopEntry.getValue());
			});
		}
		return sb.toString();
	}
}
