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

package org.apache.flink.runtime.rest.handler.router;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is adopted and simplified code from tv.cntt:netty-router library. For more information check {@link Router}.
 *
 * <p>Router that doesn't contain information about HTTP request methods and route
 * matching orders.
 *
 * <p>This class is based on:
 * https://github.com/sinetja/netty-router/blob/2.2.0/src/main/java/io/netty/handler/codec/http/router/MethodlessRouter.java
 * https://github.com/sinetja/netty-router/blob/2.2.0/src/main/java/io/netty/handler/codec/http/router/OrderlessRouter.java
 */
final class MethodlessRouter<T> {
	private static final Logger log = LoggerFactory.getLogger(MethodlessRouter.class);

	// A path pattern can only point to one target
	private final Map<PathPattern, T> routes = new LinkedHashMap<>();

	//--------------------------------------------------------------------------

	/**
	 * Returns all routes in this router, an unmodifiable map of {@code PathPattern -> Target}.
	 */
	public Map<PathPattern, T> routes() {
		return Collections.unmodifiableMap(routes);
	}

	/**
	 * This method does nothing if the path pattern has already been added.
	 * A path pattern can only point to one target.
	 */
	public MethodlessRouter<T> addRoute(String pathPattern, T target) {
		PathPattern p = new PathPattern(pathPattern);
		if (routes.containsKey(p)) {
			return this;
		}

		routes.put(p, target);
		return this;
	}

	//--------------------------------------------------------------------------

	/**
	 * Removes the route specified by the path pattern.
	 */
	public void removePathPattern(String pathPattern) {
		PathPattern p = new PathPattern(pathPattern);
		T target = routes.remove(p);
		if (target == null) {
			return;
		}
	}

	//--------------------------------------------------------------------------

	/**
	 * @return {@code null} if no match
	 */
	public RouteResult<T> route(String uri, String decodedPath, Map<String, List<String>> queryParameters, String[] pathTokens) {
		// Optimize: reuse requestPathTokens and pathParams in the loop
		Map<String, String> pathParams = new HashMap<>();
		for (Entry<PathPattern, T> entry : routes.entrySet()) {
			PathPattern pattern = entry.getKey();
			if (pattern.match(pathTokens, pathParams)) {
				T target = entry.getValue();
				return new RouteResult<T>(uri, decodedPath, pathParams, queryParameters, target);
			}

			// Reset for the next try
			pathParams.clear();
		}

		return null;
	}

	/**
	 * Checks if there's any matching route.
	 */
	public boolean anyMatched(String[] requestPathTokens) {
		Map<String, String> pathParams = new HashMap<>();
		for (PathPattern pattern : routes.keySet()) {
			if (pattern.match(requestPathTokens, pathParams)) {
				return true;
			}

			// Reset for the next loop
			pathParams.clear();
		}

		return false;
	}

	public int size() {
		return routes.size();
	}
}
