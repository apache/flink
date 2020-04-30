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

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This is adopted and simplified code from tv.cntt:netty-router library. Compared to original version this one
 * defines and guarantees an order of pattern matching routes, drops reverse routing feature and restores
 * {@link RouterHandler} which was dropped in tv.cntt:netty-router 2.X.X. Original code:
 * https://github.com/sinetja/netty-router/blob/2.2.0/src/main/java/io/netty/handler/codec/http/router/Router.java
 *
 * <p>Router that contains information about both route matching orders and
 * HTTP request methods.
 *
 * <p>Routes are guaranteed to be matched in order of their addition.
 *
 * <p>Route targets can be any type. In the below example, targets are classes:
 *
 * <pre>
 * {@code
 * Router<Class> router = new Router<Class>()
 *   .GET      ("/articles",     IndexHandler.class)
 *   .GET      ("/articles/:id", ShowHandler.class)
 *   .POST     ("/articles",     CreateHandler.class)
 *   .GET      ("/download/:*",  DownloadHandler.class)  // ":*" must be the last token
 *   .GET_FIRST("/articles/new", NewHandler.class);      // This will be matched first
 * }
 * </pre>
 *
 * <p>Slashes at both ends are ignored. These are the same:
 *
 * <pre>
 * {@code
 * router.GET("articles",   IndexHandler.class);
 * router.GET("/articles",  IndexHandler.class);
 * router.GET("/articles/", IndexHandler.class);
 * }
 * </pre>
 *
 * <p>You can remove routes by target or by path pattern:
 *
 * <pre>
 * {@code
 * router.removePathPattern("/articles");
 * }
 * </pre>
 *
 * <p>To match requests use {@link #route(HttpMethod, String)}.
 *
 * <p>From the {@link RouteResult} you can extract params embedded in
 * the path and from the query part of the request URI.
 *
 * <p>{@link #notFound(Object)} will be used as the target when there's no match.
 *
 * <pre>
 * {@code
 * router.notFound(My404Handler.class);
 * }
 * </pre>
 */
public class Router<T> {
	private final Map<HttpMethod, MethodlessRouter<T>> routers =
		new HashMap<HttpMethod, MethodlessRouter<T>>();

	private final MethodlessRouter<T> anyMethodRouter =
		new MethodlessRouter<T>();

	private T notFound;

	//--------------------------------------------------------------------------
	// Design decision:
	// We do not allow access to routers and anyMethodRouter, because we don't
	// want to expose MethodlessRouter, OrderlessRouter, and PathPattern.
	// Exposing those will complicate the use of this package.

	/**
	 * Helper for toString.
	 */
	private static <T> void aggregateRoutes(
		String method, Map<PathPattern, T> routes,
		List<String> accMethods, List<String> accPatterns, List<String> accTargets) {
		for (Map.Entry<PathPattern, T> entry : routes.entrySet()) {
			accMethods.add(method);
			accPatterns.add("/" + entry.getKey().pattern());
			accTargets.add(targetToString(entry.getValue()));
		}
	}

	/**
	 * Helper for toString.
	 */
	private static int maxLength(List<String> coll) {
		int max = 0;
		for (String e : coll) {
			int length = e.length();
			if (length > max) {
				max = length;
			}
		}
		return max;
	}

	/**
	 * Helper for toString.
	 *
	 * <p>For example, returns
	 * "io.netty.example.http.router.HttpRouterServerHandler" instead of
	 * "class io.netty.example.http.router.HttpRouterServerHandler"
	 */
	private static String targetToString(Object target) {
		if (target instanceof Class) {
			return ((Class<?>) target).getName();
		} else {
			return target.toString();
		}
	}

	/**
	 * Returns the fallback target for use when there's no match at
	 * {@link #route(HttpMethod, String)}.
	 */
	public T notFound() {
		return notFound;
	}

	/**
	 * Returns the number of routes in this router.
	 */
	public int size() {
		int ret = anyMethodRouter.size();

		for (MethodlessRouter<T> router : routers.values()) {
			ret += router.size();
		}

		return ret;
	}

	//--------------------------------------------------------------------------

	/**
	 * Add route.
	 *
	 * <p>A path pattern can only point to one target. This method does nothing if the pattern
	 * has already been added.
	 */
	public Router<T> addRoute(HttpMethod method, String pathPattern, T target) {
		getMethodlessRouter(method).addRoute(pathPattern, target);
		return this;
	}

	//--------------------------------------------------------------------------

	/**
	 * Sets the fallback target for use when there's no match at
	 * {@link #route(HttpMethod, String)}.
	 */
	public Router<T> notFound(T target) {
		this.notFound = target;
		return this;
	}

	private MethodlessRouter<T> getMethodlessRouter(HttpMethod method) {
		if (method == null) {
			return anyMethodRouter;
		}

		MethodlessRouter<T> router = routers.get(method);
		if (router == null) {
			router = new MethodlessRouter<T>();
			routers.put(method, router);
		}

		return router;
	}

	/**
	 * Removes the route specified by the path pattern.
	 */
	public void removePathPattern(String pathPattern) {
		for (MethodlessRouter<T> router : routers.values()) {
			router.removePathPattern(pathPattern);
		}
		anyMethodRouter.removePathPattern(pathPattern);
	}

	/**
	 * If there's no match, returns the result with {@link #notFound(Object) notFound}
	 * as the target if it is set, otherwise returns {@code null}.
	 */
	public RouteResult<T> route(HttpMethod method, String path) {
		return route(method, path, Collections.emptyMap());
	}

	public RouteResult<T> route(HttpMethod method, String path, Map<String, List<String>> queryParameters) {
		MethodlessRouter<T> router = routers.get(method);
		if (router == null) {
			router = anyMethodRouter;
		}

		String[] tokens = decodePathTokens(path);

		RouteResult<T> ret = router.route(path, path, queryParameters, tokens);
		if (ret != null) {
			return new RouteResult<T>(path, path, ret.pathParams(), queryParameters, ret.target());
		}

		if (router != anyMethodRouter) {
			ret = anyMethodRouter.route(path, path, queryParameters, tokens);
			if (ret != null) {
				return new RouteResult<T>(path, path, ret.pathParams(), queryParameters, ret.target());
			}
		}

		if (notFound != null) {
			return new RouteResult<T>(path, path, Collections.<String, String>emptyMap(), queryParameters, notFound);
		}

		return null;
	}

	//--------------------------------------------------------------------------

	private String[] decodePathTokens(String uri) {
		// Need to split the original URI (instead of QueryStringDecoder#path) then decode the tokens (components),
		// otherwise /test1/123%2F456 will not match /test1/:p1

		int qPos = uri.indexOf("?");
		String encodedPath = (qPos >= 0) ? uri.substring(0, qPos) : uri;

		String[] encodedTokens = PathPattern.removeSlashesAtBothEnds(encodedPath).split("/");

		String[] decodedTokens = new String[encodedTokens.length];
		for (int i = 0; i < encodedTokens.length; i++) {
			String encodedToken = encodedTokens[i];
			decodedTokens[i] = QueryStringDecoder.decodeComponent(encodedToken);
		}

		return decodedTokens;
	}

	/**
	 * Returns allowed methods for a specific URI.
	 *
	 * <p>For {@code OPTIONS *}, use {@link #allAllowedMethods()} instead of this method.
	 */
	public Set<HttpMethod> allowedMethods(String uri) {
		QueryStringDecoder decoder = new QueryStringDecoder(uri);
		String[] tokens = PathPattern.removeSlashesAtBothEnds(decoder.path()).split("/");

		if (anyMethodRouter.anyMatched(tokens)) {
			return allAllowedMethods();
		}

		Set<HttpMethod> ret = new HashSet<HttpMethod>(routers.size());
		for (Map.Entry<HttpMethod, MethodlessRouter<T>> entry : routers.entrySet()) {
			MethodlessRouter<T> router = entry.getValue();
			if (router.anyMatched(tokens)) {
				HttpMethod method = entry.getKey();
				ret.add(method);
			}
		}

		return ret;
	}

	/**
	 * Returns all methods that this router handles. For {@code OPTIONS *}.
	 */
	public Set<HttpMethod> allAllowedMethods() {
		if (anyMethodRouter.size() > 0) {
			Set<HttpMethod> ret = new HashSet<HttpMethod>(9);
			ret.add(HttpMethod.CONNECT);
			ret.add(HttpMethod.DELETE);
			ret.add(HttpMethod.GET);
			ret.add(HttpMethod.HEAD);
			ret.add(HttpMethod.OPTIONS);
			ret.add(HttpMethod.PATCH);
			ret.add(HttpMethod.POST);
			ret.add(HttpMethod.PUT);
			ret.add(HttpMethod.TRACE);
			return ret;
		} else {
			return new HashSet<HttpMethod>(routers.keySet());
		}
	}

	/**
	 * Returns visualized routing rules.
	 */
	@Override
	public String toString() {
		// Step 1/2: Dump routers and anyMethodRouter in order
		int numRoutes = size();
		List<String> methods = new ArrayList<String>(numRoutes);
		List<String> patterns = new ArrayList<String>(numRoutes);
		List<String> targets = new ArrayList<String>(numRoutes);

		// For router
		for (Entry<HttpMethod, MethodlessRouter<T>> e : routers.entrySet()) {
			HttpMethod method = e.getKey();
			MethodlessRouter<T> router = e.getValue();
			aggregateRoutes(method.toString(), router.routes(), methods, patterns, targets);
		}

		// For anyMethodRouter
		aggregateRoutes("*", anyMethodRouter.routes(), methods, patterns, targets);

		// For notFound
		if (notFound != null) {
			methods.add("*");
			patterns.add("*");
			targets.add(targetToString(notFound));
		}

		// Step 2/2: Format the List into aligned columns: <method> <patterns> <target>
		int maxLengthMethod = maxLength(methods);
		int maxLengthPattern = maxLength(patterns);
		String format = "%-" + maxLengthMethod + "s  %-" + maxLengthPattern + "s  %s\n";
		int initialCapacity = (maxLengthMethod + 1 + maxLengthPattern + 1 + 20) * methods.size();
		StringBuilder b = new StringBuilder(initialCapacity);
		for (int i = 0; i < methods.size(); i++) {
			String method = methods.get(i);
			String pattern = patterns.get(i);
			String target = targets.get(i);
			b.append(String.format(format, method, pattern, target));
		}
		return b.toString();
	}

	//--------------------------------------------------------------------------

	public Router<T> addConnect(String path, T target) {
		return addRoute(HttpMethod.CONNECT, path, target);
	}

	public Router<T> addDelete(String path, T target) {
		return addRoute(HttpMethod.DELETE, path, target);
	}

	public Router<T> addGet(String path, T target) {
		return addRoute(HttpMethod.GET, path, target);
	}

	public Router<T> addHead(String path, T target) {
		return addRoute(HttpMethod.HEAD, path, target);
	}

	public Router<T> addOptions(String path, T target) {
		return addRoute(HttpMethod.OPTIONS, path, target);
	}

	public Router<T> addPatch(String path, T target) {
		return addRoute(HttpMethod.PATCH, path, target);
	}

	public Router<T> addPost(String path, T target) {
		return addRoute(HttpMethod.POST, path, target);
	}

	public Router<T> addPut(String path, T target) {
		return addRoute(HttpMethod.PUT, path, target);
	}

	public Router<T> addTrace(String path, T target) {
		return addRoute(HttpMethod.TRACE, path, target);
	}

	public Router<T> addAny(String path, T target) {
		return addRoute(null, path, target);
	}
}
