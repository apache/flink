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

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod.GET;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod.POST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Router}.
 */
public class RouterTest {
	private Router<String> router;

	@Before
	public void setUp() {
		router = StringRouter.create();
	}

	@Test
	public void testIgnoreSlashesAtBothEnds() {
		assertEquals("index", router.route(GET, "articles").target());
		assertEquals("index", router.route(GET, "/articles").target());
		assertEquals("index", router.route(GET, "//articles").target());
		assertEquals("index", router.route(GET, "articles/").target());
		assertEquals("index", router.route(GET, "articles//").target());
		assertEquals("index", router.route(GET, "/articles/").target());
		assertEquals("index", router.route(GET, "//articles//").target());
	}

	@Test
	public void testEmptyParams() {
		RouteResult<String> routed = router.route(GET, "/articles");
		assertEquals("index", routed.target());
		assertEquals(0,       routed.pathParams().size());
	}

	@Test
	public void testParams() {
		RouteResult<String> routed = router.route(GET, "/articles/123");
		assertEquals("show", routed.target());
		assertEquals(1,      routed.pathParams().size());
		assertEquals("123",  routed.pathParams().get("id"));
	}

	@Test
	public void testNone() {
		RouteResult<String> routed = router.route(GET, "/noexist");
		assertEquals("404", routed.target());
	}

	@Test
	public void testSplatWildcard() {
		RouteResult<String> routed = router.route(GET, "/download/foo/bar.png");
		assertEquals("download",    routed.target());
		assertEquals(1,             routed.pathParams().size());
		assertEquals("foo/bar.png", routed.pathParams().get("*"));
	}

	@Test
	public void testOrder() {
		RouteResult<String> routed1 = router.route(GET, "/articles/new");
		assertEquals("new", routed1.target());
		assertEquals(0,     routed1.pathParams().size());

		RouteResult<String> routed2 = router.route(GET, "/articles/123");
		assertEquals("show", routed2.target());
		assertEquals(1,      routed2.pathParams().size());
		assertEquals("123",  routed2.pathParams().get("id"));

		RouteResult<String> routed3 = router.route(GET, "/notfound");
		assertEquals("404", routed3.target());
		assertEquals(0,     routed3.pathParams().size());

		RouteResult<String> routed4 = router.route(GET, "/articles/overview");
		assertEquals("overview", routed4.target());
		assertEquals(0,     routed4.pathParams().size());

		RouteResult<String> routed5 = router.route(GET, "/articles/overview/detailed");
		assertEquals("detailed", routed5.target());
		assertEquals(0,     routed5.pathParams().size());
	}

	@Test
	public void testAnyMethod() {
		RouteResult<String> routed1 = router.route(GET, "/anyMethod");
		assertEquals("anyMethod", routed1.target());
		assertEquals(0,           routed1.pathParams().size());

		RouteResult<String> routed2 = router.route(POST, "/anyMethod");
		assertEquals("anyMethod", routed2.target());
		assertEquals(0,           routed2.pathParams().size());
	}

	@Test
	public void testRemoveByPathPattern() {
		router.removePathPattern("/articles");
		RouteResult<String> routed = router.route(GET, "/articles");
		assertEquals("404", routed.target());
	}

	@Test
	public void testAllowedMethods() {
		assertEquals(9, router.allAllowedMethods().size());

		Set<HttpMethod> methods = router.allowedMethods("/articles");
		assertEquals(2, methods.size());
		assertTrue(methods.contains(GET));
		assertTrue(methods.contains(POST));
	}

	@Test
	public void testSubclasses() {
		Router<Class<? extends Action>> router = new Router<Class<? extends Action>>()
			.addRoute(GET, "/articles",     Index.class)
			.addRoute(GET, "/articles/:id", Show.class);

		RouteResult<Class<? extends Action>> routed1 = router.route(GET, "/articles");
		RouteResult<Class<? extends Action>> routed2 = router.route(GET, "/articles/123");
		assertNotNull(routed1);
		assertNotNull(routed2);
		assertEquals(Index.class, routed1.target());
		assertEquals(Show.class,  routed2.target());
	}

	private static final class StringRouter {
		// Utility classes should not have a public or default constructor.
		private StringRouter() { }

		static Router<String> create() {
			return new Router<String>()
				.addGet("/articles", "index")
				.addGet("/articles/new", "new")
				.addGet("/articles/overview", "overview")
				.addGet("/articles/overview/detailed", "detailed")
				.addGet("/articles/:id", "show")
				.addGet("/articles/:id/:format", "show")
				.addPost("/articles", "post")
				.addPatch("/articles/:id", "patch")
				.addDelete("/articles/:id", "delete")
				.addAny("/anyMethod", "anyMethod")
				.addGet("/download/:*", "download")
				.notFound("404");
		}
	}

	private interface Action {
	}

	private class Index implements Action {
	}

	private class Show  implements Action {
	}
}
