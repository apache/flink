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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod.GET;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod.POST;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Router}. */
class RouterTest {
    private Router<String> router;

    @BeforeEach
    public void setUp() {
        router = StringRouter.create();
    }

    @Test
    void testIgnoreSlashesAtBothEnds() {
        assertThat(router.route(GET, "articles").target()).isEqualTo("index");
        assertThat(router.route(GET, "/articles").target()).isEqualTo("index");
        assertThat(router.route(GET, "//articles").target()).isEqualTo("index");
        assertThat(router.route(GET, "articles/").target()).isEqualTo("index");
        assertThat(router.route(GET, "articles//").target()).isEqualTo("index");
        assertThat(router.route(GET, "/articles/").target()).isEqualTo("index");
        assertThat(router.route(GET, "//articles//").target()).isEqualTo("index");
    }

    @Test
    void testEmptyParams() {
        RouteResult<String> routed = router.route(GET, "/articles");
        assertThat(routed.target()).isEqualTo("index");
        assertThat(routed.pathParams()).isEmpty();
    }

    @Test
    void testParams() {
        RouteResult<String> routed = router.route(GET, "/articles/123");
        assertThat(routed.target()).isEqualTo("show");
        assertThat(routed.pathParams()).hasSize(1);
        assertThat(routed.pathParams().get("id")).isEqualTo("123");
    }

    @Test
    void testNone() {
        RouteResult<String> routed = router.route(GET, "/noexist");
        assertThat(routed.target()).isEqualTo("404");
    }

    @Test
    void testSplatWildcard() {
        RouteResult<String> routed = router.route(GET, "/download/foo/bar.png");
        assertThat(routed.target()).isEqualTo("download");
        assertThat(routed.pathParams()).hasSize(1);
        assertThat(routed.pathParams().get("*")).isEqualTo("foo/bar.png");
    }

    @Test
    void testOrder() {
        RouteResult<String> routed1 = router.route(GET, "/articles/new");
        assertThat(routed1.target()).isEqualTo("new");
        assertThat(routed1.pathParams()).isEmpty();

        RouteResult<String> routed2 = router.route(GET, "/articles/123");
        assertThat(routed2.target()).isEqualTo("show");
        assertThat(routed2.pathParams()).hasSize(1);
        assertThat(routed2.pathParams().get("id")).isEqualTo("123");

        RouteResult<String> routed3 = router.route(GET, "/notfound");
        assertThat(routed3.target()).isEqualTo("404");
        assertThat(routed3.pathParams()).isEmpty();

        RouteResult<String> routed4 = router.route(GET, "/articles/overview");
        assertThat(routed4.target()).isEqualTo("overview");
        assertThat(routed4.pathParams()).isEmpty();

        RouteResult<String> routed5 = router.route(GET, "/articles/overview/detailed");
        assertThat(routed5.target()).isEqualTo("detailed");
        assertThat(routed5.pathParams()).isEmpty();
    }

    @Test
    void testAnyMethod() {
        RouteResult<String> routed1 = router.route(GET, "/anyMethod");
        assertThat(routed1.target()).isEqualTo("anyMethod");
        assertThat(routed1.pathParams()).isEmpty();

        RouteResult<String> routed2 = router.route(POST, "/anyMethod");
        assertThat(routed2.target()).isEqualTo("anyMethod");
        assertThat(routed2.pathParams()).isEmpty();
    }

    @Test
    void testRemoveByPathPattern() {
        router.removePathPattern("/articles");
        RouteResult<String> routed = router.route(GET, "/articles");
        assertThat(routed.target()).isEqualTo("404");
    }

    @Test
    void testAllowedMethods() {
        assertThat(router.allAllowedMethods()).hasSize(9);

        Set<HttpMethod> methods = router.allowedMethods("/articles");
        assertThat(methods).hasSize(2);
        assertThat(methods.contains(GET)).isTrue();
        assertThat(methods.contains(POST)).isTrue();
    }

    @Test
    void testSubclasses() {
        Router<Class<? extends Action>> router =
                new Router<Class<? extends Action>>()
                        .addRoute(GET, "/articles", Index.class)
                        .addRoute(GET, "/articles/:id", Show.class);

        RouteResult<Class<? extends Action>> routed1 = router.route(GET, "/articles");
        RouteResult<Class<? extends Action>> routed2 = router.route(GET, "/articles/123");
        assertThat(routed1).isNotNull();
        assertThat(routed2).isNotNull();
        assertThat(routed1.target()).isEqualTo(Index.class);
        assertThat(routed2.target()).isEqualTo(Show.class);
    }

    private static final class StringRouter {
        // Utility classes should not have a public or default constructor.
        private StringRouter() {}

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

    private interface Action {}

    private class Index implements Action {}

    private class Show implements Action {}
}
