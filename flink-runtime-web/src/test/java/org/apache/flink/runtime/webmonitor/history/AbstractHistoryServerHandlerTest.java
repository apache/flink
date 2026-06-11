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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Common HTTP-level tests for {@link AbstractHistoryServerHandler} subclasses. New subclasses can
 * be exercised by adding an entry to {@link #handlerFactories()}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class AbstractHistoryServerHandlerTest {

    /** Factory that creates a concrete handler bound to the given web directory. */
    @FunctionalInterface
    public interface HandlerFactory {
        AbstractHistoryServerHandler<?> create(File webDir) throws Exception;
    }

    /**
     * Provides the concrete {@link AbstractHistoryServerHandler} implementations under test. To
     * cover an additional handler simply add another factory here.
     */
    @Parameters(name = "handlerFactory={0}")
    private static Collection<HandlerFactory> handlerFactories() {
        HandlerFactory staticFileServerHandlerFactory = HistoryServerStaticFileServerHandler::new;
        return Collections.singletonList(staticFileServerHandlerFactory);
    }

    @Parameter public HandlerFactory handlerFactory;

    @TempDir private Path tmpDir;

    private Path webDir;
    private AbstractHistoryServerHandler<?> handler;
    private WebFrontendBootstrap webUI;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path uploadDir = Files.createDirectory(tmpDir.resolve("uploadDir"));

        handler = handlerFactory.create(webDir.toFile());
        Router<?> router = new Router().addGet("/:*", handler);
        webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(getClass()),
                        uploadDir.toFile(),
                        null,
                        "localhost",
                        0,
                        new Configuration());
        baseUrl = "http://localhost:" + webUI.getServerPort();
    }

    @AfterEach
    void tearDown() {
        if (webUI != null) {
            webUI.shutdown();
        }
    }

    /**
     * Tests requests against static files served from the {@code web/} resources packaged with the
     * handler module:
     *
     * <ul>
     *   <li>a request for {@code /} is rewritten to {@code /index.html};
     *   <li>{@code /index.html} is loaded via the classloader fallback when not present on disk;
     *   <li>a missing static file (e.g. {@code /hello.html}) results in 404.
     * </ul>
     */
    @TestTemplate
    void testRespondWithStaticFile() throws Exception {
        // /index.html is loaded from the classloader (web/index.html) and served
        Tuple2<Integer, String> index = HttpUtils.getFromHTTP(baseUrl + "/index.html");
        assertThat(index.f0).isEqualTo(200);
        assertThat(index.f1).contains("Apache Flink Web Dashboard");

        // a trailing slash is rewritten to "/index.html"
        Tuple2<Integer, String> index2 = HttpUtils.getFromHTTP(baseUrl + "/");
        assertThat(index2).isEqualTo(index);

        // a static file that is neither on disk nor in the classloader yields 404
        Tuple2<Integer, String> missing = HttpUtils.getFromHTTP(baseUrl + "/hello.html");
        assertThat(missing.f0).isEqualTo(404);
        assertThat(missing.f1).contains("not found");
    }

    /**
     * Tests file-system safety checks performed by {@code responseWithFile}:
     *
     * <ul>
     *   <li>a directory request returns 405;
     *   <li>a path that escapes the {@code webDir} returns 403.
     * </ul>
     */
    @TestTemplate
    void testRespondWithFile() throws Exception {
        // requesting a directory rather than a file yields 405
        Files.createDirectory(webDir.resolve("dir.json"));
        Tuple2<Integer, String> dirNotFound = HttpUtils.getFromHTTP(baseUrl + "/dir.json");
        assertThat(dirNotFound.f0).isEqualTo(405);
        assertThat(dirNotFound.f1).contains("not found");

        // requesting a file outside of the webDir is rejected with 403
        Files.createFile(tmpDir.resolve("secret"));
        Tuple2<Integer, String> outsideWebDir = HttpUtils.getFromHTTP(baseUrl + "/../secret");
        assertThat(outsideWebDir.f0).isEqualTo(403);
        assertThat(outsideWebDir.f1).contains("Forbidden");
    }

    /**
     * Tests requests served via the {@link ArchiveStorage} resource.
     *
     * <ul>
     *   <li>an existing entry is returned with its content;
     *   <li>a missing entry results in 404.
     * </ul>
     */
    @TestTemplate
    void testRespondWithResource() throws Exception {
        String resourcePath = "overviews/job1.json";
        String resourceContent = "{\"job\":\"job1\"}";
        handler.archiveStorage.putArchiveContent(resourcePath, resourceContent);

        // request without an extension: handler will append ".json" and serve the
        // entry from the ArchiveStorage via respondWithResource
        Tuple2<Integer, String> resource = HttpUtils.getFromHTTP(baseUrl + "/overviews/job1");
        assertThat(resource.f0).isEqualTo(200);
        assertThat(resource.f1).isEqualTo(resourceContent);

        // request a missing resource: archiveStorage returns null and the handler
        // responds 404
        Tuple2<Integer, String> missing = HttpUtils.getFromHTTP(baseUrl + "/hello");
        assertThat(missing.f0).isEqualTo(404);
        assertThat(missing.f1).contains("not found");
    }
}
