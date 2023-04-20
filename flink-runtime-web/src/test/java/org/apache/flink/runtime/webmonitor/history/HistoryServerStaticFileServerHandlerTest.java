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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the HistoryServerStaticFileServerHandler. */
class HistoryServerStaticFileServerHandlerTest {

    @Test
    void testRespondWithFile(@TempDir Path tmpDir) throws Exception {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path uploadDir = Files.createDirectory(tmpDir.resolve("uploadDir"));

        Router router =
                new Router()
                        .addGet("/:*", new HistoryServerStaticFileServerHandler(webDir.toFile()));
        WebFrontendBootstrap webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(HistoryServerStaticFileServerHandlerTest.class),
                        uploadDir.toFile(),
                        null,
                        "localhost",
                        0,
                        new Configuration());

        int port = webUI.getServerPort();
        try {
            // verify that 404 message is returned when requesting a non-existent file
            Tuple2<Integer, String> notFound404 =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/hello");
            assertThat(notFound404.f0).isEqualTo(404);
            assertThat(notFound404.f1).contains("not found");

            // verify that a) a file can be loaded using the ClassLoader and b) that the
            // HistoryServer
            // index_hs.html is injected
            Tuple2<Integer, String> index =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/index.html");
            assertThat(index.f0).isEqualTo(200);
            assertThat(index.f1).contains("Apache Flink Web Dashboard");

            // verify that index.html is appended if the request path ends on '/'
            Tuple2<Integer, String> index2 =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/");
            assertThat(index2).isEqualTo(index);

            // verify that a 405 message is returned when requesting a directory
            Files.createDirectory(webDir.resolve("dir.json"));
            Tuple2<Integer, String> dirNotFound =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/dir");
            assertThat(dirNotFound.f0).isEqualTo(405);
            assertThat(dirNotFound.f1).contains("not found");

            // verify that a 403 message is returned when requesting a file outside the webDir
            Files.createFile(tmpDir.resolve("secret"));
            Tuple2<Integer, String> dirOutsideDirectory =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/../secret");
            assertThat(dirOutsideDirectory.f0).isEqualTo(403);
            assertThat(dirOutsideDirectory.f1).contains("Forbidden");
        } finally {
            webUI.shutdown();
        }
    }
}
