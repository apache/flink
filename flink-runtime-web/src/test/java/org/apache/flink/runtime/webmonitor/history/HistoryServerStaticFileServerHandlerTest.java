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
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;

/** Tests for the HistoryServerStaticFileServerHandler. */
public class HistoryServerStaticFileServerHandlerTest extends TestLogger {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testRespondWithFile() throws Exception {
        File webDir = tmp.newFolder("webDir");
        Router router =
                new Router().addGet("/:*", new HistoryServerStaticFileServerHandler(webDir));
        WebFrontendBootstrap webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(HistoryServerStaticFileServerHandlerTest.class),
                        tmp.newFolder("uploadDir"),
                        null,
                        "localhost",
                        0,
                        new Configuration());

        int port = webUI.getServerPort();
        try {
            // verify that 404 message is returned when requesting a non-existent file
            Tuple2<Integer, String> notFound404 =
                    HistoryServerTest.getFromHTTP("http://localhost:" + port + "/hello");
            Assert.assertThat(notFound404.f0, is(404));
            Assert.assertThat(notFound404.f1, containsString("not found"));

            // verify that a) a file can be loaded using the ClassLoader and b) that the
            // HistoryServer
            // index_hs.html is injected
            Tuple2<Integer, String> index =
                    HistoryServerTest.getFromHTTP("http://localhost:" + port + "/index.html");
            Assert.assertThat(index.f0, is(200));
            Assert.assertThat(index.f1, containsString("Apache Flink Web Dashboard"));

            // verify that index.html is appended if the request path ends on '/'
            Tuple2<Integer, String> index2 =
                    HistoryServerTest.getFromHTTP("http://localhost:" + port + "/");
            Assert.assertEquals(index, index2);

            // verify that a 405 message is returned when requesting a directory
            File dir = new File(webDir, "dir.json");
            dir.mkdirs();
            Tuple2<Integer, String> dirNotFound =
                    HistoryServerTest.getFromHTTP("http://localhost:" + port + "/dir");
            Assert.assertThat(dirNotFound.f0, is(405));
            Assert.assertThat(dirNotFound.f1, containsString("not found"));

            // verify that a 403 message is returned when requesting a file outside the webDir
            tmp.newFile("secret");
            Tuple2<Integer, String> dirOutsideDirectory =
                    HistoryServerTest.getFromHTTP("http://localhost:" + port + "/../secret");
            Assert.assertThat(dirOutsideDirectory.f0, is(403));
            Assert.assertThat(dirOutsideDirectory.f1, containsString("Forbidden"));
        } finally {
            webUI.shutdown();
        }
    }
}
