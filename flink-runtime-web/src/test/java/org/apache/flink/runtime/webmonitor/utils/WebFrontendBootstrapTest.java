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

package org.apache.flink.runtime.webmonitor.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.netty.Prio0InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio1InboundChannelHandlerFactory;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.history.HistoryServerStaticFileServerHandler;
import org.apache.flink.runtime.webmonitor.history.HistoryServerTest;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the WebFrontendBootstrap. */
public class WebFrontendBootstrapTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testHandlersMustBeLoaded() throws Exception {
        File webDir = tmp.newFolder("webDir");
        Configuration configuration = new Configuration();
        configuration.setString(
                Prio0InboundChannelHandlerFactory.REDIRECT_FROM_URL, "/nonExisting");
        configuration.setString(Prio0InboundChannelHandlerFactory.REDIRECT_TO_URL, "/index.html");
        Router router =
                new Router().addGet("/:*", new HistoryServerStaticFileServerHandler(webDir));
        WebFrontendBootstrap webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(WebFrontendBootstrapTest.class),
                        tmp.newFolder("uploadDir"),
                        null,
                        "localhost",
                        0,
                        configuration);

        assertEquals(webUI.inboundChannelHandlerFactories.size(), 2);
        assertTrue(
                webUI.inboundChannelHandlerFactories.get(0)
                        instanceof Prio1InboundChannelHandlerFactory);
        assertTrue(
                webUI.inboundChannelHandlerFactories.get(1)
                        instanceof Prio0InboundChannelHandlerFactory);

        int port = webUI.getServerPort();
        try {
            Tuple2<Integer, String> index =
                    HistoryServerTest.getFromHTTP("http://localhost:" + port + "/index.html");
            Assert.assertEquals(index.f0.intValue(), 200);
            Assert.assertTrue(index.f1.contains("Apache Flink Web Dashboard"));

            Tuple2<Integer, String> index2 =
                    HistoryServerTest.getFromHTTP("http://localhost:" + port + "/nonExisting");
            Assert.assertEquals(index2.f0.intValue(), 200);
            Assert.assertEquals(index, index2);
        } finally {
            webUI.shutdown();
        }
    }
}
