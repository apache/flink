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
import org.apache.flink.runtime.io.network.netty.InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio0InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio1InboundChannelHandlerFactory;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.history.HistoryServerStaticFileServerHandler;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the WebFrontendBootstrap. */
class WebFrontendBootstrapTest {

    @RegisterExtension
    static final Extension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            InboundChannelHandlerFactory.class,
                            Prio0InboundChannelHandlerFactory.class.getCanonicalName(),
                            Prio1InboundChannelHandlerFactory.class.getCanonicalName())
                    .build();

    @TempDir Path tmp;

    @Test
    void testHandlersMustBeLoaded() throws Exception {
        Path webDir = Files.createDirectories(tmp.resolve("webDir"));
        Configuration configuration = new Configuration();
        configuration.setString(
                Prio0InboundChannelHandlerFactory.REDIRECT_FROM_URL, "/nonExisting");
        configuration.setString(Prio0InboundChannelHandlerFactory.REDIRECT_TO_URL, "/index.html");
        Router<?> router =
                new Router<>()
                        .addGet("/:*", new HistoryServerStaticFileServerHandler(webDir.toFile()));
        WebFrontendBootstrap webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(WebFrontendBootstrapTest.class),
                        Files.createDirectories(webDir.resolve("uploadDir")).toFile(),
                        null,
                        "localhost",
                        0,
                        configuration);

        assertThat(webUI.inboundChannelHandlerFactories).hasSize(2);
        assertTrue(
                webUI.inboundChannelHandlerFactories.get(0)
                        instanceof Prio1InboundChannelHandlerFactory);
        assertTrue(
                webUI.inboundChannelHandlerFactories.get(1)
                        instanceof Prio0InboundChannelHandlerFactory);

        int port = webUI.getServerPort();
        try {
            Tuple2<Integer, String> index =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/index.html");
            assertThat(200).isEqualTo(index.f0.intValue());
            assertThat(index.f1.contains("Apache Flink Web Dashboard")).isTrue();

            Tuple2<Integer, String> index2 =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/nonExisting");
            assertThat(200).isEqualTo(index2.f0.intValue());
            assertThat(index2).isEqualTo(index);
        } finally {
            webUI.shutdown();
        }
    }
}
