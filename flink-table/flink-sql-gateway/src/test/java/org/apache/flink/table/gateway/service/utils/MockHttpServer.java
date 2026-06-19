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

package org.apache.flink.table.gateway.service.utils;

import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;

import static org.apache.flink.shaded.guava33.com.google.common.io.Files.copy;

/** Mock a http server to process http request. */
public class MockHttpServer implements Closeable {

    private static final String HTTP_PROTOCOL = "http";

    private final HttpServer server;
    private final NetUtils.Port port;

    private MockHttpServer(HttpServer server, NetUtils.Port port) {
        this.server = server;
        this.port = port;
    }

    public static MockHttpServer startHttpServer() throws IOException {
        NetUtils.Port port = NetUtils.getAvailablePort();
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port.getPort()), 0);
        httpServer.setExecutor(null);
        httpServer.start();
        return new MockHttpServer(httpServer, port);
    }

    public URL prepareResource(String path, File fileToDownload) throws Exception {
        server.createContext(path, new DownloadFileHttpHandler(fileToDownload));
        return new URL(
                HTTP_PROTOCOL, InetAddress.getLocalHost().getHostAddress(), port.getPort(), path);
    }

    @Override
    public void close() throws IOException {
        server.stop(0);
        try {
            port.close();
        } catch (Exception e) {
            // ignore
        }
    }

    /** Handler to mock download file. */
    private static class DownloadFileHttpHandler implements HttpHandler {

        private static final String CONTENT_TYPE_KEY = "Content-Type";

        private static final String CONTENT_TYPE_VALUE = "application/octet-stream";

        private final File file;

        public DownloadFileHttpHandler(File fileToDownload) {
            Preconditions.checkArgument(
                    fileToDownload.exists(), "The file to be download not exists!");
            this.file = fileToDownload;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add(CONTENT_TYPE_KEY, CONTENT_TYPE_VALUE);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, file.length());
            copy(this.file, exchange.getResponseBody());
            exchange.close();
        }
    }
}
