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

/**
 * *************************************************************************** This code is based on
 * the "HttpStaticFileServerHandler" from the Netty project's HTTP server example.
 *
 * <p>See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 * ***************************************************************************
 */
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;

import java.io.File;
import java.io.IOException;

/**
 * Simple file server handler used by the {@link HistoryServer} that serves requests to web
 * frontend's static files, such as HTML, CSS, JS or JSON files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.
 *
 * <p>This class is a copy of the {@link StaticFileServerHandler}. The differences are that the
 * request path is modified to end on ".json" if it does not have a filename extension; when
 * "index.html" is requested we load "index_hs.html" instead to inject the modified HistoryServer
 * WebInterface and that the caching of the "/joboverview" page is prevented.
 */
@ChannelHandler.Sharable
public class HistoryServerStaticFileServerHandler extends AbstractHistoryServerHandler<File> {

    // ------------------------------------------------------------------------

    public HistoryServerStaticFileServerHandler(File rootPath) throws IOException {
        this(new FileArchiveStorage(rootPath), rootPath);
    }

    public HistoryServerStaticFileServerHandler(
            FileArchiveStorage fileArchiveStorage, File rootPath) throws IOException {
        super(fileArchiveStorage, rootPath);
    }

    // ------------------------------------------------------------------------
    //  Responses to requests
    // ------------------------------------------------------------------------

    @Override
    protected void respondWithResource(
            ChannelHandlerContext ctx, HttpRequest request, String requestPath, File file)
            throws Exception {
        responseWithFile(ctx, request, requestPath, file);
    }
}
