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
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.webmonitor.history.kvstore.KVStore;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple file server handler used by the {@link HistoryServer} that serves requests for web
 * frontend's static files, such as HTML, CSS, JS, or JSON files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.
 *
 * <p>This class is a copy of the {@link StaticFileServerHandler}. The differences are:
 *
 * <ul>
 *   <li>The request path is used as a key for RocksDB and generates a ".json" file if it does not
 *       have a filename extension.
 *   <li>When "index.html" is requested, "index_hs.html" is loaded instead to inject the modified
 *       HistoryServer WebInterface.
 *   <li>Caching of the "/joboverview" page is prevented to ensure the latest job overview data is
 *       served.
 * </ul>
 */
@ChannelHandler.Sharable
public class HistoryServerKVStoreServerHandler extends HistoryServerServerHandler {

    private final KVStore<String, String> kvStore;
    private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    public HistoryServerKVStoreServerHandler(
            File rootPath, HistoryServerKVStoreArchiveFetcher archiveFetcher) throws IOException {
        super(rootPath);
        this.kvStore = checkNotNull(archiveFetcher.getKVStore());
    }

    /** Response when running with leading JobManager. */
    @Override
    protected void respondWithFile(
            ChannelHandlerContext ctx, HttpRequest request, String requestPath) throws Exception {

        // make sure we request the "index.html" in case there is a directory request
        if (requestPath.endsWith("/")) {
            requestPath = requestPath + "index.html";
        }

        if (!requestPath.contains(".")) { // we assume that the path ends in either .html or .js
            requestPath = requestPath + ".json";
        }

        // convert to absolute path
        final File file = new File(rootPath, requestPath);

        // RocksDB's key, initialize as null
        String key = null;

        // if the requestPath is for a json file
        if (requestPath.endsWith(".json") && !file.exists()) {
            // Remove the ".json" extension and use the rest of the request path as the key
            key = requestPath.substring(0, requestPath.length() - 5); // 5 is the length of ".json"

            // Special case for the combined overview page
            if (key.equals("/jobs/overview")) {
                key = "/jobs/combined-overview";
            }
            sendJsonResponse(ctx, request, key);
        }

        if (!file.exists()) {
            // file does not exist. Try to load it with the classloader
            ClassLoader cl = HistoryServerKVStoreServerHandler.class.getClassLoader();

            try (InputStream resourceStream = cl.getResourceAsStream("web" + requestPath)) {
                boolean success = false;
                try {
                    if (resourceStream != null) {
                        URL root = cl.getResource("web");
                        URL requested = cl.getResource("web" + requestPath);

                        if (root != null && requested != null) {
                            URI rootURI = new URI(root.getPath()).normalize();
                            URI requestedURI = new URI(requested.getPath()).normalize();

                            // Check that we don't load anything from outside of the
                            // expected scope.
                            if (!rootURI.relativize(requestedURI).equals(requestedURI)) {
                                LOG.debug("Loading missing file from classloader: {}", requestPath);
                                // ensure that directory to file exists.
                                file.getParentFile().mkdirs();
                                Files.copy(resourceStream, file.toPath());

                                success = true;
                            }
                        }
                    }
                } catch (Throwable t) {
                    LOG.error("error while responding", t);
                } finally {
                    if (!success) {
                        LOG.debug("Unable to load requested file {} from classloader", requestPath);
                        throw new NotFoundException("File not found.");
                    }
                }
            }
        }

        StaticFileServerHandler.checkFileValidity(file, rootPath, LOG);

        // cache validation
        final String ifModifiedSince = request.headers().get(IF_MODIFIED_SINCE);
        if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
            SimpleDateFormat dateFormatter =
                    new SimpleDateFormat(StaticFileServerHandler.HTTP_DATE_FORMAT, Locale.US);
            Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
            long fileLastModifiedSeconds = file.lastModified() / 1000;
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Responding 'NOT MODIFIED' for file '" + file.getAbsolutePath() + '\'');
                }

                StaticFileServerHandler.sendNotModified(ctx);
                return;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Responding with file '" + file.getAbsolutePath() + '\'');
        }

        // Don't need to close this manually. Netty's DefaultFileRegion will take care of it.
        final RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Could not find file {}.", file.getAbsolutePath());
            }
            HandlerUtils.sendErrorResponse(
                    ctx,
                    request,
                    new ErrorResponseBody("File not found."),
                    NOT_FOUND,
                    Collections.emptyMap());
            return;
        }

        try {
            long fileLength = raf.length();

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            StaticFileServerHandler.setContentTypeHeader(response, file);

            // the job overview should be updated as soon as possible
            if (!requestPath.equals("/joboverview")) {
                StaticFileServerHandler.setDateAndCacheHeaders(response, file);
            }
            if (HttpHeaders.isKeepAlive(request)) {
                response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            }
            HttpHeaders.setContentLength(response, fileLength);

            // write the initial line and the header.
            ctx.write(response);

            // write the content.
            ChannelFuture lastContentFuture;
            if (ctx.pipeline().get(SslHandler.class) == null) {
                ctx.write(
                        new DefaultFileRegion(raf.getChannel(), 0, fileLength),
                        ctx.newProgressivePromise());
                lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            } else {
                lastContentFuture =
                        ctx.writeAndFlush(
                                new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)),
                                ctx.newProgressivePromise());
                // HttpChunkedInput will write the end marker (LastHttpContent) for us.
            }

            // close the connection, if no keep-alive is needed
            if (!HttpHeaders.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Exception e) {
            raf.close();
            LOG.error("Failed to serve file.", e);
            throw new RestHandlerException("Internal server error.", INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Sends a JSON response to the client based on the specified key from the key-value store.
     *
     * <p>This method attempts to retrieve a JSON string from a key-value store using the provided
     * key. If the JSON string is found, it sends it back to the client as part of an HTTP response
     * with a 200 OK status. The content type of the response is set to "application/json;
     * charset=UTF-8" to inform the client that the server is returning JSON formatted text in UTF-8
     * encoding. The response is sent using Netty's asynchronous network IO capabilities to
     * efficiently handle the HTTP traffic.
     *
     * <p>If the connection should be kept alive according to the incoming HTTP request, it adjusts
     * the HTTP headers accordingly. Otherwise, it attaches a listener to close the connection after
     * the response is sent.
     *
     * <p>If no value is found for the provided key, the method logs an informational message but
     * does not send an error response to the client.
     *
     * @param ctx The ChannelHandlerContext through which to send the response, providing control
     *     over the underlying network connection and data flow.
     * @param request The HttpRequest object representing the client's request, used to determine if
     *     the connection should be kept alive after sending the response.
     * @param key The key used to retrieve the JSON string from the RocksDB key-value store.
     * @throws Exception Throws an exception if there are issues in retrieving the data from the
     *     key-value store or in sending the response through the network.
     */
    private void sendJsonResponse(ChannelHandlerContext ctx, HttpRequest request, String key)
            throws Exception {
        if (key == null) {
            LOG.info("Can not fetch JSON response for null key IN RocksDB");
        }
        LOG.info("Fetching JSON response for key {} in RocksDB", key);
        String jsonValue = kvStore.get(key);

        if (jsonValue != null) {
            LOG.info("JSON response fetched, sending JSON response for key {}", key);
            FullHttpResponse response =
                    new DefaultFullHttpResponse(
                            HTTP_1_1, OK, Unpooled.copiedBuffer(jsonValue, CharsetUtil.UTF_8));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
            if (HttpHeaders.isKeepAlive(request)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } else {
            LOG.info("No value found for key {} in RocksDB", key);
        }
    }
}
