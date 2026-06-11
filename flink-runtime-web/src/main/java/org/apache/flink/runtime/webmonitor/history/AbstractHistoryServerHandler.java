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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.IF_MODIFIED_SINCE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Abstract base class for HistoryServer handlers. */
@ChannelHandler.Sharable
public abstract class AbstractHistoryServerHandler<Entry>
        extends SimpleChannelInboundHandler<RoutedRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHistoryServerHandler.class);

    /** The path in which the static documents are. */
    protected final File rootPath;

    protected final ArchiveStorage<Entry> archiveStorage;

    protected AbstractHistoryServerHandler(ArchiveStorage<Entry> archiveStorage, File rootPath)
            throws IOException {
        this.archiveStorage = archiveStorage;
        this.rootPath = checkNotNull(rootPath).getCanonicalFile();
    }

    // -------------------------------------------------------------------------
    //  Netty lifecycle
    // -------------------------------------------------------------------------

    @Override
    public final void channelRead0(ChannelHandlerContext ctx, RoutedRequest routedRequest)
            throws Exception {
        try {
            respondToRequest(ctx, routedRequest);
        } catch (RestHandlerException rhe) {
            HandlerUtils.sendErrorResponse(
                    ctx,
                    routedRequest.getRequest(),
                    new ErrorResponseBody(rhe.getMessage()),
                    rhe.getHttpResponseStatus(),
                    Collections.emptyMap());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (ctx.channel().isActive()) {
            LOG.error("Caught exception", cause);
            HandlerUtils.sendErrorResponse(
                    ctx,
                    false,
                    new ErrorResponseBody("Internal server error."),
                    INTERNAL_SERVER_ERROR,
                    Collections.emptyMap());
        }
    }

    // -------------------------------------------------------------------------
    //  Request handling
    // -------------------------------------------------------------------------

    /**
     * Handles HTTP requests by routing to JSON resource or static file response.
     *
     * @param ctx Netty Channel context
     * @param routedRequest The routed HTTP request
     * @throws Exception Any exception that occurs during processing
     */
    protected void respondToRequest(ChannelHandlerContext ctx, RoutedRequest routedRequest)
            throws Exception {
        String requestPath = routedRequest.getPath();

        // make sure we request the "index.html" in case there is a directory request
        if (requestPath.endsWith("/")) {
            requestPath = requestPath + "index.html";
        }

        if (!requestPath.contains(".")) { // we assume that the path ends in either .html or .js
            requestPath = requestPath + ".json";

            LOG.debug("Responding to request for path {}", requestPath);
            Entry resource = loadResource(requestPath);

            if (resource == null) {
                LOG.debug("Unable to load requested resource {}", requestPath);
                throw new NotFoundException("Resource not found.");
            }

            respondWithResource(ctx, routedRequest.getRequest(), requestPath, resource);
        } else {
            File destFile = new File(rootPath, requestPath);
            if (!destFile.exists()) {
                tryLoadFromClassloader(destFile, requestPath);
            }
            responseWithFile(ctx, routedRequest.getRequest(), requestPath, destFile);
        }
    }

    // -------------------------------------------------------------------------
    //  Response helpers
    // -------------------------------------------------------------------------

    /** Respond to the request with the resource. */
    protected abstract void respondWithResource(
            ChannelHandlerContext ctx, HttpRequest request, String requestPath, Entry resource)
            throws Exception;

    /** Respond to the request with the file. */
    protected void responseWithFile(
            ChannelHandlerContext ctx, HttpRequest request, String requestPath, File file)
            throws Exception {
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
            if (!requestPath.equals("/joboverview.json")) {
                StaticFileServerHandler.setDateAndCacheHeaders(response, file);
            }
            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            HttpUtil.setContentLength(response, fileLength);

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
            if (!HttpUtil.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Exception e) {
            raf.close();
            LOG.error("Failed to serve file.", e);
            throw new RestHandlerException("Internal server error.", INTERNAL_SERVER_ERROR);
        }
    }

    // -------------------------------------------------------------------------
    //  Resource loading
    // -------------------------------------------------------------------------

    /**
     * Loads the resource for the given request path from the archive storage.
     *
     * @param requestPath The request path
     * @return The resource for the given request path, or null if not found
     */
    private Entry loadResource(String requestPath) throws Exception {
        String requestKey = requestPath.startsWith("/") ? requestPath.substring(1) : requestPath;
        if (archiveStorage.exists(requestKey)) {
            return archiveStorage.getEntry(requestKey);
        }
        return null;
    }

    /**
     * Attempts to load a missing resource from the classloader's {@code web/} resource directory
     * and copies it to {@code destFile} on disk.
     *
     * @param destFile destination file
     * @param requestPath relative resource path (e.g. {@code /index.html})
     */
    protected void tryLoadFromClassloader(File destFile, String requestPath) throws Exception {
        ClassLoader cl = AbstractHistoryServerHandler.class.getClassLoader();

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
                            LOG.debug("Loading missing resource from classloader: {}", requestPath);
                            // ensure that directory to file exists.
                            destFile.getParentFile().mkdirs();
                            Files.copy(resourceStream, destFile.toPath());

                            success = true;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("error while responding", t);
            }

            if (!success) {
                LOG.debug("Unable to load requested resource {} from classloader", requestPath);
                throw new NotFoundException("Resource not found.");
            }
        }
    }
}
