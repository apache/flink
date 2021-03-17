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

package org.apache.flink.runtime.webmonitor;

/**
 * *************************************************************************** This code is based on
 * the "HttpUploadServerHandler" from the Netty project's HTTP server example.
 *
 * <p>See http://netty.io and
 * https://github.com/netty/netty/blob/netty-4.0.31.Final/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java
 * ***************************************************************************
 */
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DiskFileUpload;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.EndOfDataDecoderException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.InterfaceHttpData;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Simple code which handles all HTTP requests from the user, and passes them to the Router handler
 * directly if they do not involve file upload requests. If a file is required to be uploaded, it
 * handles the upload, and in the http request to the next handler, passes the name of the file to
 * the next handler.
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);

    private static final Charset ENCODING = ConfigConstants.DEFAULT_CHARSET;

    /** A decoder factory that always stores POST chunks on disk. */
    private static final HttpDataFactory DATA_FACTORY = new DefaultHttpDataFactory(true);

    private final File tmpDir;

    private HttpRequest currentRequest;

    private HttpPostRequestDecoder currentDecoder;
    private String currentRequestPath;

    public HttpRequestHandler(File tmpDir) {
        this.tmpDir = tmpDir;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (currentDecoder != null) {
            currentDecoder.cleanFiles();
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        try {
            if (msg instanceof HttpRequest) {
                currentRequest = (HttpRequest) msg;
                currentRequestPath = null;

                if (currentDecoder != null) {
                    currentDecoder.destroy();
                    currentDecoder = null;
                }

                if (currentRequest.getMethod() == HttpMethod.GET
                        || currentRequest.getMethod() == HttpMethod.DELETE) {
                    // directly delegate to the router
                    ctx.fireChannelRead(currentRequest);
                } else if (currentRequest.getMethod() == HttpMethod.POST) {
                    // POST comes in multiple objects. First the request, then the contents
                    // keep the request and path for the remaining objects of the POST request
                    currentRequestPath =
                            new QueryStringDecoder(currentRequest.getUri(), ENCODING).path();
                    currentDecoder =
                            new HttpPostRequestDecoder(DATA_FACTORY, currentRequest, ENCODING);
                } else {
                    throw new IOException(
                            "Unsupported HTTP method: " + currentRequest.getMethod().name());
                }
            } else if (currentDecoder != null && msg instanceof HttpContent) {
                // received new chunk, give it to the current decoder
                HttpContent chunk = (HttpContent) msg;
                currentDecoder.offer(chunk);

                try {
                    while (currentDecoder.hasNext()) {
                        InterfaceHttpData data = currentDecoder.next();

                        // IF SOMETHING EVER NEEDS POST PARAMETERS, THIS WILL BE THE PLACE TO HANDLE
                        // IT
                        // all fields values will be passed with type Attribute.

                        if (data.getHttpDataType() == HttpDataType.FileUpload && tmpDir != null) {
                            DiskFileUpload file = (DiskFileUpload) data;
                            if (file.isCompleted()) {
                                String name = file.getFilename();

                                File target = new File(tmpDir, UUID.randomUUID() + "_" + name);
                                if (!tmpDir.exists()) {
                                    logExternalUploadDirDeletion(tmpDir);
                                    checkAndCreateUploadDir(tmpDir);
                                }
                                file.renameTo(target);

                                QueryStringEncoder encoder =
                                        new QueryStringEncoder(currentRequestPath);
                                encoder.addParam("filepath", target.getAbsolutePath());
                                encoder.addParam("filename", name);

                                currentRequest.setUri(encoder.toString());
                            }
                        }
                    }
                } catch (EndOfDataDecoderException ignored) {
                }

                if (chunk instanceof LastHttpContent) {
                    HttpRequest request = currentRequest;
                    currentRequest = null;
                    currentRequestPath = null;

                    currentDecoder.destroy();
                    currentDecoder = null;

                    // fire next channel handler
                    ctx.fireChannelRead(request);
                }
            }
        } catch (Throwable t) {
            currentRequest = null;
            currentRequestPath = null;

            if (currentDecoder != null) {
                currentDecoder.destroy();
                currentDecoder = null;
            }

            if (ctx.channel().isActive()) {
                byte[] bytes = ExceptionUtils.stringifyException(t).getBytes(ENCODING);

                DefaultFullHttpResponse response =
                        new DefaultFullHttpResponse(
                                HttpVersion.HTTP_1_1,
                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                Unpooled.wrappedBuffer(bytes));

                response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
                response.headers()
                        .set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

                ctx.writeAndFlush(response);
            }
        }
    }

    public static void logExternalUploadDirDeletion(File uploadDir) {
        LOG.warn(
                "Jar storage directory {} has been deleted externally. Previously uploaded jars are no longer available.",
                uploadDir.getAbsolutePath());
    }

    /**
     * Checks whether the given directory exists and is writable. If it doesn't exist this method
     * will attempt to create it.
     *
     * @param uploadDir directory to check
     * @throws IOException if the directory does not exist and cannot be created, or if the
     *     directory isn't writable
     */
    public static synchronized void checkAndCreateUploadDir(File uploadDir) throws IOException {
        if (uploadDir.exists() && uploadDir.canWrite()) {
            LOG.info("Using directory {} for web frontend JAR file uploads.", uploadDir);
        } else if (uploadDir.mkdirs() && uploadDir.canWrite()) {
            LOG.info("Created directory {} for web frontend JAR file uploads.", uploadDir);
        } else {
            LOG.warn(
                    "Jar upload directory {} cannot be created or is not writable.",
                    uploadDir.getAbsolutePath());
            throw new IOException(
                    String.format(
                            "Jar upload directory %s cannot be created or is not writable.",
                            uploadDir.getAbsolutePath()));
        }
    }
}
