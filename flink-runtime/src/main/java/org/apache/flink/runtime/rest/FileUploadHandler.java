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

package org.apache.flink.runtime.rest;

import org.apache.flink.runtime.rest.handler.FileUploads;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.Attribute;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DiskAttribute;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DiskFileUpload;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.InterfaceHttpData;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Writes multipart/form-data to disk. Delegates all other requests to the next {@link
 * ChannelInboundHandler} in the {@link ChannelPipeline}.
 */
public class FileUploadHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger LOG = LoggerFactory.getLogger(FileUploadHandler.class);

    public static final String HTTP_ATTRIBUTE_REQUEST = "request";

    private static final AttributeKey<FileUploads> UPLOADED_FILES =
            AttributeKey.valueOf("UPLOADED_FILES");

    private static final HttpDataFactory DATA_FACTORY = new DefaultHttpDataFactory(true);

    private final Path uploadDir;

    private HttpPostRequestDecoder currentHttpPostRequestDecoder;

    private HttpRequest currentHttpRequest;
    private byte[] currentJsonPayload;
    private Path currentUploadDir;

    private boolean addCRPrefix = false;

    public FileUploadHandler(final Path uploadDir) {
        super(true);

        // the clean up of temp files when jvm exits is handled by
        // org.apache.flink.util.ShutdownHookUtil; thus,
        // it's no need to register those files (post chunks and upload file chunks) to
        // java.io.DeleteOnExitHook
        // which may lead to memory leak.
        DiskAttribute.deleteOnExitTemporaryFile = false;
        DiskFileUpload.deleteOnExitTemporaryFile = false;

        DiskFileUpload.baseDirectory = uploadDir.normalize().toAbsolutePath().toString();
        // share the same directory with file upload for post chunks storage.
        DiskAttribute.baseDirectory = DiskFileUpload.baseDirectory;

        this.uploadDir = requireNonNull(uploadDir);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg)
            throws Exception {
        try {
            if (msg instanceof HttpRequest) {
                final HttpRequest httpRequest = (HttpRequest) msg;
                LOG.trace(
                        "Received request. URL:{} Method:{}",
                        httpRequest.uri(),
                        httpRequest.method());
                if (httpRequest.method().equals(HttpMethod.POST)) {
                    if (HttpPostRequestDecoder.isMultipart(httpRequest)) {
                        LOG.trace("Initializing multipart file upload.");
                        checkState(currentHttpPostRequestDecoder == null);
                        checkState(currentHttpRequest == null);
                        checkState(currentUploadDir == null);
                        currentHttpPostRequestDecoder =
                                new HttpPostRequestDecoder(DATA_FACTORY, httpRequest);
                        currentHttpRequest = ReferenceCountUtil.retain(httpRequest);

                        // make sure that we still have a upload dir in case that it got deleted in
                        // the meanwhile
                        RestServerEndpoint.createUploadDir(uploadDir, LOG, false);

                        currentUploadDir =
                                Files.createDirectory(
                                        uploadDir.resolve(UUID.randomUUID().toString()));
                    } else {
                        ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
                    }
                } else {
                    ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
                }
            } else if (msg instanceof HttpContent && currentHttpPostRequestDecoder != null) {
                LOG.trace("Received http content.");
                // make sure that we still have a upload dir in case that it got deleted in the
                // meanwhile
                RestServerEndpoint.createUploadDir(uploadDir, LOG, false);

                final HttpContent httpContent = (HttpContent) msg;
                currentHttpPostRequestDecoder.offer(httpContent);

                while (httpContent != LastHttpContent.EMPTY_LAST_CONTENT
                        && hasNext(currentHttpPostRequestDecoder)) {
                    final InterfaceHttpData data = currentHttpPostRequestDecoder.next();
                    if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
                        final DiskFileUpload fileUpload = (DiskFileUpload) data;
                        checkState(fileUpload.isCompleted());

                        // wrapping around another File instantiation is a simple way to remove any
                        // path information - we're
                        // solely interested in the filename
                        final Path dest =
                                currentUploadDir.resolve(
                                        new File(fileUpload.getFilename()).getName());
                        fileUpload.renameTo(dest.toFile());
                        LOG.trace(
                                "Upload of file {} into destination {} complete.",
                                fileUpload.getFilename(),
                                dest.toString());
                    } else if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.Attribute) {
                        final Attribute request = (Attribute) data;
                        // this could also be implemented by using the first found Attribute as the
                        // payload
                        LOG.trace("Upload of attribute {} complete.", request.getName());
                        if (data.getName().equals(HTTP_ATTRIBUTE_REQUEST)) {
                            currentJsonPayload = request.get();
                        } else {
                            handleError(
                                    ctx,
                                    "Received unknown attribute " + data.getName() + '.',
                                    HttpResponseStatus.BAD_REQUEST,
                                    null);
                            return;
                        }
                    }
                }

                if (httpContent instanceof LastHttpContent) {
                    LOG.trace("Finalizing multipart file upload.");
                    ctx.channel().attr(UPLOADED_FILES).set(new FileUploads(currentUploadDir));
                    if (currentJsonPayload != null) {
                        currentHttpRequest
                                .headers()
                                .set(HttpHeaderNames.CONTENT_LENGTH, currentJsonPayload.length);
                        currentHttpRequest
                                .headers()
                                .set(HttpHeaderNames.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);
                        ctx.fireChannelRead(currentHttpRequest);
                        ctx.fireChannelRead(
                                httpContent.replace(Unpooled.wrappedBuffer(currentJsonPayload)));
                    } else {
                        currentHttpRequest.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
                        currentHttpRequest.headers().remove(HttpHeaderNames.CONTENT_TYPE);
                        ctx.fireChannelRead(currentHttpRequest);
                        ctx.fireChannelRead(LastHttpContent.EMPTY_LAST_CONTENT);
                    }
                    reset();
                }
            } else {
                ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
            }
        } catch (Exception e) {
            handleError(ctx, "File upload failed.", HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private static boolean hasNext(HttpPostRequestDecoder decoder) {
        try {
            return decoder.hasNext();
        } catch (HttpPostRequestDecoder.EndOfDataDecoderException e) {
            // this can occur if the final chuck wasn't empty, but didn't contain any attribute data
            // unfortunately the Netty APIs don't give us any way to check this
            return false;
        }
    }

    private void handleError(
            ChannelHandlerContext ctx,
            String errorMessage,
            HttpResponseStatus responseStatus,
            @Nullable Throwable e) {
        HttpRequest tmpRequest = currentHttpRequest;
        deleteUploadedFiles();
        reset();
        LOG.warn(errorMessage, e);
        HandlerUtils.sendErrorResponse(
                ctx,
                tmpRequest,
                new ErrorResponseBody(errorMessage),
                responseStatus,
                Collections.emptyMap());
        ReferenceCountUtil.release(tmpRequest);
    }

    private void deleteUploadedFiles() {
        if (currentUploadDir != null) {
            try {
                FileUtils.deleteDirectory(currentUploadDir.toFile());
            } catch (IOException e) {
                LOG.warn("Could not cleanup uploaded files.", e);
            }
        }
    }

    private void reset() {
        // destroy() can fail because some data is stored multiple times in the decoder causing an
        // IllegalReferenceCountException
        // see https://github.com/netty/netty/issues/7814
        try {
            currentHttpPostRequestDecoder.getBodyHttpDatas().clear();
        } catch (HttpPostRequestDecoder.NotEnoughDataDecoderException ned) {
            // this method always fails if not all chunks were offered to the decoder yet
            LOG.debug("Error while resetting handler.", ned);
        }
        currentHttpPostRequestDecoder.destroy();
        currentHttpPostRequestDecoder = null;
        currentHttpRequest = null;
        currentUploadDir = null;
        currentJsonPayload = null;
    }

    public static FileUploads getMultipartFileUploads(ChannelHandlerContext ctx) {
        return Optional.ofNullable(ctx.channel().attr(UPLOADED_FILES).getAndSet(null))
                .orElse(FileUploads.EMPTY);
    }
}
