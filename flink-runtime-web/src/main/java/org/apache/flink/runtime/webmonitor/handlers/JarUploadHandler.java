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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/** Handles .jar file uploads. */
public class JarUploadHandler
        extends AbstractRestHandler<
                RestfulGateway, EmptyRequestBody, JarUploadResponseBody, EmptyMessageParameters> {

    private final Path jarDir;

    private final Executor executor;

    public JarUploadHandler(
            final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            final Time timeout,
            final Map<String, String> responseHeaders,
            final MessageHeaders<EmptyRequestBody, JarUploadResponseBody, EmptyMessageParameters>
                    messageHeaders,
            final Path jarDir,
            final Executor executor) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.jarDir = requireNonNull(jarDir);
        this.executor = requireNonNull(executor);
    }

    @Override
    @VisibleForTesting
    public CompletableFuture<JarUploadResponseBody> handleRequest(
            @Nonnull final HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request,
            @Nonnull final RestfulGateway gateway)
            throws RestHandlerException {
        Collection<File> uploadedFiles = request.getUploadedFiles();
        if (uploadedFiles.size() != 1) {
            throw new RestHandlerException(
                    "Exactly 1 file must be sent, received " + uploadedFiles.size() + '.',
                    HttpResponseStatus.BAD_REQUEST);
        }
        final Path fileUpload = uploadedFiles.iterator().next().toPath();
        return CompletableFuture.supplyAsync(
                () -> {
                    if (!fileUpload.getFileName().toString().endsWith(".jar")) {
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Only Jar files are allowed.",
                                        HttpResponseStatus.BAD_REQUEST));
                    } else {
                        final Path destination =
                                jarDir.resolve(UUID.randomUUID() + "_" + fileUpload.getFileName());
                        try {
                            Files.move(fileUpload, destination);
                        } catch (IOException e) {
                            throw new CompletionException(
                                    new RestHandlerException(
                                            String.format(
                                                    "Could not move uploaded jar file [%s] to [%s].",
                                                    fileUpload, destination),
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                            e));
                        }
                        return new JarUploadResponseBody(destination.normalize().toString());
                    }
                },
                executor);
    }
}
