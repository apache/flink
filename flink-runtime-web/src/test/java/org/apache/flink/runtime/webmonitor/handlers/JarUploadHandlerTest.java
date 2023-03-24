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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JarUploadHandler}. */
class JarUploadHandlerTest {

    private JarUploadHandler jarUploadHandler;

    private final DispatcherGateway mockDispatcherGateway =
            TestingDispatcherGateway.newBuilder().build();

    private Path jarDir;

    @BeforeEach
    void setUp(@TempDir File temporaryFolder) {
        jarDir = temporaryFolder.toPath();
        jarUploadHandler =
                new JarUploadHandler(
                        () -> CompletableFuture.completedFuture(mockDispatcherGateway),
                        Time.seconds(10),
                        Collections.emptyMap(),
                        JarUploadHeaders.getInstance(),
                        jarDir,
                        Executors.directExecutor());
    }

    @Test
    void testRejectNonJarFiles() throws Exception {
        final Path uploadedFile = Files.createFile(jarDir.resolve("katrin.png"));
        final HandlerRequest<EmptyRequestBody> request = createRequest(uploadedFile);

        assertThatThrownBy(
                        () -> jarUploadHandler.handleRequest(request, mockDispatcherGateway).get())
                .satisfies(
                        e -> {
                            final Throwable throwable =
                                    ExceptionUtils.stripCompletionException(e.getCause());
                            assertThat(throwable).isInstanceOf(RestHandlerException.class);
                            final RestHandlerException restHandlerException =
                                    (RestHandlerException) throwable;
                            assertThat(restHandlerException.getHttpResponseStatus())
                                    .isEqualTo(HttpResponseStatus.BAD_REQUEST);
                        });
    }

    @Test
    void testUploadJar() throws Exception {
        final Path uploadedFile = Files.createFile(jarDir.resolve("FooBazzleExample.jar"));
        final HandlerRequest<EmptyRequestBody> request = createRequest(uploadedFile);

        final JarUploadResponseBody jarUploadResponseBody =
                jarUploadHandler.handleRequest(request, mockDispatcherGateway).get();
        assertThat(jarUploadResponseBody.getStatus())
                .isEqualTo(JarUploadResponseBody.UploadStatus.success);
        final String returnedFileNameWithUUID = jarUploadResponseBody.getFilename();
        assertThat(returnedFileNameWithUUID).contains("_");
        final String returnedFileName =
                returnedFileNameWithUUID.substring(returnedFileNameWithUUID.lastIndexOf("_") + 1);
        assertThat(returnedFileName).isEqualTo(uploadedFile.getFileName().toString());
    }

    @Test
    void testFailedUpload() {
        final Path uploadedFile = jarDir.resolve("FooBazzleExample.jar");
        final HandlerRequest<EmptyRequestBody> request = createRequest(uploadedFile);

        assertThatThrownBy(
                        () -> jarUploadHandler.handleRequest(request, mockDispatcherGateway).get())
                .satisfies(
                        e -> {
                            final Throwable throwable =
                                    ExceptionUtils.stripCompletionException(e.getCause());
                            assertThat(throwable).isInstanceOf(RestHandlerException.class);
                            final RestHandlerException restHandlerException =
                                    (RestHandlerException) throwable;
                            assertThat(restHandlerException.getMessage())
                                    .contains("Could not move uploaded jar file");
                            assertThat(restHandlerException.getHttpResponseStatus())
                                    .isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        });
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(final Path uploadedFile) {
        return HandlerRequest.create(
                EmptyRequestBody.getInstance(),
                EmptyMessageParameters.getInstance(),
                Collections.singleton(uploadedFile.toFile()));
    }
}
