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
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link JarDeleteHandler}. */
class JarDeleteHandlerTest {

    private static final String TEST_JAR_NAME = "test.jar";

    private JarDeleteHandler jarDeleteHandler;

    private RestfulGateway restfulGateway;

    private Path jarDir;

    @BeforeEach
    private void setUp(@TempDir File tempDir) throws Exception {
        jarDir = tempDir.toPath();
        restfulGateway = new TestingRestfulGateway.Builder().build();
        jarDeleteHandler =
                new JarDeleteHandler(
                        () -> CompletableFuture.completedFuture(restfulGateway),
                        Time.seconds(10),
                        Collections.emptyMap(),
                        new JarDeleteHeaders(),
                        jarDir,
                        Executors.directExecutor());

        Files.createFile(jarDir.resolve(TEST_JAR_NAME));
    }

    @Test
    void testDeleteJarById() throws Exception {
        assertThat(Files.exists(jarDir.resolve(TEST_JAR_NAME))).isTrue();

        final HandlerRequest<EmptyRequestBody> request = createRequest(TEST_JAR_NAME);
        jarDeleteHandler.handleRequest(request, restfulGateway).get();

        assertThat(Files.exists(jarDir.resolve(TEST_JAR_NAME))).isFalse();
    }

    @Test
    void testDeleteUnknownJar() throws Exception {
        final HandlerRequest<EmptyRequestBody> request = createRequest("doesnotexist.jar");
        assertThatThrownBy(() -> jarDeleteHandler.handleRequest(request, restfulGateway).get())
                .satisfies(
                        e -> {
                            final Throwable throwable =
                                    ExceptionUtils.stripCompletionException(e.getCause());
                            assertThat(throwable).isInstanceOf(RestHandlerException.class);

                            final RestHandlerException restHandlerException =
                                    (RestHandlerException) throwable;
                            assertThat(restHandlerException.getMessage())
                                    .contains("File doesnotexist.jar does not exist in");
                            assertThat(restHandlerException.getHttpResponseStatus())
                                    .isEqualTo(HttpResponseStatus.BAD_REQUEST);
                        });
    }

    @Test
    void testFailedDelete() throws Exception {
        makeJarDirReadOnly();

        final HandlerRequest<EmptyRequestBody> request = createRequest(TEST_JAR_NAME);
        assertThatThrownBy(() -> jarDeleteHandler.handleRequest(request, restfulGateway).get())
                .satisfies(
                        e -> {
                            final Throwable throwable =
                                    ExceptionUtils.stripCompletionException(e.getCause());
                            assertThat(throwable).isInstanceOf(RestHandlerException.class);

                            final RestHandlerException restHandlerException =
                                    (RestHandlerException) throwable;
                            assertThat(restHandlerException.getMessage())
                                    .contains("Failed to delete jar");
                            assertThat(restHandlerException.getHttpResponseStatus())
                                    .isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        });
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(final String jarFileName)
            throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JarDeleteMessageParameters(),
                Collections.singletonMap(JarIdPathParameter.KEY, jarFileName),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private void makeJarDirReadOnly() {
        try {
            Files.setPosixFilePermissions(
                    jarDir,
                    new HashSet<>(
                            Arrays.asList(
                                    PosixFilePermission.OTHERS_READ,
                                    PosixFilePermission.GROUP_READ,
                                    PosixFilePermission.OWNER_READ,
                                    PosixFilePermission.OTHERS_EXECUTE,
                                    PosixFilePermission.GROUP_EXECUTE,
                                    PosixFilePermission.OWNER_EXECUTE)));
        } catch (final Exception e) {
            Assumptions.assumeTrue(e == null);
        }
    }
}
