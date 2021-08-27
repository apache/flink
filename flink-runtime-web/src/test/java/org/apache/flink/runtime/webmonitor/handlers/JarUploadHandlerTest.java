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
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for {@link JarUploadHandler}. */
public class JarUploadHandlerTest extends TestLogger {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private JarUploadHandler jarUploadHandler;

    @Mock private DispatcherGateway mockDispatcherGateway;

    private Path jarDir;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        jarDir = temporaryFolder.newFolder().toPath();
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
    public void testRejectNonJarFiles() throws Exception {
        final Path uploadedFile = Files.createFile(jarDir.resolve("katrin.png"));
        final HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request =
                createRequest(uploadedFile);

        try {
            jarUploadHandler.handleRequest(request, mockDispatcherGateway).get();
            fail("Expected exception not thrown.");
        } catch (final ExecutionException e) {
            final Throwable throwable = ExceptionUtils.stripCompletionException(e.getCause());
            assertThat(throwable, instanceOf(RestHandlerException.class));
            final RestHandlerException restHandlerException = (RestHandlerException) throwable;
            assertThat(
                    restHandlerException.getHttpResponseStatus(),
                    equalTo(HttpResponseStatus.BAD_REQUEST));
        }
    }

    @Test
    public void testUploadJar() throws Exception {
        final Path uploadedFile = Files.createFile(jarDir.resolve("FooBazzleExample.jar"));
        final HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request =
                createRequest(uploadedFile);

        final JarUploadResponseBody jarUploadResponseBody =
                jarUploadHandler.handleRequest(request, mockDispatcherGateway).get();
        assertThat(
                jarUploadResponseBody.getStatus(),
                equalTo(JarUploadResponseBody.UploadStatus.success));
        final String returnedFileNameWithUUID = jarUploadResponseBody.getFilename();
        assertThat(returnedFileNameWithUUID, containsString("_"));
        final String returnedFileName =
                returnedFileNameWithUUID.substring(returnedFileNameWithUUID.lastIndexOf("_") + 1);
        assertThat(returnedFileName, equalTo(uploadedFile.getFileName().toString()));
    }

    @Test
    public void testFailedUpload() throws Exception {
        final Path uploadedFile = jarDir.resolve("FooBazzleExample.jar");
        final HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request =
                createRequest(uploadedFile);

        try {
            jarUploadHandler.handleRequest(request, mockDispatcherGateway).get();
            fail("Expected exception not thrown.");
        } catch (final ExecutionException e) {
            final Throwable throwable = ExceptionUtils.stripCompletionException(e.getCause());
            assertThat(throwable, instanceOf(RestHandlerException.class));
            final RestHandlerException restHandlerException = (RestHandlerException) throwable;
            assertThat(
                    restHandlerException.getMessage(),
                    containsString("Could not move uploaded jar file"));
            assertThat(
                    restHandlerException.getHttpResponseStatus(),
                    equalTo(HttpResponseStatus.INTERNAL_SERVER_ERROR));
        }
    }

    private static HandlerRequest<EmptyRequestBody, EmptyMessageParameters> createRequest(
            final Path uploadedFile) throws HandlerRequestException, IOException {
        return new HandlerRequest<>(
                EmptyRequestBody.getInstance(),
                EmptyMessageParameters.getInstance(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singleton(uploadedFile.toFile()));
    }
}
