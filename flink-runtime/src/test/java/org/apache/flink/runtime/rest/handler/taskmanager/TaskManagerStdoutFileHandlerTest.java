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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerFileMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TaskManagerStdoutFileHandler}. */
class TaskManagerStdoutFileHandlerTest {

    private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();

    private static final DefaultFullHttpRequest HTTP_REQUEST =
            new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, TestUntypedMessageHeaders.URL);

    @TempDir private static File temporaryFolder;

    private static BlobServer blobServer;

    private static HandlerRequest<EmptyRequestBody> handlerRequest;

    @BeforeAll
    static void setup() throws IOException, HandlerRequestException {
        final Configuration configuration = new Configuration();

        blobServer = new BlobServer(configuration, temporaryFolder, new VoidBlobStore());

        handlerRequest =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        new TaskManagerFileMessageParameters(),
                        Collections.singletonMap(
                                TaskManagerIdPathParameter.KEY,
                                EXPECTED_TASK_MANAGER_ID.getResourceIdString()),
                        Collections.emptyMap(),
                        Collections.emptyList());
    }

    @AfterAll
    static void teardown() throws IOException {
        if (blobServer != null) {
            blobServer.close();
            blobServer = null;
        }
    }

    @Test
    void testStdoutFileHandlerHandleFileNotFoundException() throws Exception {
        final Time cacheEntryDuration = Time.milliseconds(1000L);
        final TestingChannelHandlerContext testingContext;
        CompletableFuture<Void> handleFuture;
        try (TestingTaskManagerStdoutFileHandler testingTaskManagerStdoutFileHandler =
                createTestTaskManagerStdoutFileHandler(
                        cacheEntryDuration, new FileNotFoundException("file not found"))) {
            final File outputFile = TempDirUtils.newFile(temporaryFolder.toPath());
            testingContext = new TestingChannelHandlerContext(outputFile);

            handleFuture =
                    testingTaskManagerStdoutFileHandler.respondToRequest(
                            testingContext, HTTP_REQUEST, handlerRequest, null);
        }
        assertThat(handleFuture).isCompleted();
        assertThat(testingContext.getHttpResponse())
                .isNotNull()
                .satisfies(
                        httpResponse ->
                                assertThat(httpResponse.status()).isEqualTo(HttpResponseStatus.OK));
        assertThat(testingContext.getResponseData())
                .isNotNull()
                .satisfies(
                        data ->
                                assertThat(new String(data, "UTF-8"))
                                        .isEqualTo(
                                                TaskManagerStdoutFileHandler.FILE_NOT_FOUND_INFO));
    }

    @Test
    void testStdoutFileHandlerHandleOtherException() throws Exception {
        final Time cacheEntryDuration = Time.milliseconds(1000L);
        final TestingChannelHandlerContext testingContext;
        CompletableFuture<Void> handleFuture;
        try (TestingTaskManagerStdoutFileHandler testingTaskManagerStdoutFileHandler =
                createTestTaskManagerStdoutFileHandler(
                        cacheEntryDuration, new FlinkException("excepted exception"))) {
            final File outputFile = TempDirUtils.newFile(temporaryFolder.toPath());
            testingContext = new TestingChannelHandlerContext(outputFile);

            handleFuture =
                    testingTaskManagerStdoutFileHandler.respondToRequest(
                            testingContext, HTTP_REQUEST, handlerRequest, null);
        }

        assertThat(handleFuture).isCompletedExceptionally();
        assertThat(testingContext.getHttpResponse()).isNull();
        assertThat(testingContext.getResponseData()).isNull();
    }

    private TestingTaskManagerStdoutFileHandler createTestTaskManagerStdoutFileHandler(
            Time cacheEntryDuration, Exception exceptionForRequestFileUpload) {
        final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

        return new TestingTaskManagerStdoutFileHandler(
                () -> CompletableFuture.completedFuture(null),
                TestingUtils.infiniteTime(),
                Collections.emptyMap(),
                new TestUntypedMessageHeaders(),
                () -> CompletableFuture.completedFuture(resourceManagerGateway),
                blobServer,
                cacheEntryDuration,
                exceptionForRequestFileUpload);
    }
}
