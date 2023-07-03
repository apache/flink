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
import org.apache.flink.runtime.blob.TransientBlobKey;
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
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AbstractTaskManagerFileHandler}. */
class AbstractTaskManagerFileHandlerTest {

    private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();

    private static final DefaultFullHttpRequest HTTP_REQUEST =
            new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, TestUntypedMessageHeaders.URL);

    @TempDir private static File temporaryFolder;

    private static BlobServer blobServer;

    private static HandlerRequest<EmptyRequestBody> handlerRequest;

    private String fileContent1;

    private TransientBlobKey transientBlobKey1;

    private String fileContent2;

    private TransientBlobKey transientBlobKey2;

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

    @BeforeEach
    void setupTest() throws IOException {
        fileContent1 = UUID.randomUUID().toString();
        final File file1 = createFileWithContent(fileContent1);
        transientBlobKey1 = storeFileInBlobServer(file1);

        fileContent2 = UUID.randomUUID().toString();
        final File file2 = createFileWithContent(fileContent2);
        transientBlobKey2 = storeFileInBlobServer(file2);
    }

    @AfterAll
    static void teardown() throws IOException {
        if (blobServer != null) {
            blobServer.close();
            blobServer = null;
        }
    }

    /** Tests that the {@link AbstractTaskManagerFileHandler} serves the requested file. */
    @Test
    void testFileServing() throws Exception {
        final Time cacheEntryDuration = Time.milliseconds(1000L);

        final Queue<CompletableFuture<TransientBlobKey>> requestFileUploads = new ArrayDeque<>(1);

        requestFileUploads.add(CompletableFuture.completedFuture(transientBlobKey1));

        final TestingTaskManagerFileHandler testingTaskManagerFileHandler =
                createTestTaskManagerFileHandler(
                        cacheEntryDuration, requestFileUploads, EXPECTED_TASK_MANAGER_ID);

        final File outputFile = TempDirUtils.newFile(temporaryFolder.toPath());
        final TestingChannelHandlerContext testingContext =
                new TestingChannelHandlerContext(outputFile);

        testingTaskManagerFileHandler.respondToRequest(
                testingContext, HTTP_REQUEST, handlerRequest, null);

        assertThat(outputFile).isNotEmpty();
        assertThat(FileUtils.readFileUtf8(outputFile)).isEqualTo(fileContent1);
    }

    /** Tests that files are cached. */
    @Test
    void testFileCaching() throws Exception {
        final File outputFile = runFileCachingTest(Time.milliseconds(5000L), Time.milliseconds(0L));

        assertThat(outputFile).isNotEmpty();
        assertThat(FileUtils.readFileUtf8(outputFile)).isEqualTo(fileContent1);
    }

    /** Tests that file cache entries expire. */
    @Test
    void testFileCacheExpiration() throws Exception {
        final Time cacheEntryDuration = Time.milliseconds(5L);

        final File outputFile = runFileCachingTest(cacheEntryDuration, cacheEntryDuration);

        assertThat(outputFile).isNotEmpty();
        assertThat(FileUtils.readFileUtf8(outputFile)).isEqualTo(fileContent2);
    }

    private File runFileCachingTest(Time cacheEntryDuration, Time delayBetweenRequests)
            throws Exception {
        final Queue<CompletableFuture<TransientBlobKey>> requestFileUploads = new ArrayDeque<>(2);
        requestFileUploads.add(CompletableFuture.completedFuture(transientBlobKey1));
        requestFileUploads.add(CompletableFuture.completedFuture(transientBlobKey2));

        final TestingTaskManagerFileHandler testingTaskManagerFileHandler =
                createTestTaskManagerFileHandler(
                        cacheEntryDuration, requestFileUploads, EXPECTED_TASK_MANAGER_ID);

        final File outputFile = TempDirUtils.newFile(temporaryFolder.toPath());
        final TestingChannelHandlerContext testingContext =
                new TestingChannelHandlerContext(outputFile);

        testingTaskManagerFileHandler.respondToRequest(
                testingContext, HTTP_REQUEST, handlerRequest, null);

        Thread.sleep(delayBetweenRequests.toMilliseconds());

        // the handler should not trigger the file upload again because it is still cached
        testingTaskManagerFileHandler.respondToRequest(
                testingContext, HTTP_REQUEST, handlerRequest, null);
        return outputFile;
    }

    private TestingTaskManagerFileHandler createTestTaskManagerFileHandler(
            Time cacheEntryDuration,
            Queue<CompletableFuture<TransientBlobKey>> requestFileUploads,
            ResourceID expectedTaskManagerId) {
        final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

        return new TestingTaskManagerFileHandler(
                () -> CompletableFuture.completedFuture(null),
                TestingUtils.infiniteTime(),
                Collections.emptyMap(),
                new TestUntypedMessageHeaders(),
                () -> CompletableFuture.completedFuture(resourceManagerGateway),
                blobServer,
                cacheEntryDuration,
                requestFileUploads,
                expectedTaskManagerId);
    }

    private static File createFileWithContent(String fileContent) throws IOException {
        final File file = TempDirUtils.newFile(temporaryFolder.toPath());

        // write random content into the file
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(fileContent.getBytes("UTF-8"));
        }

        return file;
    }

    private static TransientBlobKey storeFileInBlobServer(File fileToStore) throws IOException {
        // store the requested file in the BlobServer
        try (FileInputStream fileInputStream = new FileInputStream(fileToStore)) {
            return blobServer.getTransientBlobService().putTransient(fileInputStream);
        }
    }
}
