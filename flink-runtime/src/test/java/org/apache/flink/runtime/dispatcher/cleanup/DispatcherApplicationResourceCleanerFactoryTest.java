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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmanager.ApplicationStoreEntry;
import org.apache.flink.runtime.jobmanager.ApplicationWriter;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code DispatcherApplicationResourceCleanerFactoryTest} verifies that the resources are properly
 * cleaned up for the {@link GloballyCleanableApplicationResource} of the {@link
 * org.apache.flink.runtime.dispatcher.Dispatcher} in Application mode.
 */
public class DispatcherApplicationResourceCleanerFactoryTest {

    private static final ApplicationID APPLICATION_ID = new ApplicationID();

    private CleanableBlobServer blobServer;
    private CleanableApplicationWriter applicationWriter;

    private DispatcherApplicationResourceCleanerFactory testInstance;

    @BeforeEach
    public void setup() throws Exception {
        blobServer = new CleanableBlobServer();
        applicationWriter = new CleanableApplicationWriter();

        testInstance =
                new DispatcherApplicationResourceCleanerFactory(
                        Executors.directExecutor(),
                        TestingRetryStrategies.NO_RETRY_STRATEGY,
                        applicationWriter,
                        blobServer);
    }

    @Test
    public void testApplicationResourceCleaning() {
        assertCleanupNotTriggered();

        final CompletableFuture<Void> cleanupResultFuture =
                testInstance
                        .createApplicationResourceCleaner(
                                ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .cleanupAsync(APPLICATION_ID);

        assertThat(cleanupResultFuture).isCompleted();

        assertThat(applicationWriter.getGlobalCleanupFuture()).isCompleted();
        assertThat(blobServer.getGlobalCleanupFuture()).isCompleted();
    }

    private void assertCleanupNotTriggered() {
        assertThat(applicationWriter.getGlobalCleanupFuture()).isNotDone();
        assertThat(blobServer.getGlobalCleanupFuture()).isNotDone();
    }

    /** Test implementation of {@link ApplicationWriter} that tracks cleanup invocations. */
    private static class CleanableApplicationWriter implements ApplicationWriter {

        private final CompletableFuture<ApplicationID> globalCleanupFuture =
                new CompletableFuture<>();

        @Override
        public void putApplication(ApplicationStoreEntry application) {
            // No-op for testing
        }

        @Override
        public CompletableFuture<Void> globalCleanupAsync(
                ApplicationID applicationId, Executor ignoredExecutor) {
            globalCleanupFuture.complete(applicationId);
            return FutureUtils.completedVoidFuture();
        }

        public CompletableFuture<ApplicationID> getGlobalCleanupFuture() {
            return globalCleanupFuture;
        }
    }

    /** Test implementation of {@link BlobServer} that tracks cleanup invocations. */
    private static class CleanableBlobServer extends BlobServer {

        private final CompletableFuture<ApplicationID> globalCleanupFuture =
                new CompletableFuture<>();

        public CleanableBlobServer() throws IOException {
            super(
                    new Configuration(),
                    new File("non-existent-file"),
                    new TestingBlobStoreBuilder().createTestingBlobStore());
        }

        @Override
        public CompletableFuture<Void> globalCleanupAsync(
                ApplicationID applicationId, Executor ignoredExecutor) {
            globalCleanupFuture.complete(applicationId);
            return FutureUtils.completedVoidFuture();
        }

        public CompletableFuture<ApplicationID> getGlobalCleanupFuture() {
            return globalCleanupFuture;
        }
    }
}
