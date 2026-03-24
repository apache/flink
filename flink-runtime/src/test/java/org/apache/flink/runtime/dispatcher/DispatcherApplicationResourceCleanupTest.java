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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.dispatcher.cleanup.TestingApplicationResourceCleanerFactory;
import org.apache.flink.runtime.highavailability.ApplicationResultEntry;
import org.apache.flink.runtime.highavailability.ApplicationResultStore;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testutils.TestingApplicationResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests the application resource cleanup by the {@link Dispatcher}. */
@ExtendWith(TestLoggerExtension.class)
public class DispatcherApplicationResourceCleanupTest {

    @TempDir public Path tempDir;

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private static final Duration timeout = Duration.ofSeconds(10L);

    private static TestingRpcService rpcService;

    private ApplicationID applicationId;

    private TestingDispatcher dispatcher;

    private DispatcherGateway dispatcherGateway;

    private BlobServer blobServer;

    private CompletableFuture<ApplicationID> globalCleanupFuture;

    @BeforeAll
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @BeforeEach
    void setup() throws Exception {
        applicationId = new ApplicationID();

        globalCleanupFuture = new CompletableFuture<>();

        blobServer =
                BlobUtils.createBlobServer(
                        new Configuration(),
                        Reference.owned(tempDir.toFile()),
                        new TestingBlobStoreBuilder().createTestingBlobStore());
    }

    @AfterEach
    void teardown() throws Exception {
        if (dispatcher != null) {
            dispatcher.close();
        }

        if (blobServer != null) {
            blobServer.close();
        }
    }

    @AfterAll
    static void teardownClass() throws ExecutionException, InterruptedException {
        if (rpcService != null) {
            rpcService.closeAsync().get();
        }
    }

    @Test
    void testGlobalCleanupWhenApplicationFinished() throws Exception {
        startDispatcher(createTestingDispatcherBuilder());

        submitApplicationAndWait();

        CompletableFuture<?> applicationTerminationFuture =
                dispatcher.getApplicationTerminationFuture(applicationId);

        mockApplicationFinished();

        applicationTerminationFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        assertGlobalCleanupTriggered(applicationId);
    }

    @Test
    void testApplicationMarkedAsDirtyBeforeCleanup() throws Exception {
        final OneShotLatch markAsDirtyLatch = new OneShotLatch();

        final TestingDispatcher.Builder dispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setApplicationResultStore(
                                TestingApplicationResultStore.builder()
                                        .withCreateDirtyResultConsumer(
                                                ignoredApplicationResultEntry -> {
                                                    try {
                                                        markAsDirtyLatch.await();
                                                    } catch (InterruptedException e) {
                                                        Thread.currentThread().interrupt();
                                                        return FutureUtils.completedExceptionally(
                                                                e);
                                                    }
                                                    return FutureUtils.completedVoidFuture();
                                                })
                                        .build());
        startDispatcher(dispatcherBuilder);

        submitApplicationAndWait();

        mockApplicationFinished();

        assertThatNoCleanupWasTriggered();

        // Trigger the latch to allow dirty result creation to complete
        markAsDirtyLatch.trigger();

        // Now cleanup should be triggered
        assertGlobalCleanupTriggered(applicationId);
    }

    @Test
    void testApplicationMarkedAsCleanAfterCleanup() throws Exception {
        final CompletableFuture<ApplicationID> markAsCleanFuture = new CompletableFuture<>();

        final ApplicationResultStore applicationResultStore =
                TestingApplicationResultStore.builder()
                        .withMarkResultAsCleanConsumer(
                                applicationId -> {
                                    markAsCleanFuture.complete(applicationId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        final OneShotLatch cleanupLatch = new OneShotLatch();

        final TestingApplicationResourceCleanerFactory resourceCleanerFactory =
                TestingApplicationResourceCleanerFactory.builder()
                        .withGloballyCleanableResource(
                                (applicationId, ignoredExecutor) -> {
                                    try {
                                        cleanupLatch.await();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }

                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        final TestingDispatcher.Builder dispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setApplicationResultStore(applicationResultStore)
                        .setApplicationResourceCleanerFactory(resourceCleanerFactory);

        startDispatcher(dispatcherBuilder);

        submitApplicationAndWait();

        CompletableFuture<?> applicationTerminationFuture =
                dispatcher.getApplicationTerminationFuture(applicationId);

        mockApplicationFinished();

        // Mark as clean should not have been called yet
        assertFalse(markAsCleanFuture.isDone());

        // Trigger cleanup
        cleanupLatch.trigger();

        // Wait for cleanup to complete
        applicationTerminationFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        // Verify mark as clean was called
        assertEquals(
                applicationId, markAsCleanFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
    }

    @Test
    void testDispatcherTerminationTerminatesRunningApplications() throws Exception {
        startDispatcher(createTestingDispatcherBuilder());

        submitApplicationAndWait();

        CompletableFuture<?> applicationTerminationFuture =
                dispatcher.getApplicationTerminationFuture(applicationId);

        dispatcher.closeAsync().get();

        assertThrows(CancellationException.class, applicationTerminationFuture::get);
    }

    @Test
    void testFatalErrorIfApplicationCannotBeMarkedDirtyInApplicationResultStore() throws Exception {
        final ApplicationResultStore applicationResultStore =
                TestingApplicationResultStore.builder()
                        .withCreateDirtyResultConsumer(
                                applicationResult ->
                                        FutureUtils.completedExceptionally(
                                                new IOException("Expected IOException.")))
                        .build();

        startDispatcher(
                createTestingDispatcherBuilder().setApplicationResultStore(applicationResultStore));

        submitApplicationAndWait();

        mockApplicationFinished();

        // Fatal error should be reported
        final CompletableFuture<? extends Throwable> errorFuture =
                testingFatalErrorHandlerResource.getTestingFatalErrorHandler().getErrorFuture();
        assertThat(errorFuture.get()).isInstanceOf(FlinkException.class);

        testingFatalErrorHandlerResource.getTestingFatalErrorHandler().clearError();
    }

    @Test
    void testErrorHandlingIfApplicationCannotBeMarkedAsCleanInApplicationResultStore()
            throws Exception {
        final CompletableFuture<ApplicationResultEntry> dirtyApplicationFuture =
                new CompletableFuture<>();
        final ApplicationResultStore applicationResultStore =
                TestingApplicationResultStore.builder()
                        .withCreateDirtyResultConsumer(
                                applicationResultEntry -> {
                                    dirtyApplicationFuture.complete(applicationResultEntry);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .withMarkResultAsCleanConsumer(
                                applicationId ->
                                        FutureUtils.completedExceptionally(
                                                new IOException("Expected IOException.")))
                        .build();

        startDispatcher(
                createTestingDispatcherBuilder().setApplicationResultStore(applicationResultStore));

        submitApplicationAndWait();

        mockApplicationFinished();

        // No fatal error should be reported (mark as clean failure is handled gracefully)
        final CompletableFuture<? extends Throwable> errorFuture =
                testingFatalErrorHandlerResource.getTestingFatalErrorHandler().getErrorFuture();
        try {
            errorFuture.get(100, TimeUnit.MILLISECONDS);
            fail("No error should have been reported.");
        } catch (TimeoutException e) {
            // expected
        }

        // Dirty result should have been created
        assertEquals(
                applicationId,
                dirtyApplicationFuture
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS)
                        .getApplicationId());
    }

    @Test
    void testArchivingFinishedApplicationToHistoryServer() throws Exception {
        final CompletableFuture<Acknowledge> archiveFuture = new CompletableFuture<>();

        final TestingDispatcher.Builder dispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveApplicationFunction(
                                                archivedApplication -> archiveFuture)
                                        .build());

        startDispatcher(dispatcherBuilder);

        submitApplicationAndWait();

        CompletableFuture<?> applicationTerminationFuture =
                dispatcher.getApplicationTerminationFuture(applicationId);

        mockApplicationFinished();

        // Before the archiving is finished, the cleanup is not finished and the application is not
        // terminated
        assertThatNoCleanupWasTriggered();
        assertFalse(applicationTerminationFuture.isDone());

        // Complete archiving
        archiveFuture.complete(Acknowledge.get());

        // Once the archive is finished, the cleanup is finished and the application is terminated.
        assertGlobalCleanupTriggered(applicationId);
        applicationTerminationFuture.join();
    }

    private void startDispatcher(TestingDispatcher.Builder dispatcherBuilder) throws Exception {
        dispatcher = dispatcherBuilder.build(rpcService);

        dispatcher.start();

        dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
    }

    private TestingDispatcher.Builder createTestingDispatcherBuilder() {
        return TestingDispatcher.builder()
                .setBlobServer(blobServer)
                .setFatalErrorHandler(
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler())
                .setApplicationResourceCleanerFactory(
                        TestingApplicationResourceCleanerFactory.builder()
                                .withGloballyCleanableResource(
                                        (applicationId, ignoredExecutor) -> {
                                            globalCleanupFuture.complete(applicationId);
                                            return FutureUtils.completedVoidFuture();
                                        })
                                .build());
    }

    private void assertThatNoCleanupWasTriggered() {
        assertThat(globalCleanupFuture.isDone()).isFalse();
    }

    private void assertGlobalCleanupTriggered(ApplicationID applicationId)
            throws ExecutionException, InterruptedException, TimeoutException {
        assertThat(globalCleanupFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS))
                .isEqualTo(applicationId);
    }

    private CompletableFuture<Acknowledge> submitApplication() {
        return dispatcherGateway.submitApplication(
                TestingApplication.builder().setApplicationId(applicationId).build(), timeout);
    }

    private void submitApplicationAndWait() {
        submitApplication().join();
    }

    private void mockApplicationFinished() throws Exception {
        dispatcher
                .callAsyncInMainThread(
                        () -> {
                            dispatcher.notifyApplicationStatusChange(
                                    applicationId, ApplicationState.FINISHED);
                            return CompletableFuture.completedFuture(null);
                        })
                .get();
    }
}
