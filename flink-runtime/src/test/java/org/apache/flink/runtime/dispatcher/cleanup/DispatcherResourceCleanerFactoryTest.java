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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerRegistry;
import org.apache.flink.runtime.dispatcher.TestingJobManagerRunnerRegistry;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code DispatcherResourceCleanerFactoryTest} verifies that the resources are properly cleaned up
 * for both, the {@link GloballyCleanableResource} and {@link LocallyCleanableResource} of the
 * {@link org.apache.flink.runtime.dispatcher.Dispatcher}.
 */
public class DispatcherResourceCleanerFactoryTest {

    private static final JobID JOB_ID = new JobID();

    private CleanableBlobServer blobServer;

    private CompletableFuture<JobID> jobManagerRunnerRegistryLocalCleanupFuture;
    private CompletableFuture<Void> jobManagerRunnerRegistryLocalCleanupResultFuture;

    private CompletableFuture<JobID> jobGraphWriterLocalCleanupFuture;
    private CompletableFuture<JobID> jobGraphWriterGlobalCleanupFuture;

    private CompletableFuture<JobID> highAvailabilityServicesGlobalCleanupFuture;
    private JobManagerMetricGroup jobManagerMetricGroup;

    private DispatcherResourceCleanerFactory testInstance;

    @BeforeEach
    public void setup() throws Exception {
        blobServer = new CleanableBlobServer();

        MetricRegistry metricRegistry = TestingMetricRegistry.builder().build();
        jobManagerMetricGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(
                        metricRegistry, "ignored hostname");
        jobManagerMetricGroup.addJob(JOB_ID, "ignored job name");

        testInstance =
                new DispatcherResourceCleanerFactory(
                        Executors.directExecutor(),
                        TestingRetryStrategies.NO_RETRY_STRATEGY,
                        createJobManagerRunnerRegistry(),
                        createJobGraphWriter(),
                        blobServer,
                        createHighAvailabilityServices(),
                        jobManagerMetricGroup);
    }

    private JobManagerRunnerRegistry createJobManagerRunnerRegistry() {
        jobManagerRunnerRegistryLocalCleanupFuture = new CompletableFuture<>();
        jobManagerRunnerRegistryLocalCleanupResultFuture = new CompletableFuture<>();

        return TestingJobManagerRunnerRegistry.builder()
                .withLocalCleanupAsyncFunction(
                        (jobId, executor) -> {
                            jobManagerRunnerRegistryLocalCleanupFuture.complete(jobId);
                            return jobManagerRunnerRegistryLocalCleanupResultFuture;
                        })
                .build();
    }

    private JobGraphWriter createJobGraphWriter() throws Exception {
        jobGraphWriterLocalCleanupFuture = new CompletableFuture<>();
        jobGraphWriterGlobalCleanupFuture = new CompletableFuture<>();
        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setGlobalCleanupFunction(
                                (jobId, executor) -> {
                                    jobGraphWriterGlobalCleanupFuture.complete(jobId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .setLocalCleanupFunction(
                                (jobId, ignoredExecutor) -> {
                                    jobGraphWriterLocalCleanupFuture.complete(jobId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        jobGraphStore.start(null);

        return jobGraphStore;
    }

    private HighAvailabilityServices createHighAvailabilityServices() {
        highAvailabilityServicesGlobalCleanupFuture = new CompletableFuture<>();
        final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
        haServices.setGlobalCleanupFuture(highAvailabilityServicesGlobalCleanupFuture);
        return haServices;
    }

    @Test
    public void testLocalResourceCleaning() {
        assertCleanupNotTriggered();

        final CompletableFuture<Void> cleanupResultFuture =
                testInstance
                        .createLocalResourceCleaner(
                                ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .cleanupAsync(JOB_ID);

        assertWaitingForPrioritizedCleanupToFinish();
        assertThat(cleanupResultFuture).isNotCompleted();

        // makes the prioritized JobManagerRunner cleanup result terminate so that other cleanups
        // are triggered
        jobManagerRunnerRegistryLocalCleanupResultFuture.complete(null);

        assertThat(jobManagerRunnerRegistryLocalCleanupFuture).isCompleted();

        assertThat(jobGraphWriterLocalCleanupFuture).isCompleted();
        assertThat(jobGraphWriterGlobalCleanupFuture).isNotDone();

        assertThat(blobServer.getLocalCleanupFuture()).isCompleted();
        assertThat(blobServer.getGlobalCleanupFuture()).isNotDone();

        assertThat(highAvailabilityServicesGlobalCleanupFuture).isNotDone();

        assertJobMetricGroupCleanedUp();

        assertThat(cleanupResultFuture).isCompleted();
    }

    @Test
    public void testGlobalResourceCleaning()
            throws ExecutionException, InterruptedException, TimeoutException {
        assertCleanupNotTriggered();

        final CompletableFuture<Void> cleanupResultFuture =
                testInstance
                        .createGlobalResourceCleaner(
                                ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .cleanupAsync(JOB_ID);

        assertWaitingForPrioritizedCleanupToFinish();
        assertThat(cleanupResultFuture).isNotCompleted();

        // makes the prioritized JobManagerRunner cleanup result terminate so that other cleanups
        // are triggered
        jobManagerRunnerRegistryLocalCleanupResultFuture.complete(null);

        assertThat(jobManagerRunnerRegistryLocalCleanupFuture).isCompleted();

        assertThat(jobGraphWriterLocalCleanupFuture).isNotDone();
        assertThat(jobGraphWriterGlobalCleanupFuture).isCompleted();

        assertThat(blobServer.getLocalCleanupFuture()).isNotDone();
        assertThat(blobServer.getGlobalCleanupFuture()).isCompleted();

        assertThat(highAvailabilityServicesGlobalCleanupFuture).isCompleted();

        assertJobMetricGroupCleanedUp();

        assertThat(cleanupResultFuture).isCompleted();
    }

    private void assertCleanupNotTriggered() {
        assertThat(jobManagerRunnerRegistryLocalCleanupFuture).isNotDone();

        assertNoRegularCleanupsTriggered();
    }

    private void assertWaitingForPrioritizedCleanupToFinish() {
        assertThat(jobManagerRunnerRegistryLocalCleanupFuture).isCompleted();

        assertNoRegularCleanupsTriggered();
    }

    private void assertNoRegularCleanupsTriggered() {
        assertThat(jobGraphWriterLocalCleanupFuture).isNotDone();
        assertThat(jobGraphWriterGlobalCleanupFuture).isNotDone();

        assertThat(blobServer.getLocalCleanupFuture()).isNotDone();
        assertThat(blobServer.getGlobalCleanupFuture()).isNotDone();

        assertThat(highAvailabilityServicesGlobalCleanupFuture).isNotDone();

        // check whether the registered job is still listed
        assertThat(jobManagerMetricGroup.numRegisteredJobMetricGroups()).isEqualTo(1);
    }

    private void assertJobMetricGroupCleanedUp() {
        assertThat(jobManagerMetricGroup.numRegisteredJobMetricGroups()).isEqualTo(0);
    }

    private static class CleanableBlobServer extends BlobServer {

        private final CompletableFuture<JobID> localCleanupFuture = new CompletableFuture<>();
        private final CompletableFuture<JobID> globalCleanupFuture = new CompletableFuture<>();

        public CleanableBlobServer() throws IOException {
            super(
                    new Configuration(),
                    new File("non-existent-file"),
                    new TestingBlobStoreBuilder().createTestingBlobStore());
        }

        @Override
        public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor ignoredExecutor) {
            localCleanupFuture.complete(jobId);

            return FutureUtils.completedVoidFuture();
        }

        @Override
        public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor ignoredExecutor) {
            globalCleanupFuture.complete(jobId);

            return FutureUtils.completedVoidFuture();
        }

        public CompletableFuture<JobID> getLocalCleanupFuture() {
            return localCleanupFuture;
        }

        public CompletableFuture<JobID> getGlobalCleanupFuture() {
            return globalCleanupFuture;
        }
    }
}
