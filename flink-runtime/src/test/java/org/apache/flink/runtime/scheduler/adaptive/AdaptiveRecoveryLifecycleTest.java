/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.singleNoOpJobGraph;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createTaskManagerGateway;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.offerSlots;
import static org.assertj.core.api.Assertions.assertThat;

/** Lifecycle regression for a real graph supplier running on the borrowed recovery executor. */
class AdaptiveRecoveryLifecycleTest {

    private static final long TIMEOUT_SECONDS = 10;

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> mainExecutor =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> generalExecutor =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> recoveryExecutor =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> futureExecutor =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    @Test
    void closeWaitsForRunningRecoveryAndFencesSuccessfulStaleGraph() throws Exception {
        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        mainExecutor.getExecutor());
        final Thread recoveryThread =
                recoveryExecutor
                        .getExecutor()
                        .submit(Thread::currentThread)
                        .get(TIMEOUT_SECONDS, SECONDS);
        final CompletableFuture<Thread> supplierEntered = new CompletableFuture<>();
        final CompletableFuture<Void> releaseSupplier = new CompletableFuture<>();
        final CompletableFuture<ExecutionGraph> supplierSucceeded = new CompletableFuture<>();
        final CompletableFuture<JobStatus> storeShutdown = new CompletableFuture<>();
        final CompletableFuture<JobStatus> counterShutdown = new CompletableFuture<>();
        final CompletableFuture<TaskDeploymentDescriptor> deployment = new CompletableFuture<>();
        final CompletableFuture<Throwable> fatalError = new CompletableFuture<>();
        final List<JobStatus> jobStatuses = new CopyOnWriteArrayList<>();

        final StandaloneCompletedCheckpointStore store =
                new StandaloneCompletedCheckpointStore(1) {
                    @Override
                    public void shutdown(JobStatus status, CheckpointsCleaner cleaner)
                            throws Exception {
                        storeShutdown.complete(status);
                        super.shutdown(status, cleaner);
                    }
                };
        final StandaloneCheckpointIDCounter counter =
                new StandaloneCheckpointIDCounter() {
                    @Override
                    public CompletableFuture<Void> shutdown(JobStatus status) {
                        counterShutdown.complete(status);
                        return super.shutdown(status);
                    }
                };
        final TestingBlobWriter blobWriter =
                new TestingBlobWriter(0) {
                    @Override
                    public PermanentBlobKey putPermanent(JobID jobId, byte[] value)
                            throws IOException {
                        supplierEntered.complete(Thread.currentThread());
                        try {
                            releaseSupplier.get(TIMEOUT_SECONDS, SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        } catch (ExecutionException | TimeoutException e) {
                            throw new IOException(e);
                        }
                        return super.putPermanent(jobId, value);
                    }
                };

        final JobGraph jobGraph = singleNoOpJobGraph();
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder().build(), null));
        final Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ZERO);
        final DefaultDeclarativeSlotPool slotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Duration.ofMinutes(5),
                        Duration.ofSeconds(TIMEOUT_SECONDS),
                        Duration.ZERO,
                        mainThreadExecutor);
        final TaskManagerGateway taskManagerGateway =
                createTaskManagerGateway(
                        new TestingTaskExecutorGatewayBuilder()
                                .setSubmitTaskConsumer(
                                        (descriptor, jobMasterId) -> {
                                            deployment.complete(descriptor);
                                            return CompletableFuture.completedFuture(
                                                    Acknowledge.get());
                                        })
                                .createTestingTaskExecutorGateway());
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph, mainThreadExecutor, generalExecutor.getExecutor())
                        .setRecoveryExecutor(recoveryExecutor.getExecutor())
                        .setFutureExecutor(futureExecutor.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .setDeclarativeSlotPool(slotPool)
                        .setCheckpointRecoveryFactory(
                                new TestingCheckpointRecoveryFactory(store, counter))
                        .setBlobWriter(blobWriter)
                        .setExecutionGraphFactoryDecorator(
                                delegate ->
                                        (graph,
                                                completedStore,
                                                cleaner,
                                                idCounter,
                                                stats,
                                                partitionConstraint,
                                                timestamp,
                                                attempts,
                                                parallelism,
                                                listener,
                                                markFinished,
                                                schedulingContext,
                                                logger) -> {
                                            final ExecutionGraph created =
                                                    delegate.createAndRestoreExecutionGraph(
                                                            graph,
                                                            completedStore,
                                                            cleaner,
                                                            idCounter,
                                                            stats,
                                                            partitionConstraint,
                                                            timestamp,
                                                            attempts,
                                                            parallelism,
                                                            listener,
                                                            markFinished,
                                                            schedulingContext,
                                                            logger);
                                            supplierSucceeded.complete(created);
                                            return created;
                                        })
                        .setFatalErrorHandler(fatalError::complete)
                        .setJobStatusListener((id, status, timestamp) -> jobStatuses.add(status))
                        .build();
        CompletableFuture<Void> closeFuture = null;
        try {
            CompletableFuture.runAsync(
                            () -> {
                                scheduler.startScheduling();
                                offerSlots(
                                        slotPool,
                                        createSlotOffersForResourceRequirements(
                                                ResourceCounter.withResource(
                                                        ResourceProfile.UNKNOWN, 1)),
                                        taskManagerGateway);
                            },
                            mainThreadExecutor)
                    .get(TIMEOUT_SECONDS, SECONDS);
            assertThat(supplierEntered.get(TIMEOUT_SECONDS, SECONDS)).isSameAs(recoveryThread);

            // Synchronous storage can still hold recovery even while the other lanes are idle.
            final Thread generalThread =
                    generalExecutor
                            .getExecutor()
                            .submit(Thread::currentThread)
                            .get(TIMEOUT_SECONDS, SECONDS);
            final Thread futureThread =
                    futureExecutor
                            .getExecutor()
                            .submit(Thread::currentThread)
                            .get(TIMEOUT_SECONDS, SECONDS);
            assertThat(generalThread).isNotSameAs(recoveryThread).isNotSameAs(futureThread);
            assertThat(futureThread).isNotSameAs(recoveryThread);
            closeFuture =
                    CompletableFuture.supplyAsync(
                                    () -> {
                                        assertThat(scheduler.getState())
                                                .isInstanceOf(CreatingExecutionGraph.class);
                                        return scheduler.closeAsync();
                                    },
                                    mainThreadExecutor)
                            .get(TIMEOUT_SECONDS, SECONDS);
            drain(mainExecutor.getExecutor());
            assertThat(closeFuture).isNotDone();
            assertThat(storeShutdown).isNotDone();
            assertThat(counterShutdown).isNotDone();
            assertThat(deployment).isNotDone();

            releaseSupplier.complete(null);
            closeFuture.get(TIMEOUT_SECONDS, SECONDS);
            // Also proves scheduler shutdown did not shut down its borrowed recovery executor.
            assertThat(
                            recoveryExecutor
                                    .getExecutor()
                                    .submit(Thread::currentThread)
                                    .get(TIMEOUT_SECONDS, SECONDS))
                    .isSameAs(recoveryThread);
            // The result handoff schedules another main-thread callback; drain both generations.
            drain(mainExecutor.getExecutor());
            drain(mainExecutor.getExecutor());
            drain(futureExecutor.getExecutor());

            // Closing also succeeds for a failed supplier: require a successful late graph here.
            assertThat(supplierSucceeded.get(TIMEOUT_SECONDS, SECONDS)).isNotNull();
            assertThat(storeShutdown.get(TIMEOUT_SECONDS, SECONDS)).isEqualTo(JobStatus.SUSPENDED);
            assertThat(counterShutdown.get(TIMEOUT_SECONDS, SECONDS))
                    .isEqualTo(JobStatus.SUSPENDED);
            assertThat(
                            CompletableFuture.supplyAsync(scheduler::getState, mainThreadExecutor)
                                    .get(TIMEOUT_SECONDS, SECONDS))
                    .isInstanceOf(Finished.class);
            assertThat(jobStatuses).doesNotContain(JobStatus.RUNNING);
            assertThat(deployment).isNotDone();
            assertThat(fatalError).isNotDone();
        } finally {
            releaseSupplier.complete(null);
            if (closeFuture == null) {
                closeFuture =
                        CompletableFuture.supplyAsync(scheduler::closeAsync, mainThreadExecutor)
                                .get(TIMEOUT_SECONDS, SECONDS);
            }
            closeFuture.get(TIMEOUT_SECONDS, SECONDS);
        }
    }

    private static void drain(ScheduledExecutorService executor) throws Exception {
        executor.submit(() -> {}).get(TIMEOUT_SECONDS, SECONDS);
    }
}
