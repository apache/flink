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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import javax.annotation.Nullable;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration test harness that drives a real {@link JobMasterServiceLeadershipRunner} on top of
 * real {@link DefaultJobMasterServiceProcess} instances, using a {@link TestingJobMasterService} as
 * the only test double.
 *
 * <p>Unlike {@link JobMasterServiceLeadershipRunnerTest}, which stubs the process, this harness
 * exercises the runner and the process together so the two halves of the FLINK-39704 fix (the
 * runner caching a globally terminal result across leadership revocation and the process deferring
 * its {@link JobNotFinishedException} during close) are tested in combination.
 *
 * <p>Every seam that decides the race outcome is controllable from the test thread:
 *
 * <ul>
 *   <li>leadership grant/revoke via {@link #grantLeadership()} / {@link #revokeLeadership()},
 *   <li>the moment a globally terminal result is observed via {@link #reachGloballyTerminalState},
 *   <li>the moment the underlying {@link JobMasterService} finishes closing via {@link
 *       #completeCurrentServiceTermination} (this is what opens the process's close window), and
 *   <li>the leadership re-validation performed while forwarding the result via {@link
 *       #armDelayedForwardingCheck} / {@link #releaseDelayedForwardingCheck}, which lets a
 *       revocation interleave between observing the result and forwarding it.
 * </ul>
 *
 * <p>This keeps the reproduction deterministic without relying on timing.
 */
final class JobMasterServiceLeadershipRunnerTestHarness implements AutoCloseable {

    private final JobID jobId = new JobID();
    private final String jobName = "test-job";
    private final JobType jobType = JobType.STREAMING;

    private final DelayableLeaderElection leaderElection = new DelayableLeaderElection();

    private final BlockingQueue<OnCompletionActions> onCompletionActionsQueue =
            new LinkedBlockingQueue<>();
    private final Queue<CompletableFuture<Void>> serviceTerminationFutures =
            new ConcurrentLinkedQueue<>();

    private final JobMasterServiceLeadershipRunner runner;

    @Nullable private CompletableFuture<Boolean> armedForwardingCheck;

    JobMasterServiceLeadershipRunnerTestHarness() {
        final JobMasterServiceFactory jobMasterServiceFactory =
                new TestingJobMasterServiceFactory(
                        onCompletionActions -> {
                            final CompletableFuture<Void> serviceTerminationFuture =
                                    new CompletableFuture<>();
                            serviceTerminationFutures.add(serviceTerminationFuture);
                            onCompletionActionsQueue.add(onCompletionActions);
                            return CompletableFuture.completedFuture(
                                    new TestingJobMasterService(
                                            "localhost", serviceTerminationFuture, null));
                        });

        final JobMasterServiceProcessFactory processFactory =
                new DefaultJobMasterServiceProcessFactory(
                        jobId,
                        jobName,
                        jobType,
                        null,
                        System.currentTimeMillis(),
                        jobMasterServiceFactory);

        this.runner =
                new JobMasterServiceLeadershipRunner(
                        processFactory,
                        leaderElection,
                        new EmbeddedJobResultStore(),
                        TestingClassLoaderLease.newBuilder().build(),
                        new TestingFatalErrorHandler());
    }

    void start() throws Exception {
        runner.start();
    }

    /** Grants leadership and waits until the new process has been confirmed as leader. */
    UUID grantLeadership() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        leaderElection.isLeader(leaderSessionId).get();
        return leaderSessionId;
    }

    void revokeLeadership() {
        leaderElection.notLeader();
    }

    /** Makes the current process observe a globally terminal result, as the scheduler would. */
    void reachGloballyTerminalState(JobStatus terminalState) throws InterruptedException {
        onCompletionActionsQueue
                .take()
                .jobReachedGloballyTerminalState(createExecutionGraphInfo(terminalState));
    }

    /** Completes the current {@link JobMasterService}'s termination, unblocking process close. */
    void completeCurrentServiceTermination() {
        final CompletableFuture<Void> serviceTerminationFuture = serviceTerminationFutures.poll();
        if (serviceTerminationFuture != null) {
            serviceTerminationFuture.complete(null);
        }
    }

    /**
     * Holds back the next leadership re-validation performed while forwarding the result, so a
     * revocation can be injected before {@link #releaseDelayedForwardingCheck} is called. Must be
     * called after {@link #grantLeadership()} so it only affects the forwarding check.
     */
    void armDelayedForwardingCheck() {
        armedForwardingCheck = leaderElection.armForwardingCheck();
    }

    /** Releases the held forwarding check, reporting that leadership has been lost. */
    void releaseDelayedForwardingCheck() {
        if (armedForwardingCheck != null) {
            armedForwardingCheck.complete(false);
            armedForwardingCheck = null;
        }
    }

    CompletableFuture<Void> closeRunnerAsync() {
        return runner.closeAsync();
    }

    JobStatus getResultJobStatus() throws Exception {
        return runner.getResultFuture()
                .get()
                .getExecutionGraphInfo()
                .getArchivedExecutionGraph()
                .getState();
    }

    @Override
    public void close() throws Exception {
        releaseDelayedForwardingCheck();
        CompletableFuture<Void> serviceTerminationFuture;
        while ((serviceTerminationFuture = serviceTerminationFutures.poll()) != null) {
            serviceTerminationFuture.complete(null);
        }
        runner.closeAsync().get();
        leaderElection.close();
    }

    private ExecutionGraphInfo createExecutionGraphInfo(JobStatus jobStatus) {
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        jobId, jobName, jobStatus, jobType, null, null, 1L));
    }

    /**
     * {@link TestingLeaderElection} whose next {@link #hasLeadershipAsync} call can be held back,
     * to model a revocation racing with the result-forwarding leadership check.
     */
    private static final class DelayableLeaderElection extends TestingLeaderElection {
        private final AtomicReference<CompletableFuture<Boolean>> nextForwardingCheck =
                new AtomicReference<>();

        @Override
        public synchronized CompletableFuture<Boolean> hasLeadershipAsync(UUID leaderSessionId) {
            final CompletableFuture<Boolean> delayed = nextForwardingCheck.getAndSet(null);
            return delayed != null ? delayed : super.hasLeadershipAsync(leaderSessionId);
        }

        CompletableFuture<Boolean> armForwardingCheck() {
            final CompletableFuture<Boolean> delayed = new CompletableFuture<>();
            nextForwardingCheck.set(delayed);
            return delayed;
        }
    }
}
