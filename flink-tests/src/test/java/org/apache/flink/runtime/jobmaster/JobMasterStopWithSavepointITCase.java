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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTest.NoOpStreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * ITCases testing the stop with savepoint functionality. This includes checking both SUSPEND and
 * TERMINATE.
 */
public class JobMasterStopWithSavepointITCase extends AbstractTestBase {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final long CHECKPOINT_INTERVAL = 10;
    private static final int PARALLELISM = 2;

    private static OneShotLatch finishingLatch;

    private static CountDownLatch invokeLatch;

    private static CountDownLatch numberOfRestarts;
    private static final AtomicLong syncSavepointId = new AtomicLong();
    private static volatile CountDownLatch checkpointsToWaitFor;

    private Path savepointDirectory;
    private MiniClusterClient clusterClient;

    private JobGraph jobGraph;

    @Test
    public void suspendWithSavepointWithoutComplicationsShouldSucceedAndLeadJobToFinished()
            throws Exception {
        stopWithSavepointNormalExecutionHelper(false);
    }

    @Test
    public void terminateWithSavepointWithoutComplicationsShouldSucceedAndLeadJobToFinished()
            throws Exception {
        stopWithSavepointNormalExecutionHelper(true);
    }

    private void stopWithSavepointNormalExecutionHelper(final boolean terminate) throws Exception {
        setUpJobGraph(NoOpBlockingStreamTask.class, RestartStrategies.noRestart());

        final CompletableFuture<String> savepointLocationFuture = stopWithSavepoint(terminate);

        assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));

        finishingLatch.trigger();

        final String savepointLocation = savepointLocationFuture.get();
        assertThat(getJobStatus(), equalTo(JobStatus.FINISHED));

        final List<Path> savepoints;
        try (Stream<Path> savepointFiles = Files.list(savepointDirectory)) {
            savepoints = savepointFiles.map(Path::getFileName).collect(Collectors.toList());
        }
        assertThat(savepoints, hasItem(Paths.get(savepointLocation).getFileName()));
    }

    @Test
    public void throwingExceptionOnCallbackWithNoRestartsShouldFailTheSuspend() throws Exception {
        throwingExceptionOnCallbackWithoutRestartsHelper(false);
    }

    @Test
    public void throwingExceptionOnCallbackWithNoRestartsShouldFailTheTerminate() throws Exception {
        throwingExceptionOnCallbackWithoutRestartsHelper(true);
    }

    private void throwingExceptionOnCallbackWithoutRestartsHelper(final boolean terminate)
            throws Exception {
        setUpJobGraph(ExceptionOnCallbackStreamTask.class, RestartStrategies.noRestart());

        assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));

        try {
            stopWithSavepoint(terminate).get();
            fail();
        } catch (Exception e) {
        }

        // verifying that we actually received a synchronous checkpoint
        assertTrue(syncSavepointId.get() > 0);
        assertThat(
                getJobStatus(), either(equalTo(JobStatus.FAILED)).or(equalTo(JobStatus.FAILING)));
    }

    @Test
    public void throwingExceptionOnCallbackWithRestartsShouldSimplyRestartInSuspend()
            throws Exception {
        throwingExceptionOnCallbackWithRestartsHelper(false);
    }

    @Test
    public void throwingExceptionOnCallbackWithRestartsShouldSimplyRestartInTerminate()
            throws Exception {
        throwingExceptionOnCallbackWithRestartsHelper(true);
    }

    private void throwingExceptionOnCallbackWithRestartsHelper(final boolean terminate)
            throws Exception {
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(15));
        final int numberOfCheckpointsToExpect = 10;

        numberOfRestarts = new CountDownLatch(2);
        checkpointsToWaitFor = new CountDownLatch(numberOfCheckpointsToExpect);

        setUpJobGraph(
                ExceptionOnCallbackStreamTask.class,
                RestartStrategies.fixedDelayRestart(15, Time.milliseconds(10)));
        assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));
        try {
            stopWithSavepoint(terminate).get(50, TimeUnit.MILLISECONDS);
            fail();
        } catch (Exception e) {
            // expected
        }

        // wait until we restart at least 2 times and until we see at least 10 checkpoints.
        assertTrue(numberOfRestarts.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(
                checkpointsToWaitFor.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

        // verifying that we actually received a synchronous checkpoint
        assertTrue(syncSavepointId.get() > 0);

        assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));

        // make sure that we saw the synchronous savepoint and
        // that after that we saw more checkpoints due to restarts.
        final long syncSavepoint = syncSavepointId.get();
        assertTrue(syncSavepoint > 0 && syncSavepoint < numberOfCheckpointsToExpect);

        clusterClient.cancel(jobGraph.getJobID()).get();
        assertThat(
                getJobStatus(),
                either(equalTo(JobStatus.CANCELLING)).or(equalTo(JobStatus.CANCELED)));
    }

    @Test
    public void testRestartCheckpointCoordinatorIfStopWithSavepointFails() throws Exception {
        setUpJobGraph(CheckpointCountingTask.class, RestartStrategies.noRestart());

        try {
            Files.setPosixFilePermissions(savepointDirectory, Collections.emptySet());
        } catch (IOException e) {
            Assume.assumeNoException(e);
        }

        try {
            stopWithSavepoint(true).get();
            fail();
        } catch (Exception e) {
            Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            if (!checkpointExceptionOptional.isPresent()) {
                throw e;
            }
            String exceptionMessage = checkpointExceptionOptional.get().getMessage();
            assertTrue(
                    "Stop with savepoint failed because of another cause " + exceptionMessage,
                    exceptionMessage.contains(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE.message()));
        }

        final JobStatus jobStatus =
                clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
        assertThat(jobStatus, equalTo(JobStatus.RUNNING));
        // assert that checkpoints are continued to be triggered
        checkpointsToWaitFor = new CountDownLatch(1);
        assertTrue(checkpointsToWaitFor.await(60L, TimeUnit.SECONDS));
    }

    private CompletableFuture<String> stopWithSavepoint(boolean terminate) {
        return miniClusterResource
                .getMiniCluster()
                .stopWithSavepoint(
                        jobGraph.getJobID(),
                        savepointDirectory.toAbsolutePath().toString(),
                        terminate);
    }

    private JobStatus getJobStatus() throws InterruptedException, ExecutionException {
        return clusterClient.getJobStatus(jobGraph.getJobID()).get();
    }

    private void setUpJobGraph(
            final Class<? extends AbstractInvokable> invokable,
            final RestartStrategies.RestartStrategyConfiguration restartStrategy)
            throws Exception {

        finishingLatch = new OneShotLatch();

        invokeLatch = new CountDownLatch(PARALLELISM);

        numberOfRestarts = new CountDownLatch(2);
        checkpointsToWaitFor = new CountDownLatch(10);

        syncSavepointId.set(-1);

        savepointDirectory = temporaryFolder.newFolder().toPath();

        Assume.assumeTrue(
                "ClusterClient is not an instance of MiniClusterClient",
                miniClusterResource.getClusterClient() instanceof MiniClusterClient);

        clusterClient = (MiniClusterClient) miniClusterResource.getClusterClient();

        final ExecutionConfig config = new ExecutionConfig();
        config.setRestartStrategy(restartStrategy);

        final JobVertex vertex = new JobVertex("testVertex");
        vertex.setInvokableClass(invokable);
        vertex.setParallelism(PARALLELISM);

        final JobCheckpointingSettings jobCheckpointingSettings =
                new JobCheckpointingSettings(
                        new CheckpointCoordinatorConfiguration(
                                CHECKPOINT_INTERVAL,
                                60_000,
                                10,
                                1,
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                                true,
                                false,
                                false,
                                0),
                        null);

        jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setExecutionConfig(config)
                        .addJobVertex(vertex)
                        .setJobCheckpointingSettings(jobCheckpointingSettings)
                        .build();

        clusterClient.submitJob(jobGraph).get();
        assertTrue(invokeLatch.await(60, TimeUnit.SECONDS));
        waitForJob();
    }

    private void waitForJob() throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5));
        JobID jobID = jobGraph.getJobID();
        CommonTestUtils.waitForAllTaskRunning(
                () ->
                        miniClusterResource
                                .getMiniCluster()
                                .getExecutionGraph(jobID)
                                .get(60, TimeUnit.SECONDS),
                deadline);
    }

    /**
     * A {@link StreamTask} which throws an exception in the {@code notifyCheckpointComplete()} for
     * subtask 0.
     */
    public static class ExceptionOnCallbackStreamTask extends CheckpointCountingTask {

        private long synchronousSavepointId = Long.MIN_VALUE;

        public ExceptionOnCallbackStreamTask(final Environment environment) throws Exception {
            super(environment);
        }

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            final long taskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
            if (taskIndex == 0) {
                numberOfRestarts.countDown();
            }
            super.processInput(controller);
        }

        @Override
        public Future<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            final long checkpointId = checkpointMetaData.getCheckpointId();
            final CheckpointType checkpointType = checkpointOptions.getCheckpointType();

            if (checkpointType.isSynchronous()) {
                synchronousSavepointId = checkpointId;
                syncSavepointId.compareAndSet(-1, synchronousSavepointId);
            }

            return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            final long taskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
            if (checkpointId == synchronousSavepointId && taskIndex == 0) {
                throw new RuntimeException("Expected Exception");
            }

            return super.notifyCheckpointCompleteAsync(checkpointId);
        }

        @Override
        public Future<Void> notifyCheckpointAbortAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected void finishTask() {
            mailboxProcessor.allActionsCompleted();
        }
    }

    /** A {@link StreamTask} that simply waits to be terminated normally. */
    public static class NoOpBlockingStreamTask extends NoOpStreamTask {

        private transient MailboxDefaultAction.Suspension suspension;

        public NoOpBlockingStreamTask(final Environment environment) throws Exception {
            super(environment);
        }

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            invokeLatch.countDown();
            if (suspension == null) {
                suspension = controller.suspendDefaultAction();
            } else {
                controller.allActionsCompleted();
            }
        }

        @Override
        public void finishTask() throws Exception {
            finishingLatch.await();
            if (suspension != null) {
                suspension.resume();
            }
        }
    }

    /**
     * A {@link StreamTask} that simply calls {@link CountDownLatch#countDown()} when invoking
     * {@link #triggerCheckpointAsync}.
     */
    public static class CheckpointCountingTask extends NoOpStreamTask {

        private transient MailboxDefaultAction.Suspension suspension;

        public CheckpointCountingTask(final Environment environment) throws Exception {
            super(environment);
        }

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            invokeLatch.countDown();
            if (suspension == null) {
                suspension = controller.suspendDefaultAction();
            } else {
                controller.allActionsCompleted();
            }
        }

        @Override
        protected void cancelTask() throws Exception {
            super.cancelTask();
            if (suspension != null) {
                suspension.resume();
            }
        }

        @Override
        public Future<Boolean> triggerCheckpointAsync(
                final CheckpointMetaData checkpointMetaData,
                final CheckpointOptions checkpointOptions) {
            final long taskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
            if (taskIndex == 0) {
                checkpointsToWaitFor.countDown();
            }

            return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
        }
    }
}
