/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.lifecycle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommand;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestCommandAckEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.SerializedThrowable;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FAIL;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.ALL_SUBTASKS;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.SINGLE_SUBTASK;
import static org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue.TestEventHandler.TestEventNextAction.CONTINUE;
import static org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue.TestEventHandler.TestEventNextAction.STOP;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.Assert.fail;

/**
 * A helper class to control {@link TestJobWithDescription} execution using {@link
 * TestCommandDispatcher} and {@link TestEventQueue}.
 */
class TestJobExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TestJobExecutor.class);
    private final MiniClusterWithClientResource miniClusterResource;
    private final TestJobWithDescription testJob;
    private final JobID jobID;

    private TestJobExecutor(
            TestJobWithDescription testJob,
            JobID jobID,
            MiniClusterWithClientResource miniClusterResource) {
        this.testJob = testJob;
        this.jobID = jobID;
        this.miniClusterResource = miniClusterResource;
    }

    public static TestJobExecutor execute(
            TestJobWithDescription testJob, MiniClusterWithClientResource miniClusterResource)
            throws Exception {
        LOG.debug("submitGraph: {}", testJob.jobGraph);
        JobID job = miniClusterResource.getClusterClient().submitJob(testJob.jobGraph).get();
        waitForAllTaskRunning(miniClusterResource.getMiniCluster(), job, false);
        return new TestJobExecutor(testJob, job, miniClusterResource);
    }

    public TestJobExecutor waitForAllRunning() throws Exception {
        LOG.debug("waitForAllRunning in {}", jobID);
        waitForAllTaskRunning(miniClusterResource.getMiniCluster(), jobID, true);
        return this;
    }

    public TestJobExecutor waitForEvent(Class<? extends TestEvent> eventClass) throws Exception {
        LOG.debug("waitForEvent: {}", eventClass.getSimpleName());
        testJob.eventQueue.withHandler(
                e -> eventClass.isAssignableFrom(e.getClass()) ? STOP : CONTINUE);
        return this;
    }

    public TestJobExecutor stopWithSavepoint(TemporaryFolder folder, boolean withDrain)
            throws Exception {
        LOG.debug("stopWithSavepoint: {} (withDrain: {})", folder, withDrain);
        ClusterClient<?> client = miniClusterResource.getClusterClient();
        client.stopWithSavepoint(jobID, withDrain, folder.newFolder().toString()).get();
        return this;
    }

    public TestJobExecutor sendOperatorCommand(
            String operatorID, TestCommand command, TestCommandScope scope) {
        LOG.debug("send command: {} to {}/{}", command, operatorID, scope);
        testJob.commandQueue.dispatch(command, scope, operatorID);
        return this;
    }

    public void triggerFailover() throws Exception {
        LOG.debug("sendCommand: {}", FAIL);
        BlockingQueue<TestEvent> queue = new LinkedBlockingQueue<>();
        Consumer<TestEvent> listener = queue::add;
        testJob.eventQueue.addListener(listener);
        testJob.commandQueue.broadcast(FAIL, SINGLE_SUBTASK);
        try {
            waitForFailover(queue);
        } catch (TimeoutException e) {
            handleFailoverTimeout(e);
        } finally {
            testJob.eventQueue.removeListener(listener);
        }

        waitForAllRunning();
    }

    private void waitForFailover(BlockingQueue<TestEvent> queue) throws Exception {
        int timeoutMs = 10_000;
        Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeoutMs));

        String operatorId = null;
        int subtaskId = -1;
        int attemptNumber = -1;

        while (deadline.hasTimeLeft()) {
            TestEvent e = queue.poll(deadline.timeLeft().toMillis(), MILLISECONDS);
            if (e instanceof TestCommandAckEvent) {
                TestCommandAckEvent ack = (TestCommandAckEvent) e;
                if (ack.getCommand() == FAIL) {
                    operatorId = ack.operatorId;
                    subtaskId = ack.subtaskIndex;
                    attemptNumber = ack.getAttemptNumber();
                }
            } else if (e instanceof OperatorStartedEvent && operatorId != null) {
                OperatorStartedEvent started = (OperatorStartedEvent) e;
                if (started.operatorId.equals(operatorId)
                        && started.subtaskIndex == subtaskId
                        && started.getAttemptNumber() >= attemptNumber) {
                    return;
                }
            }
        }
        throw new TimeoutException("No subtask restarted in " + timeoutMs + "ms");
    }

    private void handleFailoverTimeout(TimeoutException e) throws Exception {
        String message =
                String.format(
                        "Unable to failover the job: %s; job status: %s",
                        e.getMessage(),
                        miniClusterResource.getClusterClient().getJobStatus(jobID).get());
        Optional<SerializedThrowable> throwable =
                miniClusterResource
                        .getClusterClient()
                        .requestJobResult(jobID)
                        .get()
                        .getSerializedThrowable();
        if (throwable.isPresent()) {
            throw new RuntimeException(message, throwable.get());
        } else {
            throw new RuntimeException(message);
        }
    }

    public TestJobExecutor sendBroadcastCommand(TestCommand command, TestCommandScope scope) {
        LOG.debug("sendCommand: {}", command);
        testJob.commandQueue.broadcast(command, scope);
        return this;
    }

    public TestJobExecutor waitForTermination() throws Exception {
        LOG.debug("waitForTermination");
        while (!miniClusterResource
                .getClusterClient()
                .getJobStatus(jobID)
                .get()
                .isGloballyTerminalState()) {
            Thread.sleep(1);
        }
        return this;
    }

    public TestJobExecutor assertFinishedSuccessfully() throws Exception {
        LOG.debug("assertFinishedSuccessfully");
        JobStatus jobStatus = miniClusterResource.getClusterClient().getJobStatus(jobID).get();
        if (!jobStatus.equals(FINISHED)) {
            String message = String.format("Job didn't finish successfully, status: %s", jobStatus);
            Optional<SerializedThrowable> throwable =
                    miniClusterResource
                            .getClusterClient()
                            .requestJobResult(jobID)
                            .get()
                            .getSerializedThrowable();
            if (throwable.isPresent()) {
                throw new AssertionError(message, throwable.get());
            } else {
                fail(message);
            }
        }
        return this;
    }

    public TestJobExecutor waitForSubtasksToFinish(JobVertexID id, TestCommandScope scope)
            throws Exception {
        LOG.debug("waitForSubtasksToFinish vertex {}, all subtasks: {}", id, scope);
        CommonTestUtils.waitForSubtasksToFinish(
                miniClusterResource.getMiniCluster(), jobID, id, scope == ALL_SUBTASKS);
        return this;
    }
}
