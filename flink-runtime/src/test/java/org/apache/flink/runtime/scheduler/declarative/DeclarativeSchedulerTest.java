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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.flip1.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.createSlotOffersForResourceRequirements;
import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.offerSlots;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link DeclarativeScheduler}. */
public class DeclarativeSchedulerTest extends TestLogger {

    private static final int PARALLELISM = 4;
    private static final JobVertex JOB_VERTEX;

    static {
        JOB_VERTEX = new JobVertex("v1");
        JOB_VERTEX.setParallelism(PARALLELISM);
        JOB_VERTEX.setInvokableClass(AbstractInvokable.class);
    }

    private final ComponentMainThreadExecutor mainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forMainThread();

    @Test
    public void testInitialState() throws Exception {
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor).build();

        assertThat(scheduler.getState(), instanceOf(Created.class));
    }

    @Test
    public void testIsState() throws Exception {
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor).build();

        final State state = scheduler.getState();

        assertThat(scheduler.isState(state), is(true));
        assertThat(scheduler.isState(new DummyState()), is(false));
    }

    @Test
    public void testRunIfState() throws Exception {
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor).build();

        AtomicBoolean ran = new AtomicBoolean(false);
        scheduler.runIfState(scheduler.getState(), () -> ran.set(true));
        assertThat(ran.get(), is(true));
    }

    @Test
    public void testRunIfStateWithStateMismatch() throws Exception {
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor).build();

        AtomicBoolean ran = new AtomicBoolean(false);
        scheduler.runIfState(new DummyState(), () -> ran.set(true));
        assertThat(ran.get(), is(false));
    }

    @Test
    public void testHasEnoughResources() throws Exception {
        final JobGraph jobGraph = getJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Time.minutes(10),
                        Time.minutes(10));

        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        final ResourceCounter resourceRequirement =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);

        assertThat(scheduler.hasEnoughResources(resourceRequirement), is(false));

        offerSlots(
                declarativeSlotPool, createSlotOffersForResourceRequirements(resourceRequirement));

        assertThat(scheduler.hasEnoughResources(resourceRequirement), is(true));
    }

    @Test
    public void testExecutionGraphGenerationWithAvailableResources() throws Exception {
        final JobGraph jobGraph = getJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Time.minutes(10),
                        Time.minutes(10));

        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        final int numAvailableSlots = 1;

        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, numAvailableSlots)));

        final ExecutionGraph executionGraph =
                scheduler.createExecutionGraphWithAvailableResources();

        assertThat(
                executionGraph.getJobVertex(JOB_VERTEX.getID()).getParallelism(),
                is(numAvailableSlots));
    }

    @Test
    public void testStartScheduling() throws Exception {
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor).build();

        scheduler.startScheduling();

        assertThat(scheduler.getState(), instanceOf(WaitingForResources.class));
    }

    @Test
    public void testStartSchedulingSetsResourceRequirements() throws Exception {
        final JobGraph jobGraph = getJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Time.minutes(10),
                        Time.minutes(10));

        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        assertThat(
                declarativeSlotPool.getResourceRequirements(),
                contains(ResourceRequirement.create(ResourceProfile.UNKNOWN, PARALLELISM)));
    }

    /** Tests that the listener for new slots is properly set up. */
    @Test
    public void testResourceAcquisitionTriggersExecution() throws Exception {
        final JobGraph jobGraph = getJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Time.minutes(10),
                        Time.minutes(10));

        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, PARALLELISM)));

        assertThat(scheduler.getState(), instanceOf(Executing.class));
    }

    @Test
    public void testGoToFinished() throws Exception {
        final AtomicReference<JobStatus> jobStatusUpdate = new AtomicReference<>();
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor)
                        .setJobStatusListener(
                                (jobId, newJobStatus, timestamp, error) ->
                                        jobStatusUpdate.set(newJobStatus))
                        .build();

        final ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(JobStatus.FAILED).build();

        scheduler.goToFinished(archivedExecutionGraph);

        assertThat(scheduler.getState(), instanceOf(Finished.class));
        assertThat(jobStatusUpdate.get(), is(archivedExecutionGraph.getState()));
    }

    @Test
    public void testHowToHandleFailureRejectedByStrategy() throws Exception {
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor)
                        .setRestartBackoffTimeStrategy(NoRestartBackoffTimeStrategy.INSTANCE)
                        .build();

        assertThat(scheduler.howToHandleFailure(new Exception("test")).canRestart(), is(false));
    }

    @Test
    public void testHowToHandleFailureAllowedByStrategy() throws Exception {
        final TestRestartBackoffTimeStrategy restartBackoffTimeStrategy =
                new TestRestartBackoffTimeStrategy(true, 1234);

        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor)
                        .setRestartBackoffTimeStrategy(restartBackoffTimeStrategy)
                        .build();

        final Executing.FailureResult failureResult =
                scheduler.howToHandleFailure(new Exception("test"));

        assertThat(failureResult.canRestart(), is(true));
        assertThat(
                failureResult.getBackoffTime().toMillis(),
                is(restartBackoffTimeStrategy.getBackoffTime()));
    }

    @Test
    public void testHowToHandleFailureUnrecoverableFailure() throws Exception {
        final DeclarativeScheduler scheduler =
                new DeclarativeSchedulerBuilder(getJobGraph(), mainThreadExecutor).build();

        assertThat(
                scheduler
                        .howToHandleFailure(new SuppressRestartsException(new Exception("test")))
                        .canRestart(),
                is(false));
    }

    private static JobGraph getJobGraph() {
        final JobGraph jobGraph = new JobGraph(JOB_VERTEX);
        jobGraph.setJobType(JobType.STREAMING);
        return jobGraph;
    }

    private static class DummyState implements State {

        @Override
        public void cancel() {}

        @Override
        public void suspend(Throwable cause) {}

        @Override
        public JobStatus getJobStatus() {
            return null;
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return null;
        }

        @Override
        public void handleGlobalFailure(Throwable cause) {}

        @Override
        public Logger getLogger() {
            return null;
        }
    }
}
