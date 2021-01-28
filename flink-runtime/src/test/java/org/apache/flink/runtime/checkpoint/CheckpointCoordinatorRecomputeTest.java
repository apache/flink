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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CheckpointCoordinatorRecomputeTest {

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    public void testRecomputeWithoutNewTasksToTrigger() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();
        ExecutionGraph graph = createTestGraph(DistributionPattern.ALL_TO_ALL, gateway);
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        Iterator<ExecutionJobVertex> verticesIterator = graph.getVerticesTopologically().iterator();
        ExecutionJobVertex source = verticesIterator.next();

        checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, checkpointCoordinator.getPendingCheckpoints().size());
        PendingCheckpoint pendingCheckpoint =
                checkpointCoordinator
                        .getPendingCheckpoints()
                        .entrySet()
                        .iterator()
                        .next()
                        .getValue();

        assertThat(
                pendingCheckpoint.getCheckpointBrief().getTasksToTrigger(),
                containsInAnyOrder(
                        Arrays.asList(
                                sameInstance(
                                        source.getTaskVertices()[0].getCurrentExecutionAttempt()),
                                sameInstance(
                                        source.getTaskVertices()[1].getCurrentExecutionAttempt()),
                                sameInstance(
                                        source.getTaskVertices()[2]
                                                .getCurrentExecutionAttempt()))));
        for (ExecutionVertex vertex : source.getTaskVertices()) {
            ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertEquals(
                    pendingCheckpoint.getCheckpointID(),
                    gateway.getOnlyTriggeredCheckpoint(attemptID).checkpointId);
        }

        gateway.resetCount();
        source.getTaskVertices()[0].getCurrentExecutionAttempt().markFinished();
        source.getTaskVertices()[1].getCurrentExecutionAttempt().markFinished();
        checkpointCoordinator.onTaskFinished(
                source.getTaskVertices()[0].getCurrentExecutionAttempt().getAttemptId());
        checkpointCoordinator.onTaskFinished(
                source.getTaskVertices()[1].getCurrentExecutionAttempt().getAttemptId());
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(
                pendingCheckpoint.getCheckpointBrief().getTasksToTrigger(),
                containsInAnyOrder(
                        Collections.singletonList(
                                sameInstance(
                                        source.getTaskVertices()[2]
                                                .getCurrentExecutionAttempt()))));
        assertTrue(
                pendingCheckpoint.isAcknowledgedBy(
                        source.getTaskVertices()[0].getCurrentExecutionAttempt().getAttemptId()));
        assertTrue(
                pendingCheckpoint.isAcknowledgedBy(
                        source.getTaskVertices()[1].getCurrentExecutionAttempt().getAttemptId()));
        assertEquals(0, gateway.getTotalTriggerCount());
    }

    @Test
    public void testRecomputeWithNewTasksToTrigger() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();
        ExecutionGraph graph = createTestGraph(DistributionPattern.POINTWISE, gateway);
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        Iterator<ExecutionJobVertex> verticesIterator = graph.getVerticesTopologically().iterator();
        ExecutionJobVertex source = verticesIterator.next();
        ExecutionJobVertex sink = verticesIterator.next();

        checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, checkpointCoordinator.getPendingCheckpoints().size());
        PendingCheckpoint pendingCheckpoint =
                checkpointCoordinator
                        .getPendingCheckpoints()
                        .entrySet()
                        .iterator()
                        .next()
                        .getValue();

        assertThat(
                pendingCheckpoint.getCheckpointBrief().getTasksToTrigger(),
                containsInAnyOrder(
                        Arrays.asList(
                                sameInstance(
                                        source.getTaskVertices()[0].getCurrentExecutionAttempt()),
                                sameInstance(
                                        source.getTaskVertices()[1].getCurrentExecutionAttempt()),
                                sameInstance(
                                        source.getTaskVertices()[2]
                                                .getCurrentExecutionAttempt()))));
        for (ExecutionVertex vertex : source.getTaskVertices()) {
            ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertEquals(
                    pendingCheckpoint.getCheckpointID(),
                    gateway.getOnlyTriggeredCheckpoint(attemptID).checkpointId);
        }

        gateway.resetCount();
        source.getTaskVertices()[0].getCurrentExecutionAttempt().markFinished();
        source.getTaskVertices()[1].getCurrentExecutionAttempt().markFinished();
        checkpointCoordinator.onTaskFinished(
                source.getTaskVertices()[0].getCurrentExecutionAttempt().getAttemptId());
        checkpointCoordinator.onTaskFinished(
                source.getTaskVertices()[1].getCurrentExecutionAttempt().getAttemptId());
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(
                pendingCheckpoint.getCheckpointBrief().getTasksToTrigger(),
                containsInAnyOrder(
                        Arrays.asList(
                                sameInstance(
                                        source.getTaskVertices()[2].getCurrentExecutionAttempt()),
                                sameInstance(
                                        sink.getTaskVertices()[0].getCurrentExecutionAttempt()),
                                sameInstance(
                                        sink.getTaskVertices()[1].getCurrentExecutionAttempt()))));
        assertTrue(
                pendingCheckpoint.isAcknowledgedBy(
                        source.getTaskVertices()[0].getCurrentExecutionAttempt().getAttemptId()));
        assertTrue(
                pendingCheckpoint.isAcknowledgedBy(
                        source.getTaskVertices()[1].getCurrentExecutionAttempt().getAttemptId()));

        assertEquals(2, gateway.getTotalTriggerCount());
        for (ExecutionVertex vertex :
                Arrays.asList(sink.getTaskVertices()[0], sink.getTaskVertices()[1])) {
            ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertEquals(
                    pendingCheckpoint.getCheckpointID(),
                    gateway.getOnlyTriggeredCheckpoint(attemptID).checkpointId);
        }
    }

    @Test
    public void taskCheckpointCompletedAfterRetrigger() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();
        ExecutionGraph graph = createTestGraph(DistributionPattern.POINTWISE, gateway);
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        Iterator<ExecutionJobVertex> verticesIterator = graph.getVerticesTopologically().iterator();
        ExecutionJobVertex source = verticesIterator.next();

        checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, checkpointCoordinator.getPendingCheckpoints().size());
        PendingCheckpoint pendingCheckpoint =
                checkpointCoordinator
                        .getPendingCheckpoints()
                        .entrySet()
                        .iterator()
                        .next()
                        .getValue();

        assertThat(
                pendingCheckpoint.getCheckpointBrief().getTasksToTrigger(),
                containsInAnyOrder(
                        Arrays.asList(
                                sameInstance(
                                        source.getTaskVertices()[0].getCurrentExecutionAttempt()),
                                sameInstance(
                                        source.getTaskVertices()[1].getCurrentExecutionAttempt()),
                                sameInstance(
                                        source.getTaskVertices()[2]
                                                .getCurrentExecutionAttempt()))));
        for (ExecutionVertex vertex : source.getTaskVertices()) {
            ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertEquals(
                    pendingCheckpoint.getCheckpointID(),
                    gateway.getOnlyTriggeredCheckpoint(attemptID).checkpointId);
        }

        gateway.resetCount();
        graph.getAllExecutionVertices()
                .forEach(
                        task -> {
                            task.getCurrentExecutionAttempt().markFinished();
                            checkpointCoordinator.onTaskFinished(
                                    task.getCurrentExecutionAttempt().getAttemptId());
                        });
        manuallyTriggeredScheduledExecutor.triggerAll();

        // There should be no more tasks to trigger
        assertEquals(0, pendingCheckpoint.getCheckpointBrief().getTasksToTrigger().size());
        assertEquals(0, gateway.getTotalTriggerCount());

        // And the pending checkpoint should be finished
        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(
                1, checkpointCoordinator.getCheckpointStore().getNumberOfRetainedCheckpoints());
        assertEquals(
                pendingCheckpoint.getCheckpointID(),
                checkpointCoordinator
                        .getCheckpointStore()
                        .getAllCheckpoints()
                        .get(0)
                        .getCheckpointID());
    }

    // -------------------------------- Utilities ------------------------------------------------

    private ExecutionGraph createTestGraph(
            DistributionPattern distributionPattern, TaskManagerGateway taskManagerGateway)
            throws Exception {
        JobVertex source =
                ExecutionGraphTestUtils.createJobVertex("source", 3, NoOpInvokable.class);
        JobVertex sink = ExecutionGraphTestUtils.createJobVertex("source", 3, NoOpInvokable.class);
        sink.connectNewDataSetAsInput(source, distributionPattern, ResultPartitionType.PIPELINED);

        ExecutionGraph graph = ExecutionGraphTestUtils.createSimpleTestGraph(source, sink);
        graph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
        graph.transitionToRunning();
        graph.getAllExecutionVertices()
                .forEach(
                        task -> {
                            LogicalSlot slot =
                                    new TestingLogicalSlotBuilder()
                                            .setTaskManagerGateway(taskManagerGateway)
                                            .createTestingLogicalSlot();
                            task.getCurrentExecutionAttempt().tryAssignResource(slot);
                            task.getCurrentExecutionAttempt()
                                    .transitionState(ExecutionState.RUNNING);
                        });

        return graph;
    }

    private CheckpointCoordinator createCheckpointCoordinator(ExecutionGraph graph)
            throws Exception {
        return new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                .setExecutionGraph(graph)
                .setTimer(manuallyTriggeredScheduledExecutor)
                .build();
    }
}
