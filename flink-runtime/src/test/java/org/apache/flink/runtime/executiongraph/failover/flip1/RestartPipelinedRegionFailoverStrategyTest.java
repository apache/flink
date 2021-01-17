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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionConnectionException;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests the failure handling logic of the {@link RestartPipelinedRegionFailoverStrategy}. */
public class RestartPipelinedRegionFailoverStrategyTest extends TestLogger {

    /**
     * Tests for scenes that a task fails for its own error, in which case the region containing the
     * failed task and its consumer regions should be restarted.
     *
     * <pre>
     *     (v1) -+-> (v4)
     *           x
     *     (v2) -+-> (v5)
     *
     *     (v3) -+-> (v6)
     *
     *           ^
     *           |
     *       (blocking)
     * </pre>
     *
     * Each vertex is in an individual region.
     */
    @Test
    public void testRegionFailoverForRegionInternalErrors() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex(ExecutionState.SCHEDULED);
        TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex(ExecutionState.RUNNING);

        topology.connect(v1, v4, ResultPartitionType.BLOCKING);
        topology.connect(v1, v5, ResultPartitionType.BLOCKING);
        topology.connect(v2, v4, ResultPartitionType.BLOCKING);
        topology.connect(v2, v5, ResultPartitionType.BLOCKING);
        topology.connect(v3, v6, ResultPartitionType.BLOCKING);

        RestartPipelinedRegionFailoverStrategy strategy =
                new RestartPipelinedRegionFailoverStrategy(topology);

        verifyThatFailedExecution(strategy, v1).restarts(v1, v4, v5);
        verifyThatFailedExecution(strategy, v2).restarts(v2, v4, v5);
        verifyThatFailedExecution(strategy, v3).restarts(v3, v6);
        verifyThatFailedExecution(strategy, v4).restarts(v4);
        verifyThatFailedExecution(strategy, v5).restarts(v5);
        verifyThatFailedExecution(strategy, v6).restarts(v6);
    }

    /**
     * Tests for scenes that a task fails for data consumption error, in which case the region
     * containing the failed task, the region containing the unavailable result partition and all
     * their consumer regions should be restarted.
     *
     * <pre>
     *     (v1) -+-> (v4)
     *           x
     *     (v2) -+-> (v5)
     *
     *     (v3) -+-> (v6)
     *
     *           ^
     *           |
     *       (blocking)
     * </pre>
     *
     * Each vertex is in an individual region.
     */
    @Test
    public void testRegionFailoverForDataConsumptionErrors() throws Exception {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();

        TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex(ExecutionState.RUNNING);
        TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex(ExecutionState.RUNNING);
        TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex(ExecutionState.RUNNING);

        topology.connect(v1, v4, ResultPartitionType.BLOCKING);
        topology.connect(v1, v5, ResultPartitionType.BLOCKING);
        topology.connect(v2, v4, ResultPartitionType.BLOCKING);
        topology.connect(v2, v5, ResultPartitionType.BLOCKING);
        topology.connect(v3, v6, ResultPartitionType.BLOCKING);

        RestartPipelinedRegionFailoverStrategy strategy =
                new RestartPipelinedRegionFailoverStrategy(topology);

        Iterator<TestingSchedulingResultPartition> v4InputEdgeIterator =
                v4.getConsumedResults().iterator();
        TestingSchedulingResultPartition v1out = v4InputEdgeIterator.next();
        verifyThatFailedExecution(strategy, v4)
                .partitionConnectionCause(v1out)
                .restarts(v1, v4, v5);
        TestingSchedulingResultPartition v2out = v4InputEdgeIterator.next();
        verifyThatFailedExecution(strategy, v4)
                .partitionConnectionCause(v2out)
                .restarts(v2, v4, v5);

        Iterator<TestingSchedulingResultPartition> v5InputEdgeIterator =
                v5.getConsumedResults().iterator();
        v1out = v5InputEdgeIterator.next();
        verifyThatFailedExecution(strategy, v5)
                .partitionConnectionCause(v1out)
                .restarts(v1, v4, v5);
        v2out = v5InputEdgeIterator.next();
        verifyThatFailedExecution(strategy, v5)
                .partitionConnectionCause(v2out)
                .restarts(v2, v4, v5);

        TestingSchedulingResultPartition v3out = v6.getConsumedResults().iterator().next();
        verifyThatFailedExecution(strategy, v6).partitionConnectionCause(v3out).restarts(v3, v6);
    }

    /**
     * Tests to verify region failover results regarding different input result partition
     * availability combinations.
     *
     * <pre>
     *     (v1) --rp1--\
     *                 (v3)
     *     (v2) --rp2--/
     *
     *             ^
     *             |
     *         (blocking)
     * </pre>
     *
     * Each vertex is in an individual region. rp1, rp2 are result partitions.
     */
    @Test
    public void testRegionFailoverForVariousResultPartitionAvailabilityCombinations()
            throws Exception {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();

        TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex(ExecutionState.RUNNING);

        topology.connect(v1, v3, ResultPartitionType.BLOCKING);
        topology.connect(v2, v3, ResultPartitionType.BLOCKING);

        TestResultPartitionAvailabilityChecker availabilityChecker =
                new TestResultPartitionAvailabilityChecker();
        RestartPipelinedRegionFailoverStrategy strategy =
                new RestartPipelinedRegionFailoverStrategy(topology, availabilityChecker);

        IntermediateResultPartitionID rp1ID = v1.getProducedResults().iterator().next().getId();
        IntermediateResultPartitionID rp2ID = v2.getProducedResults().iterator().next().getId();

        // -------------------------------------------------
        // Combination1: (rp1 == available, rp2 == available)
        // -------------------------------------------------
        availabilityChecker.failedPartitions.clear();

        verifyThatFailedExecution(strategy, v1).restarts(v1, v3);
        verifyThatFailedExecution(strategy, v2).restarts(v2, v3);
        verifyThatFailedExecution(strategy, v3).restarts(v3);

        // -------------------------------------------------
        // Combination2: (rp1 == unavailable, rp2 == available)
        // -------------------------------------------------
        availabilityChecker.failedPartitions.clear();
        availabilityChecker.markResultPartitionFailed(rp1ID);

        verifyThatFailedExecution(strategy, v1).restarts(v1, v3);
        verifyThatFailedExecution(strategy, v2).restarts(v1, v2, v3);
        verifyThatFailedExecution(strategy, v3).restarts(v1, v3);

        // -------------------------------------------------
        // Combination3: (rp1 == available, rp2 == unavailable)
        // -------------------------------------------------
        availabilityChecker.failedPartitions.clear();
        availabilityChecker.markResultPartitionFailed(rp2ID);

        verifyThatFailedExecution(strategy, v1).restarts(v1, v2, v3);
        verifyThatFailedExecution(strategy, v2).restarts(v2, v3);
        verifyThatFailedExecution(strategy, v3).restarts(v2, v3);

        // -------------------------------------------------
        // Combination4: (rp1 == unavailable, rp == unavailable)
        // -------------------------------------------------
        availabilityChecker.failedPartitions.clear();
        availabilityChecker.markResultPartitionFailed(rp1ID);
        availabilityChecker.markResultPartitionFailed(rp2ID);

        verifyThatFailedExecution(strategy, v1).restarts(v1, v2, v3);
        verifyThatFailedExecution(strategy, v2).restarts(v1, v2, v3);
        verifyThatFailedExecution(strategy, v3).restarts(v1, v2, v3);
    }

    /**
     * Tests region failover scenes for topology with multiple vertices.
     *
     * <pre>
     *     (v1) ---> (v2) --|--> (v3) ---> (v4) --|--> (v5) ---> (v6)
     *
     *           ^          ^          ^          ^          ^
     *           |          |          |          |          |
     *     (pipelined) (blocking) (pipelined) (blocking) (pipelined)
     * </pre>
     *
     * Component 1: 1,2; component 2: 3,4; component 3: 5,6
     */
    @Test
    public void testRegionFailoverForMultipleVerticesRegions() throws Exception {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();

        TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex(ExecutionState.FINISHED);
        TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex(ExecutionState.RUNNING);
        TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex(ExecutionState.RUNNING);
        TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex(ExecutionState.FAILED);
        TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex(ExecutionState.CANCELED);

        topology.connect(v1, v2, ResultPartitionType.PIPELINED);
        topology.connect(v2, v3, ResultPartitionType.BLOCKING);
        topology.connect(v3, v4, ResultPartitionType.PIPELINED);
        topology.connect(v4, v5, ResultPartitionType.BLOCKING);
        topology.connect(v5, v6, ResultPartitionType.PIPELINED);

        RestartPipelinedRegionFailoverStrategy strategy =
                new RestartPipelinedRegionFailoverStrategy(topology);

        verifyThatFailedExecution(strategy, v3).restarts(v3, v4, v5, v6);

        TestingSchedulingResultPartition v2out = v3.getConsumedResults().iterator().next();
        verifyThatFailedExecution(strategy, v3)
                .partitionConnectionCause(v2out)
                .restarts(v1, v2, v3, v4, v5, v6);
    }

    /**
     * Tests region failover does not restart vertexes which are already in initial CREATED state.
     *
     * <pre>
     *     (v1) --|--> (v2)
     *
     *            ^
     *            |
     *       (blocking)
     * </pre>
     *
     * Component 1: 1; component 2: 2
     */
    @Test
    public void testRegionFailoverDoesNotRestartCreatedExecutions() {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();

        TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex(ExecutionState.CREATED);
        TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex(ExecutionState.CREATED);

        topology.connect(v1, v2, ResultPartitionType.BLOCKING);

        FailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

        verifyThatFailedExecution(strategy, v2).restarts();
        TestingSchedulingResultPartition v1out = v2.getConsumedResults().iterator().next();
        verifyThatFailedExecution(strategy, v2).partitionConnectionCause(v1out).restarts();
    }

    /**
     * Tests approximate local recovery downstream failover .
     *
     * <pre>
     *     (v1) -----> (v2) -----> (v4)
     *      |                       ^
     *      |--------> (v3) --------|
     * </pre>
     */
    @Test
    public void testRegionFailoverForPipelinedApproximate() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex(ExecutionState.RUNNING);
        TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex(ExecutionState.RUNNING);
        TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex(ExecutionState.RUNNING);
        TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex(ExecutionState.RUNNING);

        topology.connect(v1, v2, ResultPartitionType.PIPELINED_APPROXIMATE);
        topology.connect(v1, v3, ResultPartitionType.PIPELINED_APPROXIMATE);
        topology.connect(v2, v4, ResultPartitionType.PIPELINED_APPROXIMATE);
        topology.connect(v3, v4, ResultPartitionType.PIPELINED_APPROXIMATE);

        RestartPipelinedRegionFailoverStrategy strategy =
                new RestartPipelinedRegionFailoverStrategy(topology);

        verifyThatFailedExecution(strategy, v1).restarts(v1, v2, v3, v4);
        verifyThatFailedExecution(strategy, v2).restarts(v2, v4);
        verifyThatFailedExecution(strategy, v3).restarts(v3, v4);
        verifyThatFailedExecution(strategy, v4).restarts(v4);
    }

    private static VerificationContext verifyThatFailedExecution(
            FailoverStrategy strategy, SchedulingExecutionVertex executionVertex) {
        return new VerificationContext(strategy, executionVertex);
    }

    private static class VerificationContext {
        private final FailoverStrategy strategy;
        private final SchedulingExecutionVertex executionVertex;
        private Throwable cause = new Exception("Test failure");

        private VerificationContext(
                FailoverStrategy strategy, SchedulingExecutionVertex executionVertex) {
            this.strategy = strategy;
            this.executionVertex = executionVertex;
        }

        private VerificationContext partitionConnectionCause(
                SchedulingResultPartition failedProducer) {
            return cause(
                    new PartitionConnectionException(
                            new ResultPartitionID(failedProducer.getId(), new ExecutionAttemptID()),
                            new Exception("Test failure")));
        }

        private VerificationContext cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        private void restarts(SchedulingExecutionVertex... expectedResult) {
            assertThat(
                    strategy.getTasksNeedingRestart(executionVertex.getId(), cause),
                    containsInAnyOrder(
                            Stream.of(expectedResult)
                                    .map(SchedulingExecutionVertex::getId)
                                    .toArray()));
        }
    }

    private static class TestResultPartitionAvailabilityChecker
            implements ResultPartitionAvailabilityChecker {

        private final HashSet<IntermediateResultPartitionID> failedPartitions;

        public TestResultPartitionAvailabilityChecker() {
            this.failedPartitions = new HashSet<>();
        }

        @Override
        public boolean isAvailable(IntermediateResultPartitionID resultPartitionID) {
            return !failedPartitions.contains(resultPartitionID);
        }

        public void markResultPartitionFailed(IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.add(resultPartitionID);
        }

        public void removeResultPartitionFromFailedState(
                IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.remove(resultPartitionID);
        }
    }
}
