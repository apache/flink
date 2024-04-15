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

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** This tests verifies the checking logic of {@link VertexFinishedStateChecker}. */
class VertexFinishedStateCheckerTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testRestoringPartiallyFinishedChainsFailsWithoutUidHash() throws Exception {
        // If useUidHash is set to false, the operator states would still be keyed with the
        // generated ID, which simulates the case of restoring a checkpoint taken after jobs
        // started. The checker should still be able to access the stored state correctly, otherwise
        // it would mark op1 as running and pass the check wrongly.
        testRestoringPartiallyFinishedChainsFails(false);
    }

    @Test
    void testRestoringPartiallyFinishedChainsFailsWithUidHash() throws Exception {
        testRestoringPartiallyFinishedChainsFails(true);
    }

    private void testRestoringPartiallyFinishedChainsFails(boolean useUidHash) throws Exception {
        final JobVertexID jobVertexID1 = new JobVertexID();
        final JobVertexID jobVertexID2 = new JobVertexID();
        // The op1 has uidHash set.
        OperatorIDPair op1 = OperatorIDPair.of(new OperatorID(), new OperatorID());
        OperatorIDPair op2 = OperatorIDPair.generatedIDOnly(new OperatorID());
        OperatorIDPair op3 = OperatorIDPair.generatedIDOnly(new OperatorID());

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID2, 1, 1, singletonList(op3), true)
                        .addJobVertex(jobVertexID1, 1, 1, Arrays.asList(op1, op2), true)
                        .build(EXECUTOR_EXTENSION.getExecutor());

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        operatorStates.put(
                useUidHash ? op1.getUserDefinedOperatorID().get() : op1.getGeneratedOperatorID(),
                new FullyFinishedOperatorState(op1.getGeneratedOperatorID(), 1, 1));
        operatorStates.put(
                op2.getGeneratedOperatorID(),
                new OperatorState(op2.getGeneratedOperatorID(), 1, 1));

        Set<ExecutionJobVertex> vertices = new HashSet<>();
        vertices.add(graph.getJobVertex(jobVertexID1));
        VertexFinishedStateChecker finishedStateChecker =
                new VertexFinishedStateChecker(vertices, operatorStates);

        assertThatThrownBy(finishedStateChecker::validateOperatorsFinishedState)
                .hasMessage(
                        "Can not restore vertex "
                                + "anon("
                                + jobVertexID1
                                + ")"
                                + " which contain mixed operator finished state: [ALL_RUNNING, FULLY_FINISHED]")
                .isInstanceOf(FlinkRuntimeException.class);
    }

    @Test
    void testAddingRunningOperatorBeforeFinishedOneFails() throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedStateChecker.VertexFinishedState.ALL_RUNNING,
                jobVertexID2,
                "vert2",
                VertexFinishedStateChecker.VertexFinishedState.FULLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with fully finished vertices"
                        + " predeceased with the ones not fully finished. Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a predecessor not fully finished");
    }

    @Test
    void testAddingPartiallyFinishedOperatorBeforeFinishedOneFails() throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedStateChecker.VertexFinishedState.PARTIALLY_FINISHED,
                jobVertexID2,
                "vert2",
                VertexFinishedStateChecker.VertexFinishedState.FULLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with fully finished vertices"
                        + " predeceased with the ones not fully finished. Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a predecessor not fully finished");
    }

    @Test
    void testAddingAllRunningOperatorBeforePartiallyFinishedOneWithAllToAllFails()
            throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedStateChecker.VertexFinishedState.ALL_RUNNING,
                jobVertexID2,
                "vert2",
                VertexFinishedStateChecker.VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with running or partially finished ones and connected via the ALL_TO_ALL edges. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a all running predecessor");
    }

    @Test
    void testAddingPartiallyFinishedOperatorBeforePartiallyFinishedOneWithAllToAllFails()
            throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedStateChecker.VertexFinishedState.PARTIALLY_FINISHED,
                jobVertexID2,
                "vert2",
                VertexFinishedStateChecker.VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with running or partially finished ones and connected via the ALL_TO_ALL edges. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a partially finished predecessor");
    }

    @Test
    void
            testAddingPartiallyFinishedOperatorBeforePartiallyFinishedOneWithPointwiseAndAllToAllFails()
                    throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedStateChecker.VertexFinishedState.PARTIALLY_FINISHED,
                jobVertexID2,
                "vert2",
                VertexFinishedStateChecker.VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {
                    DistributionPattern.POINTWISE, DistributionPattern.ALL_TO_ALL
                },
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with running or partially finished ones and connected via the ALL_TO_ALL edges. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a partially finished predecessor");
    }

    @Test
    void testAddingAllRunningOperatorBeforePartiallyFinishedOneFails() throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedStateChecker.VertexFinishedState.ALL_RUNNING,
                jobVertexID2,
                "vert2",
                VertexFinishedStateChecker.VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.POINTWISE},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with all running ones. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a all running predecessor");
    }

    private void testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
            JobVertexID firstVertexId,
            String firstVertexName,
            VertexFinishedStateChecker.VertexFinishedState firstOperatorFinishedState,
            JobVertexID secondVertexId,
            String secondVertexName,
            VertexFinishedStateChecker.VertexFinishedState secondOperatorFinishedState,
            DistributionPattern[] distributionPatterns,
            Class<? extends Throwable> expectedExceptionalClass,
            String expectedMessage)
            throws Exception {
        OperatorIDPair op1 = OperatorIDPair.generatedIDOnly(new OperatorID());
        OperatorIDPair op2 = OperatorIDPair.generatedIDOnly(new OperatorID());
        JobVertex vertex1 = new JobVertex(firstVertexName, firstVertexId, singletonList(op1));
        JobVertex vertex2 = new JobVertex(secondVertexName, secondVertexId, singletonList(op2));
        vertex1.setInvokableClass(NoOpInvokable.class);
        vertex2.setInvokableClass(NoOpInvokable.class);

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(vertex1, true)
                        .addJobVertex(vertex2, false)
                        .setDistributionPattern(distributionPatterns[0])
                        .build(EXECUTOR_EXTENSION.getExecutor());

        // Adds the additional edges
        for (int i = 1; i < distributionPatterns.length; ++i) {
            vertex2.connectNewDataSetAsInput(
                    vertex1, distributionPatterns[i], ResultPartitionType.PIPELINED);
        }

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        operatorStates.put(
                op1.getGeneratedOperatorID(),
                createOperatorState(op1.getGeneratedOperatorID(), firstOperatorFinishedState));
        operatorStates.put(
                op2.getGeneratedOperatorID(),
                createOperatorState(op2.getGeneratedOperatorID(), secondOperatorFinishedState));

        Set<ExecutionJobVertex> vertices = new HashSet<>();
        vertices.add(graph.getJobVertex(vertex1.getID()));
        vertices.add(graph.getJobVertex(vertex2.getID()));
        VertexFinishedStateChecker finishedStateChecker =
                new VertexFinishedStateChecker(vertices, operatorStates);

        assertThatThrownBy(finishedStateChecker::validateOperatorsFinishedState)
                .hasMessage(expectedMessage)
                .isInstanceOf(expectedExceptionalClass);
    }

    private OperatorState createOperatorState(
            OperatorID operatorId, VertexFinishedStateChecker.VertexFinishedState finishedState) {
        switch (finishedState) {
            case ALL_RUNNING:
                return new OperatorState(operatorId, 2, 2);
            case PARTIALLY_FINISHED:
                OperatorState operatorState = new OperatorState(operatorId, 2, 2);
                operatorState.putState(0, FinishedOperatorSubtaskState.INSTANCE);
                return operatorState;
            case FULLY_FINISHED:
                return new FullyFinishedOperatorState(operatorId, 2, 2);
            default:
                throw new UnsupportedOperationException(
                        "Not supported finished state: " + finishedState);
        }
    }
}
