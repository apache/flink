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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.TestingOperatorCoordinator;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.SerializedValue;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Test for {@link DefaultOperatorCoordinatorHandler}. */
public class DefaultOperatorCoordinatorHandlerTest {
    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Test
    public void testRegisterAndStartNewCoordinators() throws Exception {

        final JobVertex[] jobVertices = createJobVertices(BLOCKING);
        OperatorID operatorId1 = OperatorID.fromJobVertexID(jobVertices[0].getID());
        OperatorID operatorId2 = OperatorID.fromJobVertexID(jobVertices[1].getID());

        ExecutionGraph executionGraph = createDynamicGraph(jobVertices);
        ExecutionJobVertex ejv1 = executionGraph.getJobVertex(jobVertices[0].getID());
        ExecutionJobVertex ejv2 = executionGraph.getJobVertex(jobVertices[1].getID());
        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        executionGraph.initializeJobVertex(
                ejv1, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        DefaultOperatorCoordinatorHandler handler =
                new DefaultOperatorCoordinatorHandler(executionGraph, throwable -> {});
        assertThat(handler.getCoordinatorMap().keySet(), containsInAnyOrder(operatorId1));

        executionGraph.initializeJobVertex(
                ejv2, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        handler.registerAndStartNewCoordinators(
                ejv2.getOperatorCoordinators(), executionGraph.getJobMasterMainThreadExecutor());

        assertThat(
                handler.getCoordinatorMap().keySet(), containsInAnyOrder(operatorId1, operatorId2));
    }

    private JobVertex[] createJobVertices(ResultPartitionType resultPartitionType)
            throws IOException {
        final JobVertex[] jobVertices = new JobVertex[2];
        final int parallelism = 3;
        jobVertices[0] = createNoOpVertex(parallelism);
        jobVertices[1] = createNoOpVertex(parallelism);
        jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, resultPartitionType);

        jobVertices[0].addOperatorCoordinator(
                new SerializedValue<>(
                        new TestingOperatorCoordinator.Provider(
                                OperatorID.fromJobVertexID(jobVertices[0].getID()))));

        jobVertices[1].addOperatorCoordinator(
                new SerializedValue<>(
                        new TestingOperatorCoordinator.Provider(
                                OperatorID.fromJobVertexID(jobVertices[1].getID()))));

        return jobVertices;
    }

    private DefaultExecutionGraph createDynamicGraph(JobVertex... jobVertices) throws Exception {
        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobGraph(new JobGraph(new JobID(), "TestJob", jobVertices))
                .buildDynamicGraph(EXECUTOR_RESOURCE.getExecutor());
    }
}
