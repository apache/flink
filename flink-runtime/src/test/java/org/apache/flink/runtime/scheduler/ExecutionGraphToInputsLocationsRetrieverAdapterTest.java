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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link ExecutionGraphToInputsLocationsRetrieverAdapter}. */
public class ExecutionGraphToInputsLocationsRetrieverAdapterTest extends TestLogger {

    /** Tests that can get the producers of consumed result partitions. */
    @Test
    public void testGetConsumedResultPartitionsProducers() throws Exception {
        final JobVertex producer1 = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex producer2 = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex consumer = ExecutionGraphTestUtils.createNoOpVertex(1);
        consumer.connectNewDataSetAsInput(
                producer1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        consumer.connectNewDataSetAsInput(
                producer2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        final ExecutionGraph eg =
                ExecutionGraphTestUtils.createSimpleTestGraph(producer1, producer2, consumer);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        ExecutionVertexID evIdOfProducer1 = new ExecutionVertexID(producer1.getID(), 0);
        ExecutionVertexID evIdOfProducer2 = new ExecutionVertexID(producer2.getID(), 0);
        ExecutionVertexID evIdOfConsumer = new ExecutionVertexID(consumer.getID(), 0);

        Collection<Collection<ExecutionVertexID>> producersOfProducer1 =
                inputsLocationsRetriever.getConsumedResultPartitionsProducers(evIdOfProducer1);
        Collection<Collection<ExecutionVertexID>> producersOfProducer2 =
                inputsLocationsRetriever.getConsumedResultPartitionsProducers(evIdOfProducer2);
        Collection<Collection<ExecutionVertexID>> producersOfConsumer =
                inputsLocationsRetriever.getConsumedResultPartitionsProducers(evIdOfConsumer);

        assertThat(producersOfProducer1, is(empty()));
        assertThat(producersOfProducer2, is(empty()));
        assertThat(producersOfConsumer, hasSize(2));
        assertThat(producersOfConsumer, hasItem(Collections.singletonList(evIdOfProducer1)));
        assertThat(producersOfConsumer, hasItem(Collections.singletonList(evIdOfProducer2)));
    }

    /** Tests that it will get empty task manager location if vertex is not scheduled. */
    @Test
    public void testGetEmptyTaskManagerLocationIfVertexNotScheduled() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(jobVertex);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertex.getID(), 0);
        Optional<CompletableFuture<TaskManagerLocation>> taskManagerLocation =
                inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);

        assertFalse(taskManagerLocation.isPresent());
    }

    /** Tests that it can get the task manager location in an Execution. */
    @Test
    public void testGetTaskManagerLocationWhenScheduled() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        final TestingLogicalSlot testingLogicalSlot =
                new TestingLogicalSlotBuilder().createTestingLogicalSlot();
        final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(jobVertex);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        final ExecutionVertex onlyExecutionVertex = eg.getAllExecutionVertices().iterator().next();
        onlyExecutionVertex.deployToSlot(testingLogicalSlot);

        ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertex.getID(), 0);
        Optional<CompletableFuture<TaskManagerLocation>> taskManagerLocationOptional =
                inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);

        assertTrue(taskManagerLocationOptional.isPresent());

        final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture =
                taskManagerLocationOptional.get();
        assertThat(
                taskManagerLocationFuture.get(), is(testingLogicalSlot.getTaskManagerLocation()));
    }

    /**
     * Tests that it will throw exception when getting the task manager location of a non existing
     * execution.
     */
    @Test
    public void testGetNonExistingExecutionVertexWillThrowException() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(jobVertex);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        ExecutionVertexID invalidExecutionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        try {
            inputsLocationsRetriever.getTaskManagerLocation(invalidExecutionVertexId);
            fail("Should throw exception if execution vertex doesn't exist!");
        } catch (IllegalStateException expected) {
            // expect this exception
        }
    }
}
