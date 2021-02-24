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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class contains test concerning the correct conversion from {@link JobGraph} to {@link
 * ExecutionGraph} objects. It also tests that {@link EdgeManagerBuildUtil#connectVertexToResult}
 * builds {@link DistributionPattern#ALL_TO_ALL} connections correctly.
 */
public class DefaultExecutionGraphConstructionTest {

    private ExecutionGraph createDefaultExecutionGraph() throws Exception {
        return TestingDefaultExecutionGraphBuilder.newBuilder().build();
    }

    @Test
    public void testExecutionAttemptIdInTwoIdenticalJobsIsNotSame() throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3));

        ExecutionGraph eg1 = createDefaultExecutionGraph();
        ExecutionGraph eg2 = createDefaultExecutionGraph();
        eg1.attachJobGraph(ordered);
        eg2.attachJobGraph(ordered);

        assertThat(
                Sets.intersection(
                        eg1.getRegisteredExecutions().keySet(),
                        eg2.getRegisteredExecutions().keySet()),
                is(empty()));
    }

    /**
     * Creates a JobGraph of the following form:
     *
     * <pre>
     *  v1--->v2-->\
     *              \
     *               v4 --->\
     *        ----->/        \
     *  v3-->/                v5
     *       \               /
     *        ------------->/
     * </pre>
     */
    @Test
    public void testCreateSimpleGraphBipartite() throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);
        v4.setParallelism(11);
        v5.setParallelism(4);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4, v5));

        ExecutionGraph eg = createDefaultExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        verifyTestGraph(eg, v1, v2, v3, v4, v5);
    }

    @Test
    public void testAttachViaDataSets() throws Exception {
        // construct part one of the execution graph
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);

        // this creates an intermediate result for v1
        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        // create results for v2 and v3
        IntermediateDataSet v2result = v2.createAndAddResultDataSet(ResultPartitionType.PIPELINED);
        IntermediateDataSet v3result_1 =
                v3.createAndAddResultDataSet(ResultPartitionType.PIPELINED);
        IntermediateDataSet v3result_2 =
                v3.createAndAddResultDataSet(ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3));

        ExecutionGraph eg = createDefaultExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        // attach the second part of the graph

        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");
        v4.setParallelism(11);
        v5.setParallelism(4);

        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v4.connectDataSetAsInput(v2result, DistributionPattern.ALL_TO_ALL);
        v4.connectDataSetAsInput(v3result_1, DistributionPattern.ALL_TO_ALL);
        v5.connectNewDataSetAsInput(
                v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectDataSetAsInput(v3result_2, DistributionPattern.ALL_TO_ALL);

        List<JobVertex> ordered2 = new ArrayList<JobVertex>(Arrays.asList(v4, v5));

        try {
            eg.attachJobGraph(ordered2);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        // verify
        verifyTestGraph(eg, v1, v2, v3, v4, v5);
    }

    @Test
    public void testAttachViaIds() throws Exception {
        // construct part one of the execution graph
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);

        // this creates an intermediate result for v1
        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        // create results for v2 and v3
        IntermediateDataSet v2result = v2.createAndAddResultDataSet(ResultPartitionType.PIPELINED);
        IntermediateDataSet v3result_1 =
                v3.createAndAddResultDataSet(ResultPartitionType.PIPELINED);
        IntermediateDataSet v3result_2 =
                v3.createAndAddResultDataSet(ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3));

        ExecutionGraph eg = createDefaultExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        // attach the second part of the graph

        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");
        v4.setParallelism(11);
        v5.setParallelism(4);

        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v4.connectIdInput(v2result.getId(), DistributionPattern.ALL_TO_ALL);
        v4.connectIdInput(v3result_1.getId(), DistributionPattern.ALL_TO_ALL);
        v5.connectNewDataSetAsInput(
                v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectIdInput(v3result_2.getId(), DistributionPattern.ALL_TO_ALL);

        List<JobVertex> ordered2 = new ArrayList<JobVertex>(Arrays.asList(v4, v5));

        try {
            eg.attachJobGraph(ordered2);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        // verify
        verifyTestGraph(eg, v1, v2, v3, v4, v5);
    }

    private void verifyTestGraph(
            ExecutionGraph eg,
            JobVertex v1,
            JobVertex v2,
            JobVertex v3,
            JobVertex v4,
            JobVertex v5) {
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v1, null, Collections.singletonList(v2));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v2, Collections.singletonList(v1), Collections.singletonList(v4));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v3, null, Arrays.asList(v4, v5));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v4, Arrays.asList(v2, v3), Collections.singletonList(v5));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v5, Arrays.asList(v4, v3), null);
    }

    @Test
    public void testCannotConnectMissingId() throws Exception {
        // construct part one of the execution graph
        JobVertex v1 = new JobVertex("vertex1");
        v1.setParallelism(7);
        v1.setInvokableClass(AbstractInvokable.class);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1));

        ExecutionGraph eg = createDefaultExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        // attach the second part of the graph
        JobVertex v2 = new JobVertex("vertex2");
        v2.setInvokableClass(AbstractInvokable.class);
        v2.connectIdInput(new IntermediateDataSetID(), DistributionPattern.ALL_TO_ALL);

        List<JobVertex> ordered2 = new ArrayList<JobVertex>(Arrays.asList(v2));

        try {
            eg.attachJobGraph(ordered2);
            fail("Attached wrong jobgraph");
        } catch (JobException e) {
            // expected
        }
    }

    @Test
    public void testCannotConnectWrongOrder() throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);
        v4.setParallelism(11);
        v5.setParallelism(4);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v5, v4));

        ExecutionGraph eg = createDefaultExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
            fail("Attached wrong jobgraph");
        } catch (JobException e) {
            // expected
        }
    }

    @Test
    public void testSetupInputSplits() {
        try {
            final InputSplit[] emptySplits = new InputSplit[0];

            InputSplitAssigner assigner1 = mock(InputSplitAssigner.class);
            InputSplitAssigner assigner2 = mock(InputSplitAssigner.class);

            @SuppressWarnings("unchecked")
            InputSplitSource<InputSplit> source1 = mock(InputSplitSource.class);
            @SuppressWarnings("unchecked")
            InputSplitSource<InputSplit> source2 = mock(InputSplitSource.class);

            when(source1.createInputSplits(Matchers.anyInt())).thenReturn(emptySplits);
            when(source2.createInputSplits(Matchers.anyInt())).thenReturn(emptySplits);
            when(source1.getInputSplitAssigner(emptySplits)).thenReturn(assigner1);
            when(source2.getInputSplitAssigner(emptySplits)).thenReturn(assigner2);

            final JobID jobId = new JobID();
            final String jobName = "Test Job Sample Name";
            final Configuration cfg = new Configuration();

            JobVertex v1 = new JobVertex("vertex1");
            JobVertex v2 = new JobVertex("vertex2");
            JobVertex v3 = new JobVertex("vertex3");
            JobVertex v4 = new JobVertex("vertex4");
            JobVertex v5 = new JobVertex("vertex5");

            v1.setParallelism(5);
            v2.setParallelism(7);
            v3.setParallelism(2);
            v4.setParallelism(11);
            v5.setParallelism(4);

            v1.setInvokableClass(AbstractInvokable.class);
            v2.setInvokableClass(AbstractInvokable.class);
            v3.setInvokableClass(AbstractInvokable.class);
            v4.setInvokableClass(AbstractInvokable.class);
            v5.setInvokableClass(AbstractInvokable.class);

            v2.connectNewDataSetAsInput(
                    v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
            v4.connectNewDataSetAsInput(
                    v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
            v4.connectNewDataSetAsInput(
                    v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
            v5.connectNewDataSetAsInput(
                    v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
            v5.connectNewDataSetAsInput(
                    v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            v3.setInputSplitSource(source1);
            v5.setInputSplitSource(source2);

            List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4, v5));

            ExecutionGraph eg = createDefaultExecutionGraph();
            try {
                eg.attachJobGraph(ordered);
            } catch (JobException e) {
                e.printStackTrace();
                fail("Job failed with exception: " + e.getMessage());
            }

            assertEquals(assigner1, eg.getAllVertices().get(v3.getID()).getSplitAssigner());
            assertEquals(assigner2, eg.getAllVertices().get(v5.getID()).getSplitAssigner());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMoreThanOneConsumerForIntermediateResult() {
        try {
            JobVertex v1 = new JobVertex("vertex1");
            JobVertex v2 = new JobVertex("vertex2");
            JobVertex v3 = new JobVertex("vertex3");

            v1.setParallelism(5);
            v2.setParallelism(7);
            v3.setParallelism(2);

            IntermediateDataSet result =
                    v1.createAndAddResultDataSet(ResultPartitionType.PIPELINED);
            v2.connectDataSetAsInput(result, DistributionPattern.ALL_TO_ALL);
            v3.connectDataSetAsInput(result, DistributionPattern.ALL_TO_ALL);

            List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3));

            ExecutionGraph eg = createDefaultExecutionGraph();

            try {
                eg.attachJobGraph(ordered);
                fail("Should not be possible");
            } catch (RuntimeException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
