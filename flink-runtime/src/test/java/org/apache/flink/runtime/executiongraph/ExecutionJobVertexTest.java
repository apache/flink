/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.function.Function;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link ExecutionJobVertex} */
public class ExecutionJobVertexTest {
    @Test
    public void testParallelismGreaterThanMaxParallelism() {
        JobVertex jobVertex = new JobVertex("testVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);
        // parallelism must be smaller than the max parallelism
        jobVertex.setParallelism(172);
        jobVertex.setMaxParallelism(4);

        assertThrows(
                "higher than the max parallelism",
                JobException.class,
                () -> ExecutionGraphTestUtils.getExecutionJobVertex(jobVertex));
    }

    @Test
    public void testLazyInitialization() throws Exception {
        final int parallelism = 3;
        final int configuredMaxParallelism = 12;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(parallelism, configuredMaxParallelism, -1);

        assertThat(ejv.getParallelism(), is(parallelism));
        assertThat(ejv.getMaxParallelism(), is(configuredMaxParallelism));
        assertThat(ejv.isInitialized(), is(false));

        assertThat(ejv.getTaskVertices().length, is(0));

        try {
            ejv.getInputs();
            Assert.fail("failure is expected");
        } catch (IllegalStateException e) {
            // ignore
        }

        try {
            ejv.getProducedDataSets();
            Assert.fail("failure is expected");
        } catch (IllegalStateException e) {
            // ignore
        }

        try {
            ejv.getSplitAssigner();
            Assert.fail("failure is expected");
        } catch (IllegalStateException e) {
            // ignore
        }

        try {
            ejv.getOperatorCoordinators();
            Assert.fail("failure is expected");
        } catch (IllegalStateException e) {
            // ignore
        }

        try {
            ejv.connectToPredecessors(Collections.emptyMap());
            Assert.fail("failure is expected");
        } catch (IllegalStateException e) {
            // ignore
        }

        try {
            ejv.executionVertexFinished();
            Assert.fail("failure is expected");
        } catch (IllegalStateException e) {
            // ignore
        }

        try {
            ejv.executionVertexUnFinished();
            Assert.fail("failure is expected");
        } catch (IllegalStateException e) {
            // ignore
        }

        initializeVertex(ejv);

        assertThat(ejv.isInitialized(), is(true));
        assertThat(ejv.getTaskVertices().length, is(3));
        assertThat(ejv.getInputs().size(), is(0));
        assertThat(ejv.getProducedDataSets().length, is(1));
        assertThat(ejv.getOperatorCoordinators().size(), is(0));
    }

    @Test(expected = IllegalStateException.class)
    public void testErrorIfInitializationWithoutParallelismDecided() throws Exception {
        final ExecutionJobVertex ejv = createDynamicExecutionJobVertex();

        initializeVertex(ejv);
    }

    @Test
    public void testSetParallelismLazily() throws Exception {
        final int parallelism = 3;
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(-1, -1, defaultMaxParallelism);

        assertThat(ejv.isParallelismDecided(), is(false));

        ejv.setParallelism(parallelism);

        assertThat(ejv.isParallelismDecided(), is(true));
        assertThat(ejv.getParallelism(), is(parallelism));

        initializeVertex(ejv);

        assertThat(ejv.getTaskVertices().length, is(parallelism));
    }

    @Test
    public void testConfiguredMaxParallelismIsRespected() throws Exception {
        final int configuredMaxParallelism = 12;
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(
                        -1, configuredMaxParallelism, defaultMaxParallelism);

        assertThat(ejv.getMaxParallelism(), is(configuredMaxParallelism));
    }

    @Test
    public void testComputingMaxParallelismFromConfiguredParallelism() throws Exception {
        final int parallelism = 300;
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(parallelism, -1, defaultMaxParallelism);

        assertThat(ejv.getMaxParallelism(), is(512));
    }

    @Test
    public void testFallingBackToDefaultMaxParallelism() throws Exception {
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(-1, -1, defaultMaxParallelism);

        assertThat(ejv.getMaxParallelism(), is(defaultMaxParallelism));
    }

    static void initializeVertex(ExecutionJobVertex vertex) throws Exception {
        vertex.initialize(
                1,
                Time.milliseconds(1L),
                1L,
                new DefaultSubtaskAttemptNumberStore(Collections.emptyList()));
    }

    private static ExecutionJobVertex createDynamicExecutionJobVertex() throws Exception {
        return createDynamicExecutionJobVertex(-1, -1, 1);
    }

    public static ExecutionJobVertex createDynamicExecutionJobVertex(
            int parallelism, int maxParallelism, int defaultMaxParallelism) throws Exception {
        JobVertex jobVertex = new JobVertex("testVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);
        jobVertex.createAndAddResultDataSet(
                new IntermediateDataSetID(), ResultPartitionType.BLOCKING);

        if (maxParallelism > 0) {
            jobVertex.setMaxParallelism(maxParallelism);
        }

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        }

        final DefaultExecutionGraph eg = TestingDefaultExecutionGraphBuilder.newBuilder().build();
        final VertexParallelismStore vertexParallelismStore =
                computeVertexParallelismStoreForDynamicGraph(
                        Collections.singletonList(jobVertex), defaultMaxParallelism);
        final VertexParallelismInformation vertexParallelismInfo =
                vertexParallelismStore.getParallelismInfo(jobVertex.getID());

        return new ExecutionJobVertex(eg, jobVertex, vertexParallelismInfo);
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices in a dynamic graph, which
     * will set defaults and ensure that the returned store contains valid parallelisms, with the
     * configured default max parallelism.
     *
     * @param vertices the vertices to compute parallelism for
     * @param defaultMaxParallelism the global default max parallelism
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStoreForDynamicGraph(
            Iterable<JobVertex> vertices, int defaultMaxParallelism) {
        // for dynamic graph, there is no need to normalize vertex parallelism. if the max
        // parallelism is not configured and the parallelism is a positive value, max
        // parallelism can be computed against the parallelism, otherwise it needs to use the
        // global default max parallelism.
        return SchedulerBase.computeVertexParallelismStore(
                vertices,
                v -> {
                    if (v.getParallelism() > 0) {
                        return SchedulerBase.getDefaultMaxParallelism(v);
                    } else {
                        return defaultMaxParallelism;
                    }
                },
                Function.identity());
    }
}
