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
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ExecutionJobVertex} */
class ExecutionJobVertexTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testParallelismGreaterThanMaxParallelism() {
        JobVertex jobVertex = new JobVertex("testVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);
        // parallelism must be smaller than the max parallelism
        jobVertex.setParallelism(172);
        jobVertex.setMaxParallelism(4);

        assertThatThrownBy(() -> ExecutionGraphTestUtils.getExecutionJobVertex(jobVertex))
                .isInstanceOf(JobException.class)
                .hasMessageContaining("higher than the max parallelism");
    }

    @Test
    void testLazyInitialization() throws Exception {
        final int parallelism = 3;
        final int configuredMaxParallelism = 12;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(parallelism, configuredMaxParallelism, -1);

        assertThat(ejv.getParallelism()).isEqualTo(parallelism);
        assertThat(ejv.getMaxParallelism()).isEqualTo(configuredMaxParallelism);
        assertThat(ejv.isInitialized()).isFalse();

        assertThat(ejv.getTaskVertices()).isEmpty();

        assertThatThrownBy(ejv::getInputs).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(ejv::getProducedDataSets).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(ejv::getSplitAssigner).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(ejv::getOperatorCoordinators).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> ejv.connectToPredecessors(Collections.emptyMap()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(ejv::executionVertexFinished).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(ejv::executionVertexUnFinished)
                .isInstanceOf(IllegalStateException.class);

        initializeVertex(ejv);

        assertThat(ejv.isInitialized()).isTrue();
        assertThat(ejv.getTaskVertices()).hasSize(3);
        assertThat(ejv.getInputs()).isEmpty();
        assertThat(ejv.getProducedDataSets()).hasSize(1);
        assertThat(ejv.getOperatorCoordinators()).isEmpty();
    }

    @Test
    void testErrorIfInitializationWithoutParallelismDecided() throws Exception {
        final ExecutionJobVertex ejv = createDynamicExecutionJobVertex();

        assertThatThrownBy(() -> initializeVertex(ejv)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testSetParallelismLazily() throws Exception {
        final int parallelism = 3;
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(-1, -1, defaultMaxParallelism);

        assertThat(ejv.isParallelismDecided()).isFalse();

        ejv.setParallelism(parallelism);

        assertThat(ejv.isParallelismDecided()).isTrue();
        assertThat(ejv.getParallelism()).isEqualTo(parallelism);

        initializeVertex(ejv);

        assertThat(ejv.getTaskVertices()).hasSize(parallelism);
    }

    @Test
    void testConfiguredMaxParallelismIsRespected() throws Exception {
        final int configuredMaxParallelism = 12;
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(
                        -1, configuredMaxParallelism, defaultMaxParallelism);

        assertThat(ejv.getMaxParallelism()).isEqualTo(configuredMaxParallelism);
    }

    @Test
    void testComputingMaxParallelismFromConfiguredParallelism() throws Exception {
        final int parallelism = 300;
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(parallelism, -1, defaultMaxParallelism);

        assertThat(ejv.getMaxParallelism()).isEqualTo(512);
    }

    @Test
    void testFallingBackToDefaultMaxParallelism() throws Exception {
        final int defaultMaxParallelism = 13;
        final ExecutionJobVertex ejv =
                createDynamicExecutionJobVertex(-1, -1, defaultMaxParallelism);

        assertThat(ejv.getMaxParallelism()).isEqualTo(defaultMaxParallelism);
    }

    static void initializeVertex(ExecutionJobVertex vertex) throws Exception {
        vertex.initialize(
                1,
                Time.milliseconds(1L),
                1L,
                new DefaultSubtaskAttemptNumberStore(Collections.emptyList()),
                new CoordinatorStoreImpl(),
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
    }

    private static ExecutionJobVertex createDynamicExecutionJobVertex() throws Exception {
        return createDynamicExecutionJobVertex(-1, -1, 1);
    }

    public static ExecutionJobVertex createDynamicExecutionJobVertex(
            int parallelism, int maxParallelism, int defaultMaxParallelism) throws Exception {
        JobVertex jobVertex = new JobVertex("testVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);
        jobVertex.getOrCreateResultDataSet(
                new IntermediateDataSetID(), ResultPartitionType.BLOCKING);

        if (maxParallelism > 0) {
            jobVertex.setMaxParallelism(maxParallelism);
        }

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        }

        final DefaultExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .build(EXECUTOR_RESOURCE.getExecutor());
        final VertexParallelismStore vertexParallelismStore =
                AdaptiveBatchScheduler.computeVertexParallelismStoreForDynamicGraph(
                        Collections.singletonList(jobVertex), defaultMaxParallelism);
        final VertexParallelismInformation vertexParallelismInfo =
                vertexParallelismStore.getParallelismInfo(jobVertex.getID());

        return new ExecutionJobVertex(eg, jobVertex, vertexParallelismInfo);
    }
}
