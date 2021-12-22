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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

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
        final JobVertex jobVertex = new JobVertex("testVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);
        jobVertex.createAndAddResultDataSet(ResultPartitionType.BLOCKING);

        final DefaultExecutionGraph eg = TestingDefaultExecutionGraphBuilder.newBuilder().build();
        final DefaultVertexParallelismInfo vertexParallelismInfo =
                new DefaultVertexParallelismInfo(3, 8, max -> Optional.empty());
        final ExecutionJobVertex ejv = new ExecutionJobVertex(eg, jobVertex, vertexParallelismInfo);

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
    }

    static void initializeVertex(ExecutionJobVertex vertex) throws Exception {
        vertex.initialize(
                1,
                Time.milliseconds(1L),
                1L,
                new DefaultSubtaskAttemptNumberStore(Collections.emptyList()));
    }
}
