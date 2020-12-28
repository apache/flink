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

package org.apache.flink.graph.test.operations;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test expected errors for {@link Graph#inDegrees()}, {@link Graph#outDegrees()}, and {@link
 * Graph#getDegrees()}.
 */
public class DegreesWithExceptionITCase extends AbstractTestBase {

    private static final int PARALLELISM = 4;

    /** Test outDegrees() with an edge having a srcId that does not exist in the vertex DataSet. */
    @Test
    public void testOutDegreesInvalidEdgeSrcId() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeInvalidSrcData(env),
                        env);

        try {
            graph.outDegrees().output(new DiscardingOutputFormat<>());
            env.execute();

            fail("graph.outDegrees() did not fail.");
        } catch (Exception e) {
            // We expect the job to fail with an exception
        }
    }

    /** Test inDegrees() with an edge having a trgId that does not exist in the vertex DataSet. */
    @Test
    public void testInDegreesInvalidEdgeTrgId() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeInvalidTrgData(env),
                        env);

        try {
            graph.inDegrees().output(new DiscardingOutputFormat<>());
            env.execute();

            fail("graph.inDegrees() did not fail.");
        } catch (Exception e) {
            // We expect the job to fail with an exception
        }
    }

    /** Test getDegrees() with an edge having a trgId that does not exist in the vertex DataSet. */
    @Test
    public void testGetDegreesInvalidEdgeTrgId() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeInvalidTrgData(env),
                        env);

        try {
            graph.getDegrees().output(new DiscardingOutputFormat<>());
            env.execute();

            fail("graph.getDegrees() did not fail.");
        } catch (Exception e) {
            // We expect the job to fail with an exception
        }
    }

    /** Test getDegrees() with an edge having a srcId that does not exist in the vertex DataSet. */
    @Test
    public void testGetDegreesInvalidEdgeSrcId() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeInvalidSrcData(env),
                        env);

        try {
            graph.getDegrees().output(new DiscardingOutputFormat<>());
            env.execute();

            fail("graph.getDegrees() did not fail.");
        } catch (Exception e) {
            // We expect the job to fail with an exception
        }
    }

    /**
     * Test getDegrees() with an edge having a srcId and a trgId that does not exist in the vertex
     * DataSet.
     */
    @Test
    public void testGetDegreesInvalidEdgeSrcTrgId() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeInvalidSrcTrgData(env),
                        env);

        try {
            graph.getDegrees().output(new DiscardingOutputFormat<>());
            env.execute();

            fail("graph.getDegrees() did not fail.");
        } catch (Exception e) {
            // We expect the job to fail with an exception
        }
    }
}
