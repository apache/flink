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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.junit.Assert.fail;

/** Test expected exceptions for {@link Graph#groupReduceOnEdges}. */
public class ReduceOnEdgesWithExceptionITCase extends AbstractTestBase {

    private static final int PARALLELISM = 4;

    /**
     * Test groupReduceOnEdges() with an edge having a srcId that does not exist in the vertex
     * DataSet.
     */
    @Test
    public void testGroupReduceOnEdgesInvalidEdgeSrcId() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeInvalidSrcData(env),
                        env);

        try {
            DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
                    graph.groupReduceOnEdges(
                            new SelectNeighborsValueGreaterThanFour(), EdgeDirection.ALL);

            verticesWithAllNeighbors.output(new DiscardingOutputFormat<>());
            env.execute();

            fail("Expected an exception.");
        } catch (Exception e) {
            // We expect the job to fail with an exception
        }
    }

    /**
     * Test groupReduceOnEdges() with an edge having a trgId that does not exist in the vertex
     * DataSet.
     */
    @Test
    public void testGroupReduceOnEdgesInvalidEdgeTrgId() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeInvalidTrgData(env),
                        env);

        try {
            DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
                    graph.groupReduceOnEdges(
                            new SelectNeighborsValueGreaterThanFour(), EdgeDirection.ALL);

            verticesWithAllNeighbors.output(new DiscardingOutputFormat<>());
            env.execute();

            fail("Expected an exception.");
        } catch (Exception e) {
            // We expect the job to fail with an exception
        }
    }

    @SuppressWarnings("serial")
    private static final class SelectNeighborsValueGreaterThanFour
            implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

        @Override
        public void iterateEdges(
                Vertex<Long, Long> v,
                Iterable<Edge<Long, Long>> edges,
                Collector<Tuple2<Long, Long>> out)
                throws Exception {
            for (Edge<Long, Long> edge : edges) {
                if (v.getValue() > 4) {
                    if (v.getId().equals(edge.getTarget())) {
                        out.collect(new Tuple2<>(v.getId(), edge.getSource()));
                    } else {
                        out.collect(new Tuple2<>(v.getId(), edge.getTarget()));
                    }
                }
            }
        }
    }
}
