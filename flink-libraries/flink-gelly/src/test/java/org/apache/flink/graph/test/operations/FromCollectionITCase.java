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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/** Test creating graphs from collections. */
@RunWith(Parameterized.class)
public class FromCollectionITCase extends MultipleProgramsTestBase {

    public FromCollectionITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expectedResult;

    @Test
    public void testFromCollectionVerticesEdges() throws Exception {
        /*
         * Test fromCollection(vertices, edges):
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongLongVertices(),
                        TestGraphUtils.getLongLongEdges(),
                        env);

        DataSet<Edge<Long, Long>> data = graph.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,12\n"
                        + "1,3,13\n"
                        + "2,3,23\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testFromCollectionEdgesNoInitialValue() throws Exception {
        /*
         * Test fromCollection(edges) with no initial value for the vertices
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, NullValue, Long> graph =
                Graph.fromCollection(TestGraphUtils.getLongLongEdges(), env);

        DataSet<Vertex<Long, NullValue>> data = graph.getVertices();
        List<Vertex<Long, NullValue>> result = data.collect();

        expectedResult = "1,(null)\n" + "2,(null)\n" + "3,(null)\n" + "4,(null)\n" + "5,(null)\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testFromCollectionEdgesWithInitialValue() throws Exception {
        /*
         * Test fromCollection(edges) with vertices initialised by a
         * function that takes the id and doubles it
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongLongEdges(), new InitVerticesMapper(), env);

        DataSet<Vertex<Long, Long>> data = graph.getVertices();
        List<Vertex<Long, Long>> result = data.collect();

        expectedResult = "1,2\n" + "2,4\n" + "3,6\n" + "4,8\n" + "5,10\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    private static final class InitVerticesMapper implements MapFunction<Long, Long> {
        public Long map(Long vertexId) {
            return vertexId * 2;
        }
    }
}
