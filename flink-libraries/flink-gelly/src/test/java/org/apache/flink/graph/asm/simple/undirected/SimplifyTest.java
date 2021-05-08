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

package org.apache.flink.graph.asm.simple.undirected;

import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.generator.TestUtils;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/** Tests for {@link Simplify}. */
public class SimplifyTest extends AsmTestBase {

    protected Graph<IntValue, NullValue, NullValue> graph;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();

        Object[][] edges =
                new Object[][] {
                    new Object[] {0, 0},
                    new Object[] {0, 1},
                    new Object[] {0, 1},
                    new Object[] {0, 2},
                    new Object[] {0, 2},
                    new Object[] {1, 0},
                    new Object[] {2, 2},
                };

        List<Edge<IntValue, NullValue>> edgeList = new LinkedList<>();

        for (Object[] edge : edges) {
            edgeList.add(
                    new Edge<>(
                            new IntValue((int) edge[0]),
                            new IntValue((int) edge[1]),
                            NullValue.getInstance()));
        }

        graph = Graph.fromCollection(edgeList, env);
    }

    @Test
    public void testWithFullFlip() throws Exception {
        String expectedResult =
                "(0,1,(null))\n" + "(0,2,(null))\n" + "(1,0,(null))\n" + "(2,0,(null))";

        Graph<IntValue, NullValue, NullValue> simpleGraph = graph.run(new Simplify<>(false));

        TestBaseUtils.compareResultAsText(simpleGraph.getEdges().collect(), expectedResult);
    }

    @Test
    public void testWithClipAndFlip() throws Exception {
        String expectedResult = "(0,1,(null))\n" + "(1,0,(null))";

        Graph<IntValue, NullValue, NullValue> simpleGraph = graph.run(new Simplify<>(true));

        TestBaseUtils.compareResultAsText(simpleGraph.getEdges().collect(), expectedResult);
    }

    @Test
    public void testParallelism() throws Exception {
        int parallelism = 2;

        Graph<IntValue, NullValue, NullValue> simpleGraph = graph.run(new Simplify<>(true));

        simpleGraph.getVertices().output(new DiscardingOutputFormat<>());
        simpleGraph.getEdges().output(new DiscardingOutputFormat<>());

        TestUtils.verifyParallelism(env, parallelism);
    }
}
