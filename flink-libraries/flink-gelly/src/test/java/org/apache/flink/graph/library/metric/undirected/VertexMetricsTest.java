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

package org.apache.flink.graph.library.metric.undirected;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.metric.undirected.VertexMetrics.Result;
import org.apache.flink.types.NullValue;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link VertexMetrics}. */
public class VertexMetricsTest extends AsmTestBase {

    /**
     * Validate a test result.
     *
     * @param graph input graph
     * @param includeZeroDegreeVertices whether to include zero-degree vertices
     * @param result expected {@link Result}
     * @param averageDegree result average degree
     * @param density result density
     * @param <T> graph ID type
     * @throws Exception on error
     */
    private static <T extends Comparable<T>> void validate(
            Graph<T, NullValue, NullValue> graph,
            boolean includeZeroDegreeVertices,
            Result result,
            float averageDegree,
            float density)
            throws Exception {
        Result vertexMetrics =
                new VertexMetrics<T, NullValue, NullValue>()
                        .setIncludeZeroDegreeVertices(includeZeroDegreeVertices)
                        .run(graph)
                        .execute();

        assertEquals(result, vertexMetrics);
        assertEquals(averageDegree, vertexMetrics.getAverageDegree(), ACCURACY);
        assertEquals(density, vertexMetrics.getDensity(), ACCURACY);
    }

    @Test
    public void testWithSimpleGraph() throws Exception {
        validate(undirectedSimpleGraph, false, new Result(6, 7, 13, 4, 6), 14f / 6, 7f / 15);
    }

    @Test
    public void testWithCompleteGraph() throws Exception {
        long expectedDegree = completeGraphVertexCount - 1;
        long expectedEdges = completeGraphVertexCount * expectedDegree / 2;
        long expectedMaximumTriplets =
                CombinatoricsUtils.binomialCoefficient((int) expectedDegree, 2);
        long expectedTriplets = completeGraphVertexCount * expectedMaximumTriplets;

        Result expectedResult =
                new Result(
                        completeGraphVertexCount,
                        expectedEdges,
                        expectedTriplets,
                        expectedDegree,
                        expectedMaximumTriplets);

        validate(completeGraph, false, expectedResult, expectedDegree, 1.0f);
    }

    @Test
    public void testWithEmptyGraphWithVertices() throws Exception {
        validate(emptyGraphWithVertices, false, new Result(0, 0, 0, 0, 0), Float.NaN, Float.NaN);
        validate(emptyGraphWithVertices, true, new Result(3, 0, 0, 0, 0), 0.0f, 0.0f);
    }

    @Test
    public void testWithEmptyGraphWithoutVertices() throws Exception {
        Result expectedResult = new Result(0, 0, 0, 0, 0);
        validate(emptyGraphWithoutVertices, false, expectedResult, Float.NaN, Float.NaN);
        validate(emptyGraphWithoutVertices, true, expectedResult, Float.NaN, Float.NaN);
    }

    @Test
    public void testWithRMatGraph() throws Exception {
        Result expectedResult = new Result(902, 10442, 1003442, 463, 106953);
        validate(undirectedRMatGraph(10, 16), false, expectedResult, 23.1529933f, 0.0256969f);
    }
}
