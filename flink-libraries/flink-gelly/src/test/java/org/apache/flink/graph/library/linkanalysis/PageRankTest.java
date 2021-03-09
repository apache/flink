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

package org.apache.flink.graph.library.linkanalysis;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.library.linkanalysis.PageRank.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Tests for {@link PageRank}. */
public class PageRankTest extends AsmTestBase {

    private static final double DAMPING_FACTOR = 0.85;

    /*
     * This test result can be verified with the following Python script.

    import networkx as nx

    graph=nx.read_edgelist('directedSimpleGraph.csv', delimiter=',', create_using=nx.DiGraph())
    pagerank=nx.algorithms.link_analysis.pagerank(graph, tol=0.000001)

    for key in sorted(pagerank):
    	print('{}: {}'.format(key, pagerank[key]))
     */
    @Test
    public void testWithSimpleGraph() throws Exception {
        DataSet<Result<IntValue>> pr =
                new PageRank<IntValue, NullValue, NullValue>(DAMPING_FACTOR, 20)
                        .run(directedSimpleGraph);

        List<Double> expectedResults = new ArrayList<>();
        expectedResults.add(0.0909212166211);
        expectedResults.add(0.279516064311);
        expectedResults.add(0.129562719068);
        expectedResults.add(0.223268406353);
        expectedResults.add(0.185810377026);
        expectedResults.add(0.0909212166211);

        for (Result<IntValue> result : pr.collect()) {
            int id = result.getVertexId0().getValue();
            assertEquals(expectedResults.get(id), result.getPageRankScore().getValue(), ACCURACY);
        }
    }

    /**
     * Validate a test where each result has the same values.
     *
     * @param graph input graph
     * @param count number of results
     * @param score result PageRank score
     * @param <T> graph ID type
     * @throws Exception on error
     */
    private static <T> void validate(Graph<T, NullValue, NullValue> graph, long count, double score)
            throws Exception {
        DataSet<Result<T>> pr =
                new PageRank<T, NullValue, NullValue>(DAMPING_FACTOR, ACCURACY)
                        .setIncludeZeroDegreeVertices(true)
                        .run(graph);

        List<Result<T>> results = pr.collect();

        assertEquals(count, results.size());

        for (Result<T> result : results) {
            assertEquals(score, result.getPageRankScore().getValue(), ACCURACY);
        }
    }

    @Test
    public void testWithCompleteGraph() throws Exception {
        validate(completeGraph, completeGraphVertexCount, 1.0 / completeGraphVertexCount);
    }

    @Test
    public void testWithEmptyGraphWithVertices() throws Exception {
        validate(emptyGraphWithVertices, emptyGraphVertexCount, 1.0 / emptyGraphVertexCount);
    }

    @Test
    public void testWithEmptyGraphWithoutVertices() throws Exception {
        validate(emptyGraphWithoutVertices, 0, Double.NaN);
    }

    /*
     * This test result can be verified with the following Python script.

    import networkx as nx

    graph=nx.read_edgelist('directedRMatGraph.csv', delimiter=',', create_using=nx.DiGraph())
    pagerank=nx.algorithms.link_analysis.pagerank(graph, tol=0.000001)

    for key in [0, 1, 2, 8, 13, 29, 109, 394, 652, 1020]:
    	print('{}: {}'.format(key, pagerank[str(key)]))
     */
    @Test
    public void testWithRMatGraph() throws Exception {
        DataSet<Result<LongValue>> pr =
                new PageRank<LongValue, NullValue, NullValue>(DAMPING_FACTOR, ACCURACY)
                        .run(directedRMatGraph(10, 16));

        Map<Long, Result<LongValue>> results = new HashMap<>();
        for (Result<LongValue> result : new Collect<Result<LongValue>>().run(pr).execute()) {
            results.put(result.getVertexId0().getValue(), result);
        }

        assertEquals(902, results.size());

        Map<Long, Double> expectedResults = new HashMap<>();
        // a pseudo-random selection of results, both high and low
        expectedResults.put(0L, 0.0271152394743);
        expectedResults.put(1L, 0.0132848430616);
        expectedResults.put(2L, 0.0121819700294);
        expectedResults.put(8L, 0.0115923214664);
        expectedResults.put(13L, 0.00183241122822);
        expectedResults.put(29L, 0.000848190646547);
        expectedResults.put(109L, 0.00030846825644);
        expectedResults.put(394L, 0.000828826945546);
        expectedResults.put(652L, 0.000683948671035);
        expectedResults.put(1020L, 0.000250442325034);

        for (Map.Entry<Long, Double> expected : expectedResults.entrySet()) {
            double value = results.get(expected.getKey()).getPageRankScore().getValue();

            assertEquals(expected.getValue(), value, ACCURACY);
        }
    }
}
