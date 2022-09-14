/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility for computing strongly connected components.
 *
 * <p>The computation is an implementation of Tarjan's algorithm.
 *
 * <p>Ref: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm.
 */
public final class StronglyConnectedComponentsComputeUtils {

    private StronglyConnectedComponentsComputeUtils() {}

    static Set<Set<Integer>> computeStronglyConnectedComponents(
            final int numVertex, final List<List<Integer>> outEdges) {
        final Set<Set<Integer>> stronglyConnectedComponents = new HashSet<>();

        // a vertex will be added into this stack when it is visited for the first time
        final Deque<Integer> visitingStack = new ArrayDeque<>(numVertex);
        final boolean[] onVisitingStack = new boolean[numVertex];

        // stores the order that a vertex is visited for the first time, -1 indicates it is not
        // visited yet
        final int[] vertexIndices = new int[numVertex];
        Arrays.fill(vertexIndices, -1);

        final AtomicInteger indexCounter = new AtomicInteger(0);

        final int[] vertexLowLinks = new int[numVertex];

        for (int vertex = 0; vertex < numVertex; vertex++) {
            if (!isVisited(vertex, vertexIndices)) {
                dfs(
                        vertex,
                        outEdges,
                        vertexIndices,
                        vertexLowLinks,
                        visitingStack,
                        onVisitingStack,
                        indexCounter,
                        stronglyConnectedComponents);
            }
        }

        return stronglyConnectedComponents;
    }

    private static boolean isVisited(final int vertex, final int[] vertexIndices) {
        return vertexIndices[vertex] != -1;
    }

    private static void dfs(
            final int rootVertex,
            final List<List<Integer>> outEdges,
            final int[] vertexIndices,
            final int[] vertexLowLinks,
            final Deque<Integer> visitingStack,
            final boolean[] onVisitingStack,
            final AtomicInteger indexCounter,
            final Set<Set<Integer>> stronglyConnectedComponents) {

        final Deque<Tuple2<Integer, Integer>> dfsLoopStack = new ArrayDeque<>();
        dfsLoopStack.add(new Tuple2<>(rootVertex, 0));

        while (!dfsLoopStack.isEmpty()) {
            Tuple2<Integer, Integer> tuple = dfsLoopStack.pollLast();
            final int currentVertex = tuple.f0;
            final int vertexOutEdgeIndex = tuple.f1;

            if (vertexOutEdgeIndex == 0) {
                startTraversingVertex(
                        currentVertex,
                        vertexIndices,
                        vertexLowLinks,
                        visitingStack,
                        onVisitingStack,
                        indexCounter);
            } else if (vertexOutEdgeIndex > 0) {
                finishTraversingOutEdge(
                        currentVertex, vertexOutEdgeIndex - 1, outEdges, vertexLowLinks);
            }

            if (traverseOutEdges(
                    currentVertex,
                    vertexOutEdgeIndex,
                    outEdges,
                    vertexIndices,
                    vertexLowLinks,
                    onVisitingStack,
                    dfsLoopStack)) {
                continue;
            }

            if (vertexLowLinks[currentVertex] == vertexIndices[currentVertex]) {
                stronglyConnectedComponents.add(
                        createConnectedComponent(currentVertex, visitingStack, onVisitingStack));
            }
        }
    }

    private static void startTraversingVertex(
            final int currentVertex,
            final int[] vertexIndices,
            final int[] vertexLowLinks,
            final Deque<Integer> visitingStack,
            final boolean[] onVisitingStack,
            final AtomicInteger indexCounter) {

        vertexIndices[currentVertex] = indexCounter.get();
        vertexLowLinks[currentVertex] = indexCounter.getAndIncrement();
        visitingStack.add(currentVertex);
        onVisitingStack[currentVertex] = true;
    }

    private static void finishTraversingOutEdge(
            final int currentVertex,
            final int vertexOutEdgeIndex,
            final List<List<Integer>> outEdges,
            final int[] vertexLowLinks) {

        final int successorVertex = outEdges.get(currentVertex).get(vertexOutEdgeIndex);
        vertexLowLinks[currentVertex] =
                Math.min(vertexLowLinks[currentVertex], vertexLowLinks[successorVertex]);
    }

    private static boolean traverseOutEdges(
            final int currentVertex,
            final int vertexOutEdgeIndex,
            final List<List<Integer>> outEdges,
            final int[] vertexIndices,
            final int[] vertexLowLinks,
            final boolean[] onVisitingStack,
            final Deque<Tuple2<Integer, Integer>> dfsLoopStack) {

        for (int i = vertexOutEdgeIndex; i < outEdges.get(currentVertex).size(); i++) {
            final int successorVertex = outEdges.get(currentVertex).get(i);
            if (!isVisited(successorVertex, vertexIndices)) {
                dfsLoopStack.add(new Tuple2<>(currentVertex, i + 1));
                dfsLoopStack.add(new Tuple2<>(successorVertex, 0));
                return true;
            } else if (onVisitingStack[successorVertex]) {
                // this is deliberate and the proof can be found in Tarjan's paper
                vertexLowLinks[currentVertex] =
                        Math.min(vertexLowLinks[currentVertex], vertexIndices[successorVertex]);
            }
        }
        return false;
    }

    private static Set<Integer> createConnectedComponent(
            final int currentVertex,
            final Deque<Integer> visitingStack,
            final boolean[] onVisitingStack) {

        final Set<Integer> scc = new HashSet<>();
        while (onVisitingStack[currentVertex]) {
            final int v = visitingStack.pollLast();
            onVisitingStack[v] = false;
            scc.add(v);
        }
        return scc;
    }
}
