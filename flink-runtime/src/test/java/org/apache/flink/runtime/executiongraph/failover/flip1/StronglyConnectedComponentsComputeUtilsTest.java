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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.executiongraph.failover.flip1.StronglyConnectedComponentsComputeUtils.computeStronglyConnectedComponents;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StronglyConnectedComponentsComputeUtils}. */
class StronglyConnectedComponentsComputeUtilsTest {

    @Test
    void testWithCycles() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(2, 3),
                        Arrays.asList(0),
                        Arrays.asList(1),
                        Arrays.asList(4),
                        Collections.emptyList());

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(5, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(new HashSet<>(Arrays.asList(0, 1, 2)));
        expected.add(Collections.singleton(3));
        expected.add(Collections.singleton(4));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testWithMultipleCycles() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2),
                        Arrays.asList(0),
                        Arrays.asList(1, 2, 4),
                        Arrays.asList(3, 5),
                        Arrays.asList(2, 6),
                        Arrays.asList(5),
                        Arrays.asList(4, 6, 7));

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(8, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(new HashSet<>(Arrays.asList(0, 1, 2)));
        expected.add(new HashSet<>(Arrays.asList(3, 4)));
        expected.add(new HashSet<>(Arrays.asList(5, 6)));
        expected.add(Collections.singleton(7));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testWithConnectedCycles() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2, 4, 5),
                        Arrays.asList(3, 6),
                        Arrays.asList(2, 7),
                        Arrays.asList(0, 5),
                        Arrays.asList(6),
                        Arrays.asList(5),
                        Arrays.asList(3, 6));

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(8, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(new HashSet<>(Arrays.asList(0, 1, 4)));
        expected.add(new HashSet<>(Arrays.asList(2, 3, 7)));
        expected.add(new HashSet<>(Arrays.asList(5, 6)));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testWithNoEdge() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(5, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(Collections.singleton(0));
        expected.add(Collections.singleton(1));
        expected.add(Collections.singleton(2));
        expected.add(Collections.singleton(3));
        expected.add(Collections.singleton(4));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testWithNoCycle() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2),
                        Arrays.asList(3),
                        Arrays.asList(4),
                        Collections.emptyList());

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(5, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(Collections.singleton(0));
        expected.add(Collections.singleton(1));
        expected.add(Collections.singleton(2));
        expected.add(Collections.singleton(3));
        expected.add(Collections.singleton(4));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testLargeGraph() {
        final int n = 100000;
        final List<List<Integer>> edges = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            edges.add(Collections.singletonList((i + 1) % n));
        }

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(n, edges);

        final Set<Integer> singleComponent =
                IntStream.range(0, n).boxed().collect(Collectors.toSet());

        assertThat(result).isEqualTo(Collections.singleton(singleComponent));
    }
}
