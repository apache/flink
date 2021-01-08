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

package org.apache.flink.table.planner.plan.nodes.exec.processor.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.TestingBatchExecNode;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/** Tests for {@link TopologyGraph}. */
public class TopologyGraphTest {

    private TestingBatchExecNode[] buildLinkedNodes() {
        // 0 -> 1 -> 2 --------> 5
        //       \-> 3 -> 4 ----/
        //            \
        //             \-> 6 -> 7
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[8];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode();
        }
        nodes[1].addInput(nodes[0]);
        nodes[2].addInput(nodes[1]);
        nodes[3].addInput(nodes[1]);
        nodes[4].addInput(nodes[3]);
        nodes[5].addInput(nodes[2]);
        nodes[5].addInput(nodes[4]);
        nodes[6].addInput(nodes[3]);
        nodes[7].addInput(nodes[6]);

        return nodes;
    }

    private Tuple2<TopologyGraph, TestingBatchExecNode[]> buildTopologyGraph() {
        TestingBatchExecNode[] nodes = buildLinkedNodes();

        return Tuple2.of(new TopologyGraph(Arrays.asList(nodes[5], nodes[7])), nodes);
    }

    private Tuple2<TopologyGraph, TestingBatchExecNode[]> buildBoundedTopologyGraph() {
        // bounded at nodes 2 and 3
        TestingBatchExecNode[] nodes = buildLinkedNodes();

        return Tuple2.of(
                new TopologyGraph(
                        Arrays.asList(nodes[5], nodes[7]),
                        new HashSet<>(Arrays.asList(nodes[2], nodes[3]))),
                nodes);
    }

    @Test
    public void testCanReach() {
        Tuple2<TopologyGraph, TestingBatchExecNode[]> tuple2 = buildTopologyGraph();
        TopologyGraph graph = tuple2.f0;
        TestingBatchExecNode[] nodes = tuple2.f1;

        String[] canReach =
                new String[] {
                    "11111111",
                    "01111111",
                    "00100100",
                    "00011111",
                    "00001100",
                    "00000100",
                    "00000011",
                    "00000001"
                };
        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 8; j++) {
                if (canReach[i].charAt(j) == '1') {
                    Assertions.assertTrue(graph.canReach(nodes[i], nodes[j]));
                } else {
                    Assertions.assertFalse(graph.canReach(nodes[i], nodes[j]));
                }
            }
        }
    }

    @Test
    public void testLink() {
        Tuple2<TopologyGraph, TestingBatchExecNode[]> tuple2 = buildTopologyGraph();
        TopologyGraph graph = tuple2.f0;
        TestingBatchExecNode[] nodes = tuple2.f1;

        Assertions.assertTrue(graph.link(nodes[2], nodes[4]));
        Assertions.assertTrue(graph.link(nodes[3], nodes[5]));
        Assertions.assertTrue(graph.link(nodes[5], nodes[6]));
        Assertions.assertFalse(graph.link(nodes[7], nodes[2]));
        Assertions.assertFalse(graph.link(nodes[7], nodes[4]));
        Assertions.assertTrue(graph.link(nodes[0], nodes[7]));
    }

    @Test
    public void testUnlink() {
        Tuple2<TopologyGraph, TestingBatchExecNode[]> tuple2 = buildTopologyGraph();
        TopologyGraph graph = tuple2.f0;
        TestingBatchExecNode[] nodes = tuple2.f1;

        graph.unlink(nodes[2], nodes[5]);
        Assertions.assertTrue(graph.canReach(nodes[0], nodes[5]));
        graph.unlink(nodes[4], nodes[5]);
        Assertions.assertFalse(graph.canReach(nodes[0], nodes[5]));
        graph.unlink(nodes[3], nodes[6]);
        Assertions.assertFalse(graph.canReach(nodes[0], nodes[7]));
    }

    @Test
    public void testCalculateMaximumDistance() {
        Tuple2<TopologyGraph, TestingBatchExecNode[]> tuple2 = buildTopologyGraph();
        TopologyGraph graph = tuple2.f0;
        TestingBatchExecNode[] nodes = tuple2.f1;

        Map<ExecNode<?>, Integer> result = graph.calculateMaximumDistance();
        Assertions.assertEquals(8, result.size());
        Assertions.assertEquals(0, result.get(nodes[0]).intValue());
        Assertions.assertEquals(1, result.get(nodes[1]).intValue());
        Assertions.assertEquals(2, result.get(nodes[2]).intValue());
        Assertions.assertEquals(2, result.get(nodes[3]).intValue());
        Assertions.assertEquals(3, result.get(nodes[4]).intValue());
        Assertions.assertEquals(3, result.get(nodes[6]).intValue());
        Assertions.assertEquals(4, result.get(nodes[5]).intValue());
        Assertions.assertEquals(4, result.get(nodes[7]).intValue());
    }

    @Test
    public void testBoundedCalculateMaximumDistance() {
        Tuple2<TopologyGraph, TestingBatchExecNode[]> tuple2 = buildBoundedTopologyGraph();
        TopologyGraph graph = tuple2.f0;
        TestingBatchExecNode[] nodes = tuple2.f1;

        Map<ExecNode<?>, Integer> result = graph.calculateMaximumDistance();
        Assertions.assertEquals(6, result.size());
        Assertions.assertEquals(0, result.get(nodes[2]).intValue());
        Assertions.assertEquals(0, result.get(nodes[3]).intValue());
        Assertions.assertEquals(1, result.get(nodes[4]).intValue());
        Assertions.assertEquals(1, result.get(nodes[6]).intValue());
        Assertions.assertEquals(2, result.get(nodes[5]).intValue());
        Assertions.assertEquals(2, result.get(nodes[7]).intValue());
    }

    @Test
    public void testMakeAsFarAs() {
        Tuple2<TopologyGraph, TestingBatchExecNode[]> tuple2 = buildTopologyGraph();
        TopologyGraph graph = tuple2.f0;
        TestingBatchExecNode[] nodes = tuple2.f1;

        graph.makeAsFarAs(nodes[4], nodes[7]);
        Map<ExecNode<?>, Integer> distances = graph.calculateMaximumDistance();
        Assertions.assertEquals(4, distances.get(nodes[7]).intValue());
        Assertions.assertEquals(4, distances.get(nodes[4]).intValue());
    }
}
