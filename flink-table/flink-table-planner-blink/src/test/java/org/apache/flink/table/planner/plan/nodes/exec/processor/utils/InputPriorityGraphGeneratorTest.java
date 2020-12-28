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

import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.TestingBatchExecNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Tests for {@link InputPriorityGraphGenerator}. */
public class InputPriorityGraphGeneratorTest {

    @Test
    public void testCalculatePipelinedAncestors() {
        // P = ExecEdge.DamBehavior.PIPELINED, E = ExecEdge.DamBehavior.END_INPUT
        //
        // 0 ------P----> 1 -E--> 2
        //   \-----P----> 3 -P-/
        // 4 -E-> 5 -P-/ /
        // 6 -----E-----/
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[7];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode();
        }
        nodes[1].addInput(nodes[0]);
        nodes[2].addInput(
                nodes[1], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.END_INPUT).build());
        nodes[2].addInput(nodes[3]);
        nodes[3].addInput(nodes[0]);
        nodes[3].addInput(nodes[5]);
        nodes[3].addInput(
                nodes[6], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.END_INPUT).build());
        nodes[5].addInput(
                nodes[4], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.END_INPUT).build());

        TestingInputPriorityConflictResolver resolver =
                new TestingInputPriorityConflictResolver(
                        Collections.singletonList(nodes[2]),
                        Collections.emptySet(),
                        ExecEdge.DamBehavior.END_INPUT);
        List<ExecNode<?>> ancestors = resolver.calculatePipelinedAncestors(nodes[2]);
        Assert.assertEquals(2, ancestors.size());
        Assert.assertTrue(ancestors.contains(nodes[0]));
        Assert.assertTrue(ancestors.contains(nodes[5]));
    }

    @Test
    public void testCalculateBoundedPipelinedAncestors() {
        // P = ExecEdge.DamBehavior.PIPELINED, E = ExecEdge.DamBehavior.END_INPUT
        //
        // 0 -P-> 1 -P-> 2
        // 3 -P-> 4 -E/
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[5];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode();
        }
        nodes[1].addInput(nodes[0]);
        nodes[2].addInput(nodes[1]);
        nodes[2].addInput(
                nodes[4], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.END_INPUT).build());
        nodes[4].addInput(nodes[3]);

        TestingInputPriorityConflictResolver resolver =
                new TestingInputPriorityConflictResolver(
                        Collections.singletonList(nodes[2]),
                        new HashSet<>(Collections.singleton(nodes[1])),
                        ExecEdge.DamBehavior.END_INPUT);
        List<ExecNode<?>> ancestors = resolver.calculatePipelinedAncestors(nodes[2]);
        Assert.assertEquals(1, ancestors.size());
        Assert.assertTrue(ancestors.contains(nodes[1]));
    }

    private static class TestingInputPriorityConflictResolver extends InputPriorityGraphGenerator {

        private TestingInputPriorityConflictResolver(
                List<ExecNode<?>> roots,
                Set<ExecNode<?>> boundaries,
                ExecEdge.DamBehavior safeDamBehavior) {
            super(roots, boundaries, safeDamBehavior);
        }

        @Override
        protected void resolveInputPriorityConflict(
                ExecNode<?> node, int higherInput, int lowerInput) {
            // do nothing
        }
    }
}
