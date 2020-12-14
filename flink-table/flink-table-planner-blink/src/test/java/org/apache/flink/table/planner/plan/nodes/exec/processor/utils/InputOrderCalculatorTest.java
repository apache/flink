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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

/**
 * Tests for {@link InputOrderCalculator}.
 */
public class InputOrderCalculatorTest {

	@Test
	public void testCheckPipelinedPath() {
		// P = ExecEdge.DamBehavior.PIPELINED, E = ExecEdge.DamBehavior.END_INPUT B = ExecEdge.DamBehavior.BLOCKING
		//
		// 0 -P-> 1 ----E----\
		//         \-P-\      \
		// 2 ----E----> 3 -P-> 4
		// 5 -P-> 6 -B-/
		TestingBatchExecNode[] nodes = new TestingBatchExecNode[7];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = new TestingBatchExecNode();
		}
		nodes[1].addInput(nodes[0]);
		nodes[3].addInput(nodes[1]);
		nodes[3].addInput(nodes[2], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.END_INPUT).build());
		nodes[3].addInput(nodes[6], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).build());
		nodes[4].addInput(nodes[1], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.END_INPUT).build());
		nodes[4].addInput(nodes[3]);
		nodes[6].addInput(nodes[5]);

		Assert.assertFalse(
			InputOrderCalculator.checkPipelinedPath(nodes[4], new HashSet<>(
				Arrays.asList(nodes[2], nodes[5], nodes[6]))));
		Assert.assertTrue(
			InputOrderCalculator.checkPipelinedPath(nodes[4], new HashSet<>(
				Arrays.asList(nodes[0], nodes[2]))));
	}

	@Test
	public void testCalculateInputOrder() {
		// P = ExecEdge.DamBehavior.PIPELINED, B = ExecEdge.DamBehavior.BLOCKING
		// P1 = PIPELINED + priority 1
		//
		// 0 -(P1)-> 3 -(B0)-\
		//                    6 -(B0)-\
		//            /-(P1)-/         \
		// 1 -(P1)-> 4                  8
		//            \-(B0)-\         /
		//                    7 -(P1)-/
		// 2 -(P1)-> 5 -(P1)-/
		TestingBatchExecNode[] nodes = new TestingBatchExecNode[9];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = new TestingBatchExecNode();
		}
		nodes[3].addInput(nodes[0], ExecEdge.builder().priority(1).build());
		nodes[4].addInput(nodes[1], ExecEdge.builder().priority(1).build());
		nodes[5].addInput(nodes[2], ExecEdge.builder().priority(1).build());
		nodes[6].addInput(nodes[3], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).priority(0).build());
		nodes[6].addInput(nodes[4], ExecEdge.builder().priority(1).build());
		nodes[7].addInput(nodes[4], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).priority(0).build());
		nodes[7].addInput(nodes[5], ExecEdge.builder().priority(1).build());
		nodes[8].addInput(nodes[6], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).priority(0).build());
		nodes[8].addInput(nodes[7], ExecEdge.builder().priority(1).build());

		InputOrderCalculator calculator = new InputOrderCalculator(
			nodes[8],
			new HashSet<>(Arrays.asList(nodes[1], nodes[3], nodes[5])),
			ExecEdge.DamBehavior.BLOCKING);
		Map<ExecNode<?>, Integer> result = calculator.calculate();
		Assert.assertEquals(3, result.size());
		Assert.assertEquals(0, result.get(nodes[3]).intValue());
		Assert.assertEquals(1, result.get(nodes[1]).intValue());
		Assert.assertEquals(2, result.get(nodes[5]).intValue());
	}

	@Test
	public void testCalculateInputOrderWithRelatedBoundaries() {
		// P = ExecEdge.DamBehavior.PIPELINED, B = ExecEdge.DamBehavior.BLOCKING
		// P1 = PIPELINED + priority 1
		//
		// /------------(P0)------------\
		// 0 -(P0)-> 1 -(B0)-> 2 -(P0)-> 4 -(P1)-> 5
		//           3 -(P1)-/           6 -(B0)-/
		TestingBatchExecNode[] nodes = new TestingBatchExecNode[7];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = new TestingBatchExecNode();
		}
		nodes[1].addInput(nodes[0]);
		nodes[2].addInput(nodes[1], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).build());
		nodes[2].addInput(nodes[3], ExecEdge.builder().priority(1).build());
		nodes[4].addInput(nodes[0]);
		nodes[4].addInput(nodes[2]);
		nodes[5].addInput(nodes[4], ExecEdge.builder().priority(1).build());
		nodes[5].addInput(nodes[6], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).build());

		InputOrderCalculator calculator = new InputOrderCalculator(
			nodes[5],
			new HashSet<>(Arrays.asList(nodes[0], nodes[1], nodes[3], nodes[6])),
			ExecEdge.DamBehavior.BLOCKING);
		Map<ExecNode<?>, Integer> result = calculator.calculate();
		Assert.assertEquals(4, result.size());
		Assert.assertEquals(1, result.get(nodes[0]).intValue());
		Assert.assertEquals(1, result.get(nodes[1]).intValue());
		Assert.assertEquals(2, result.get(nodes[3]).intValue());
		Assert.assertEquals(0, result.get(nodes[6]).intValue());
	}

	@Test
	public void testCalculateInputOrderWithUnaffectedRelatedBoundaries() {
		// P = ExecEdge.DamBehavior.PIPELINED, B = ExecEdge.DamBehavior.BLOCKING
		// P1 = PIPELINED + priority 1
		//
		// 0 --(P0)-> 1 -------(B0)-----> 2 -(P0)-\
		//  \          \--(B0)-> 3 -(P1)-/         4
		//   \-(B0)-> 5 -------(P1)-----> 6 -(P0)-/
		//                     7 --(B0)--/
		TestingBatchExecNode[] nodes = new TestingBatchExecNode[8];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = new TestingBatchExecNode();
		}
		nodes[1].addInput(nodes[0]);
		nodes[2].addInput(nodes[1], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).build());
		nodes[2].addInput(nodes[3], ExecEdge.builder().priority(1).build());
		nodes[3].addInput(nodes[1], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).build());
		nodes[4].addInput(nodes[2]);
		nodes[4].addInput(nodes[6]);
		nodes[5].addInput(nodes[0], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).build());
		nodes[6].addInput(nodes[5], ExecEdge.builder().priority(1).build());
		nodes[6].addInput(nodes[7], ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.BLOCKING).build());

		InputOrderCalculator calculator = new InputOrderCalculator(
			nodes[4],
			new HashSet<>(Arrays.asList(nodes[1], nodes[3], nodes[5], nodes[7])),
			ExecEdge.DamBehavior.BLOCKING);
		Map<ExecNode<?>, Integer> result = calculator.calculate();
		Assert.assertEquals(4, result.size());
		Assert.assertEquals(0, result.get(nodes[1]).intValue());
		Assert.assertEquals(1, result.get(nodes[3]).intValue());
		Assert.assertEquals(1, result.get(nodes[5]).intValue());
		Assert.assertEquals(0, result.get(nodes[7]).intValue());
	}

	@Test(expected = IllegalStateException.class)
	public void testCalculateInputOrderWithLoop() {
		TestingBatchExecNode a = new TestingBatchExecNode();
		TestingBatchExecNode b = new TestingBatchExecNode();
		for (int i = 0; i < 2; i++) {
			b.addInput(a, ExecEdge.builder().priority(i).build());
		}

		InputOrderCalculator calculator = new InputOrderCalculator(
			b,
			Collections.emptySet(),
			ExecEdge.DamBehavior.BLOCKING);
		calculator.calculate();
	}
}
