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

package org.apache.flink.graph.library.clustering.undirected;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GlobalClusteringCoefficientTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		Result expectedResult = new Result(13, 6);

		Result globalClusteringCoefficient = new GlobalClusteringCoefficient<IntValue, NullValue, NullValue>()
			.run(undirectedSimpleGraph)
			.execute();

		assertEquals(expectedResult, globalClusteringCoefficient);
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long expectedCount = completeGraphVertexCount * CombinatoricsUtils.binomialCoefficient((int)expectedDegree, 2);

		Result expectedResult = new Result(expectedCount, expectedCount);

		Result globalClusteringCoefficient = new GlobalClusteringCoefficient<LongValue, NullValue, NullValue>()
			.run(completeGraph)
			.execute();

		assertEquals(expectedResult, globalClusteringCoefficient);
	}

	@Test
	public void testWithEmptyGraph()
			throws Exception {
		Result expectedResult = new Result(0, 0);

		Result globalClusteringCoefficient = new GlobalClusteringCoefficient<LongValue, NullValue, NullValue>()
			.run(emptyGraph)
			.execute();

		assertEquals(expectedResult, globalClusteringCoefficient);
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		Result expectedResult = new Result(1003442, 225147);

		Result globalClusteringCoefficient = new GlobalClusteringCoefficient<LongValue, NullValue, NullValue>()
			.run(undirectedRMatGraph(10, 16))
			.execute();

		assertEquals(expectedResult, globalClusteringCoefficient);
	}
}
