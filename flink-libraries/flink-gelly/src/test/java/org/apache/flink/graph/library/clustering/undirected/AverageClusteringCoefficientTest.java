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

import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AverageClusteringCoefficientTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		// see results in LocalClusteringCoefficientTest.testSimpleGraph
		Result expectedResult = new Result(6, 1.0/1 + 2.0/3 + 2.0/3 + 1.0/6);

		Result averageClusteringCoefficient = new AverageClusteringCoefficient<IntValue, NullValue, NullValue>()
			.run(undirectedSimpleGraph)
			.execute();

		assertEquals(expectedResult, averageClusteringCoefficient);
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		Result expectedResult = new Result(completeGraphVertexCount, completeGraphVertexCount);

		Result averageClusteringCoefficient = new AverageClusteringCoefficient<LongValue, NullValue, NullValue>()
			.run(completeGraph)
			.execute();

		assertEquals(expectedResult, averageClusteringCoefficient);
	}

	@Test
	public void testWithEmptyGraph()
			throws Exception {
		Result expectedResult = new Result(emptyGraphVertexCount, 0);

		Result averageClusteringCoefficient = new AverageClusteringCoefficient<LongValue, NullValue, NullValue>()
			.run(emptyGraph)
			.execute();

		assertEquals(expectedResult, averageClusteringCoefficient);
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		Result expectedResult = new Result(902, 380.40109);

		Result averageClusteringCoefficient = new AverageClusteringCoefficient<LongValue, NullValue, NullValue>()
			.run(undirectedRMatGraph)
			.execute();

		assertEquals(expectedResult.getNumberOfVertices(), averageClusteringCoefficient.getNumberOfVertices());
		assertEquals(expectedResult.getAverageClusteringCoefficient(), averageClusteringCoefficient.getAverageClusteringCoefficient(), 0.000001);
	}
}
