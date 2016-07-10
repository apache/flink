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

package org.apache.flink.graph.library.clustering.directed;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TriangleCountTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		long triangleCount = new TriangleCount<IntValue, NullValue, NullValue>()
			.run(directedSimpleGraph)
			.execute();

		assertEquals(2, triangleCount);
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long expectedCount = completeGraphVertexCount * CombinatoricsUtils.binomialCoefficient((int)expectedDegree, 2) / 3;

		long triangleCount = new TriangleCount<LongValue, NullValue, NullValue>()
			.run(completeGraph)
			.execute();

		assertEquals(expectedCount, triangleCount);
	}

	@Test
	public void testWithEmptyGraph()
			throws Exception {
		long triangleCount = new TriangleCount<LongValue, NullValue, NullValue>()
			.run(emptyGraph)
			.execute();

		assertEquals(0, triangleCount);
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		long triangleCount = new TriangleCount<LongValue, NullValue, NullValue>()
			.run(directedRMatGraph)
			.execute();

		assertEquals(75049, triangleCount);
	}
}
