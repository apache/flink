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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LocalClusteringCoefficient}.
 */
public class LocalClusteringCoefficientTest
extends AsmTestBase {

	@Test
	public void testSimpleGraph()
			throws Exception {
		String expectedResult =
			"(0,2,1)\n" +
			"(1,3,2)\n" +
			"(2,3,2)\n" +
			"(3,4,1)\n" +
			"(4,1,0)\n" +
			"(5,1,0)";

		DataSet<Result<IntValue>> cc = undirectedSimpleGraph
			.run(new LocalClusteringCoefficient<>());

		TestBaseUtils.compareResultAsText(cc.collect(), expectedResult);
	}

	@Test
	public void testCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long expectedTriangleCount = CombinatoricsUtils.binomialCoefficient((int) expectedDegree, 2);

		DataSet<Result<LongValue>> cc = completeGraph
			.run(new LocalClusteringCoefficient<>());

		List<Result<LongValue>> results = cc.collect();

		assertEquals(completeGraphVertexCount, results.size());

		for (Result<LongValue> result : results) {
			assertEquals(expectedDegree, result.getDegree().getValue());
			assertEquals(expectedTriangleCount, result.getTriangleCount().getValue());
		}
	}

	@Test
	public void testRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> cc = undirectedRMatGraph(10, 16)
			.run(new LocalClusteringCoefficient<>());

		Checksum checksum = new ChecksumHashCode<Result<LongValue>>()
			.run(cc)
			.execute();

		assertEquals(902, checksum.getCount());
		assertEquals(0x000001cab2d3677bL, checksum.getChecksum());
	}
}
