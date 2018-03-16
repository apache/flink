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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.library.clustering.directed.TriangleListing.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TriangleListing}.
 */
public class TriangleListingTest extends AsmTestBase {

	@Test
	public void testSimpleGraphSorted() throws Exception {
		DataSet<Result<IntValue>> tl = directedSimpleGraph
			.run(new TriangleListing<IntValue, NullValue, NullValue>()
				.setSortTriangleVertices(true));

		String expectedResult =
			"(0,1,2,41)\n" +
			"(1,2,3,22)";

		TestBaseUtils.compareResultAsText(tl.collect(), expectedResult);
	}

	@Test
	public void testSimpleGraphPermuted() throws Exception {
		DataSet<Result<IntValue>> tl = directedSimpleGraph
			.run(new TriangleListing<IntValue, NullValue, NullValue>()
				.setPermuteResults(true));

		String expectedResult =
			// permutation of (0,1,2,41)
			"1st vertex ID: 0, 2nd vertex ID: 1, 3rd vertex ID: 2, edge directions: 0->1, 0->2, 1<-2\n" +
			"1st vertex ID: 0, 2nd vertex ID: 2, 3rd vertex ID: 1, edge directions: 0->2, 0->1, 2->1\n" +
			"1st vertex ID: 1, 2nd vertex ID: 0, 3rd vertex ID: 2, edge directions: 1<-0, 1<-2, 0->2\n" +
			"1st vertex ID: 1, 2nd vertex ID: 2, 3rd vertex ID: 0, edge directions: 1<-2, 1<-0, 2<-0\n" +
			"1st vertex ID: 2, 2nd vertex ID: 0, 3rd vertex ID: 1, edge directions: 2<-0, 2->1, 0->1\n" +
			"1st vertex ID: 2, 2nd vertex ID: 1, 3rd vertex ID: 0, edge directions: 2->1, 2<-0, 1<-0\n" +
			// permutation of (1,2,3,22)
			"1st vertex ID: 1, 2nd vertex ID: 2, 3rd vertex ID: 3, edge directions: 1<-2, 1<-3, 2->3\n" +
			"1st vertex ID: 1, 2nd vertex ID: 3, 3rd vertex ID: 2, edge directions: 1<-3, 1<-2, 3<-2\n" +
			"1st vertex ID: 2, 2nd vertex ID: 1, 3rd vertex ID: 3, edge directions: 2->1, 2->3, 1<-3\n" +
			"1st vertex ID: 2, 2nd vertex ID: 3, 3rd vertex ID: 1, edge directions: 2->3, 2->1, 3->1\n" +
			"1st vertex ID: 3, 2nd vertex ID: 1, 3rd vertex ID: 2, edge directions: 3->1, 3<-2, 1<-2\n" +
			"1st vertex ID: 3, 2nd vertex ID: 2, 3rd vertex ID: 1, edge directions: 3<-2, 3->1, 2->1";

		List<String> printableStrings = new ArrayList<>();

		for (Result<IntValue> result : tl.collect()) {
			printableStrings.add(result.toPrintableString());
		}

		TestBaseUtils.compareResultAsText(printableStrings, expectedResult);
	}

	@Test
	public void testCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long expectedCount = completeGraphVertexCount * CombinatoricsUtils.binomialCoefficient((int) expectedDegree, 2) / 3;

		DataSet<Result<LongValue>> tl = completeGraph
			.run(new TriangleListing<>());

		List<Result<LongValue>> results = tl.collect();

		assertEquals(expectedCount, results.size());

		for (Result<LongValue> result : results) {
			assertEquals(0b111111, result.getBitmask().getValue());
		}
	}

	@Test
	public void testWithEmptyGraphWithVertices() throws Exception {
		DataSet<Result<LongValue>> tl = emptyGraphWithVertices
			.run(new TriangleListing<>());

		assertEquals(0, tl.collect().size());
	}

	@Test
	public void testWithEmptyGraphWithoutVertices() throws Exception {
		DataSet<Result<LongValue>> tl = emptyGraphWithoutVertices
			.run(new TriangleListing<>());

		assertEquals(0, tl.collect().size());
	}

	@Test
	public void testRMatGraph() throws Exception {
		DataSet<Result<LongValue>> tl = directedRMatGraph(10, 16)
			.run(new TriangleListing<LongValue, NullValue, NullValue>()
				.setSortTriangleVertices(true));

		Checksum checksum = new ChecksumHashCode<Result<LongValue>>()
			.run(tl)
			.execute();

		assertEquals(75049, checksum.getCount());
		assertEquals(0x000092399c79299eL, checksum.getChecksum());
	}
}
