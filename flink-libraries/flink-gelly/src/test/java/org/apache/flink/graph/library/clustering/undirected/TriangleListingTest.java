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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TriangleListingTest
extends AsmTestBase {

	@Test
	public void testSimpleGraph()
			throws Exception {
		DataSet<Tuple3<IntValue, IntValue, IntValue>> tl = undirectedSimpleGraph
			.run(new TriangleListing<IntValue, NullValue, NullValue>()
				.setSortTriangleVertices(true));

		String expectedResult =
			"(0,1,2)\n" +
			"(1,2,3)";

		TestBaseUtils.compareResultAsText(tl.collect(), expectedResult);
	}

	@Test
	public void testCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long expectedCount = completeGraphVertexCount * CombinatoricsUtils.binomialCoefficient((int)expectedDegree, 2) / 3;

		DataSet<Tuple3<LongValue, LongValue, LongValue>> tl = completeGraph
			.run(new TriangleListing<LongValue, NullValue, NullValue>());

		ChecksumHashCode checksum = DataSetUtils.checksumHashCode(tl);

		assertEquals(expectedCount, checksum.getCount());
	}

	@Test
	public void testRMatGraph()
			throws Exception {
		DataSet<Tuple3<LongValue, LongValue, LongValue>> tl = undirectedRMatGraph
			.run(new TriangleListing<LongValue, NullValue, NullValue>()
				.setSortTriangleVertices(true));

		ChecksumHashCode checksum = DataSetUtils.checksumHashCode(tl);

		assertEquals(75049, checksum.getCount());
		assertEquals(0x00000001a5b500afL, checksum.getChecksum());
	}
}
