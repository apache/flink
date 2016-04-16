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

package org.apache.flink.graph.asm.degree.annotate.undirected;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VertexDegreeTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		String expectedResult =
			"(0,2)\n" +
			"(1,3)\n" +
			"(2,3)\n" +
			"(3,4)\n" +
			"(4,1)\n" +
			"(5,1)";

		DataSet<Vertex<IntValue,LongValue>> sourceDegrees = undirectedSimpleGraph
			.run(new VertexDegree<IntValue, NullValue, NullValue>());

		TestBaseUtils.compareResultAsText(sourceDegrees.collect(), expectedResult);

		DataSet<Vertex<IntValue,LongValue>> targetDegrees = undirectedSimpleGraph
			.run(new VertexDegree<IntValue, NullValue, NullValue>()
			.setReduceOnTargetLabel(true));

		TestBaseUtils.compareResultAsText(targetDegrees.collect(), expectedResult);
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;

		DataSet<Vertex<LongValue,LongValue>> sourceDegrees = completeGraph
			.run(new VertexDegree<LongValue, NullValue, NullValue>());

		for (Vertex<LongValue,LongValue> vertex : sourceDegrees.collect()) {
			assertEquals(expectedDegree, vertex.getValue().getValue());
		}

		DataSet<Vertex<LongValue,LongValue>> targetDegrees = completeGraph
			.run(new VertexDegree<LongValue, NullValue, NullValue>()
				.setReduceOnTargetLabel(true));

		for (Vertex<LongValue,LongValue> vertex : targetDegrees.collect()) {
			assertEquals(expectedDegree, vertex.getValue().getValue());
		}
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		ChecksumHashCode sourceDegreeChecksum = DataSetUtils.checksumHashCode(undirectedRMatGraph
			.run(new VertexDegree<LongValue, NullValue, NullValue>()));

		assertEquals(902, sourceDegreeChecksum.getCount());
		assertEquals(0x0000000000e1fb30L, sourceDegreeChecksum.getChecksum());

		ChecksumHashCode targetDegreeChecksum = DataSetUtils.checksumHashCode(undirectedRMatGraph
			.run(new VertexDegree<LongValue, NullValue, NullValue>()
				.setReduceOnTargetLabel(true)));

		assertEquals(sourceDegreeChecksum, targetDegreeChecksum);
	}
}
