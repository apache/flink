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

package org.apache.flink.graph.asm.degree.annotate.directed;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EdgeDegreesPairTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		String expectedResult =
			"(0,1,((null),(2,2,0),(3,2,1)))\n" +
			"(0,2,((null),(2,2,0),(3,1,2)))\n" +
			"(1,2,((null),(3,2,1),(3,1,2)))\n" +
			"(1,3,((null),(3,2,1),(4,2,2)))\n" +
			"(2,3,((null),(3,1,2),(4,2,2)))\n" +
			"(3,4,((null),(4,2,2),(1,0,1)))\n" +
			"(3,5,((null),(4,2,2),(1,0,1)))";

		DataSet<Edge<IntValue, Tuple3<NullValue, Degrees, Degrees>>> degrees = directedSimpleGraph
			.run(new EdgeDegreesPair<IntValue, NullValue, NullValue>());

		TestBaseUtils.compareResultAsText(degrees.collect(), expectedResult);
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		ChecksumHashCode degreeChecksum = DataSetUtils.checksumHashCode(directedRMatGraph
			.run(new EdgeDegreesPair<LongValue, NullValue, NullValue>()));

		assertEquals(16384, degreeChecksum.getCount());
		assertEquals(0x00001f68dfabd17cL, degreeChecksum.getChecksum());
	}
}
