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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EdgeTargetDegreeTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		String expectedResult =
			"(0,1,3)\n" +
			"(0,2,3)\n" +
			"(1,0,2)\n" +
			"(1,2,3)\n" +
			"(1,3,4)\n" +
			"(2,0,2)\n" +
			"(2,1,3)\n" +
			"(2,3,4)\n" +
			"(3,1,3)\n" +
			"(3,2,3)\n" +
			"(3,4,1)\n" +
			"(3,5,1)\n" +
			"(4,3,4)\n" +
			"(5,3,4)";

		DataSet<Edge<IntValue,LongValue>> sourceDegree = undirectedSimpleGraph
				.run(new EdgeTargetDegree<IntValue, NullValue, NullValue>());

		TestBaseUtils.compareResultAsText(sourceDegree.collect(), expectedResult);

		DataSet<Edge<IntValue,LongValue>> targetDegree = undirectedSimpleGraph
			.run(new EdgeTargetDegree<IntValue, NullValue, NullValue>()
				.setReduceOnSourceLabel(true));

		TestBaseUtils.compareResultAsText(targetDegree.collect(), expectedResult);
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		ChecksumHashCode sourceDegreeChecksum = DataSetUtils.checksumHashCode(undirectedRMatGraph
			.run(new EdgeSourceDegree<LongValue, NullValue, NullValue>()));

		assertEquals(20884, sourceDegreeChecksum.getCount());
		assertEquals(0x000000019b836f94L, sourceDegreeChecksum.getChecksum());

		ChecksumHashCode targetDegreeChecksum = DataSetUtils.checksumHashCode(undirectedRMatGraph
			.run(new EdgeTargetDegree<LongValue, NullValue, NullValue>()
				.setReduceOnSourceLabel(true)));

		assertEquals(sourceDegreeChecksum, targetDegreeChecksum);
	}
}
