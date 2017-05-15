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

package org.apache.flink.graph.library.similarity;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.similarity.AdamicAdar.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AdamicAdarTest
extends AsmTestBase {

	private float[] ilog = {
		1.0f / (float)Math.log(2),
		1.0f / (float)Math.log(3),
		1.0f / (float)Math.log(3),
		1.0f / (float)Math.log(4),
		1.0f / (float)Math.log(1),
		1.0f / (float)Math.log(1)
	};

	@Test
	public void testSimpleGraph()
			throws Exception {
		DataSet<Result<IntValue>> aa = undirectedSimpleGraph
			.run(new AdamicAdar<IntValue, NullValue, NullValue>());

		String expectedResult =
			"(0,1," + ilog[2] + ")\n" +
			"(0,2," + ilog[1] + ")\n" +
			"(0,3," + (ilog[1] + ilog[2]) + ")\n" +
			"(1,2," + (ilog[0] + ilog[3]) + ")\n" +
			"(1,3," + ilog[2] + ")\n" +
			"(1,4," + ilog[3] + ")\n" +
			"(1,5," + ilog[3] + ")\n" +
			"(2,3," + ilog[1] + ")\n" +
			"(2,4," + ilog[3] + ")\n" +
			"(2,5," + ilog[3] + ")\n" +
			"(4,5," + ilog[3] + ")";

		TestBaseUtils.compareResultAsText(aa.collect(), expectedResult);
	}

	@Test
	public void testSimpleGraphWithMinimumScore()
			throws Exception {
		DataSet<Result<IntValue>> aa = undirectedSimpleGraph
			.run(new AdamicAdar<IntValue, NullValue, NullValue>()
				.setMinimumScore(0.75f));

		String expectedResult =
			"(0,1," + ilog[2] + ")\n" +
			"(0,2," + ilog[1] + ")\n" +
			"(0,3," + (ilog[1] + ilog[2]) + ")\n" +
			"(1,2," + (ilog[0] + ilog[3]) + ")\n" +
			"(1,3," + ilog[2] + ")\n" +
			"(2,3," + ilog[1] + ")";

		TestBaseUtils.compareResultAsText(aa.collect(), expectedResult);
	}

	@Test
	public void testSimpleGraphWithMinimumRatio()
			throws Exception {
		DataSet<Result<IntValue>> aa = undirectedSimpleGraph
			.run(new AdamicAdar<IntValue, NullValue, NullValue>()
				.setMinimumRatio(1.5f));

		String expectedResult =
			"(0,3," + (ilog[1] + ilog[2]) + ")\n" +
			"(1,2," + (ilog[0] + ilog[3]) + ")";

		TestBaseUtils.compareResultAsText(aa.collect(), expectedResult);
	}

	@Test
	public void testCompleteGraph()
			throws Exception {
		float expectedScore = (completeGraphVertexCount - 2) / (float)Math.log(completeGraphVertexCount - 1);

		DataSet<Result<LongValue>> aa = completeGraph
			.run(new AdamicAdar<LongValue, NullValue, NullValue>());

		for (Result<LongValue> result : aa.collect()) {
			assertEquals(expectedScore, result.getAdamicAdarScore().getValue(), 0.00001);
		}
	}

	@Test
	public void testRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> aa = undirectedRMatGraph(8, 8)
			.run(new AdamicAdar<LongValue, NullValue, NullValue>());

		assertEquals(13954, aa.count());
	}
}
