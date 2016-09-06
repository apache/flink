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

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.graph.library.similarity.JaccardIndex.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JaccardIndexTest
extends AsmTestBase {

	@Test
	public void testSimpleGraph()
			throws Exception {
		DataSet<Result<IntValue>> ji = undirectedSimpleGraph
			.run(new JaccardIndex<IntValue, NullValue, NullValue>());

		String expectedResult =
			"(0,1,(1,4))\n" +
			"(0,2,(1,4))\n" +
			"(0,3,(2,4))\n" +
			"(1,2,(2,4))\n" +
			"(1,3,(1,6))\n" +
			"(1,4,(1,3))\n" +
			"(1,5,(1,3))\n" +
			"(2,3,(1,6))\n" +
			"(2,4,(1,3))\n" +
			"(2,5,(1,3))\n" +
			"(4,5,(1,1))\n";

		TestBaseUtils.compareResultAsText(ji.collect(), expectedResult);
	}

	@Test
	public void testSimpleGraphWithMinimumScore()
			throws Exception {
		DataSet<Result<IntValue>> ji = undirectedSimpleGraph
			.run(new JaccardIndex<IntValue, NullValue, NullValue>()
				.setMinimumScore(1, 2));

		String expectedResult =
			"(0,3,(2,4))\n" +
			"(1,2,(2,4))\n" +
			"(4,5,(1,1))\n";

		TestBaseUtils.compareResultAsText(ji.collect(), expectedResult);
	}

	@Test
	public void testSimpleGraphWithMaximumScore()
			throws Exception {
		DataSet<Result<IntValue>> ji = undirectedSimpleGraph
			.run(new JaccardIndex<IntValue, NullValue, NullValue>()
				.setMaximumScore(1, 2));

		String expectedResult =
			"(0,1,(1,4))\n" +
			"(0,2,(1,4))\n" +
			"(1,3,(1,6))\n" +
			"(1,4,(1,3))\n" +
			"(1,5,(1,3))\n" +
			"(2,3,(1,6))\n" +
			"(2,4,(1,3))\n" +
			"(2,5,(1,3))\n";

		TestBaseUtils.compareResultAsText(ji.collect(), expectedResult);
	}

	@Test
	public void testCompleteGraph()
			throws Exception {
		DataSet<Result<LongValue>> ji = completeGraph
			.run(new JaccardIndex<LongValue, NullValue, NullValue>()
				.setGroupSize(4));

		for (Result<LongValue> result : ji.collect()) {
			// the intersection includes every vertex
			assertEquals(completeGraphVertexCount, result.getDistinctNeighborCount().getValue());

			// the union only excludes the two vertices from the similarity score
			assertEquals(completeGraphVertexCount - 2, result.getSharedNeighborCount().getValue());
		}
	}

	@Test
	public void testRMatGraph()
			throws Exception {
		long vertexCount = 1 << 8;
		long edgeCount = 8 * vertexCount;

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
			.generate()
			.run(new Simplify<LongValue, NullValue, NullValue>(false));

		DataSet<Result<LongValue>> ji = graph
			.run(new JaccardIndex<LongValue, NullValue, NullValue>()
				.setGroupSize(4));

		ChecksumHashCode checksum = DataSetUtils.checksumHashCode(ji);

		assertEquals(13954, checksum.getCount());
		assertEquals(0x00001b1a1f7a9d0bL, checksum.getChecksum());
	}
}
