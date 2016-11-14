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

package org.apache.flink.graph.library.link_analysis;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.link_analysis.HITS.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HITSTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		DataSet<Result<IntValue>> hits = new HITS<IntValue, NullValue, NullValue>(10)
			.run(directedSimpleGraph);

		List<Tuple2<Double, Double>> expectedResults = new ArrayList<>();
		expectedResults.add(Tuple2.of(0.5446287864731747, 0.0));
		expectedResults.add(Tuple2.of(0.0, 0.8363240238999012));
		expectedResults.add(Tuple2.of(0.6072453524686667,0.26848532437604833));
		expectedResults.add(Tuple2.of(0.5446287864731747,0.39546603929699625));
		expectedResults.add(Tuple2.of(0.0, 0.26848532437604833));
		expectedResults.add(Tuple2.of(0.194966796646811, 0.0));

		for (Result<IntValue> result : hits.collect()) {
			int id = result.f0.getValue();
			assertEquals(expectedResults.get(id).f0, result.getHubScore().getValue(), 0.000001);
			assertEquals(expectedResults.get(id).f1, result.getAuthorityScore().getValue(), 0.000001);
		}
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		double expectedScore = 1.0 / Math.sqrt(completeGraphVertexCount);

		DataSet<Result<LongValue>> hits = new HITS<LongValue, NullValue, NullValue>(0.000001)
			.run(completeGraph);

		List<Result<LongValue>> results = hits.collect();

		assertEquals(completeGraphVertexCount, results.size());

		for (Result<LongValue> result : results) {
			assertEquals(expectedScore, result.getHubScore().getValue(), 0.000001);
			assertEquals(expectedScore, result.getAuthorityScore().getValue(), 0.000001);
		}
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		ChecksumHashCode checksum = DataSetUtils.checksumHashCode(directedRMatGraph
			.run(new HITS<LongValue, NullValue, NullValue>(0.000001)));

		assertEquals(902, checksum.getCount());
		assertEquals(0x000001cbba6dbcd0L, checksum.getChecksum());
	}
}
