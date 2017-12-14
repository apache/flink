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

package org.apache.flink.graph.library.metric;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.test.TestGraphUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ChecksumHashCode}.
 */
public class ChecksumHashCodeTest extends AsmTestBase {

	@Test
	public void testSmallGraph() throws Exception {
		Graph<Long, Long, Long> graph = Graph.fromDataSet(
			TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env),
			env);

		Checksum checksum = graph
			.run(new ChecksumHashCode<>())
			.execute();

		assertEquals(12, checksum.getCount());
		assertEquals(0x4cd1, checksum.getChecksum());
	}

	@Test
	public void testEmptyGraphWithVertices() throws Exception {
		Checksum checksum = emptyGraphWithVertices
			.run(new ChecksumHashCode<>())
			.execute();

		assertEquals(3, checksum.getCount());
		assertEquals(0x109b, checksum.getChecksum());
	}

	@Test
	public void testEmptyGraphWithoutVertices() throws Exception {
		Checksum checksum = emptyGraphWithoutVertices
			.run(new ChecksumHashCode<>())
			.execute();

		assertEquals(0, checksum.getCount());
		assertEquals(0, checksum.getChecksum());
	}
}
