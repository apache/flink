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

package org.apache.flink.graph.drivers;

import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link ClusteringCoefficient}.
 */
@RunWith(Parameterized.class)
public class ClusteringCoefficientITCase
extends CopyableValueDriverBaseITCase {

	public ClusteringCoefficientITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String order, String simplify, String output) {
		return new String[] {
			"--algorithm", "ClusteringCoefficient", "--order", order,
			"--input", "RMatGraph", "--scale", Integer.toString(scale), "--type", idType, "--simplify", simplify,
			"--output", output};
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new ClusteringCoefficient().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "ClusteringCoefficient"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testHashWithSmallDirectedRMatGraph() throws Exception {
		String expected = "\n" +
			new Checksum(117, 0x0000003621c62ca1L) + "\n\n" +
			"triplet count: 29286, triangle count: 11466, global clustering coefficient: 0.39151813[0-9]+\n" +
			"vertex count: 117, average clustering coefficient: 0.45125697[0-9]+\n";

		expectedOutput(parameters(7, "directed", "directed", "hash"), expected);
	}

	@Test
	public void testHashWithSmallUndirectedRMatGraph() throws Exception {
		String expected = "\n\n" +
			"triplet count: 29286, triangle count: 11466, global clustering coefficient: 0.39151813[0-9]+\n" +
			"vertex count: 117, average clustering coefficient: 0.57438679[0-9]+\n";

		expectedOutput(parameters(7, "directed", "undirected", "hash"),
			"\n" + new Checksum(117, 0x0000003875b38c43L) + expected);
		expectedOutput(parameters(7, "undirected", "undirected", "hash"),
			"\n" + new Checksum(117, 0x0000003c20344c75L) + expected);
	}

	@Test
	public void testHashWithLargeDirectedRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		// skip 'byte' which cannot store vertex IDs for scale > 8
		Assume.assumeFalse(idType.equals("byte") || idType.equals("nativeByte"));

		String expected = "\n" +
			new Checksum(3349, 0x0000067a9d18e7f3L) + "\n\n" +
			"triplet count: 9276207, triangle count: 1439454, global clustering coefficient: 0.15517700[0-9]+\n" +
			"vertex count: 3349, average clustering coefficient: 0.24571815[0-9]+\n";

		expectedOutput(parameters(12, "directed", "directed", "hash"), expected);
	}

	@Test
	public void testHashWithLargeUndirectedRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		// skip 'byte' which cannot store vertex IDs for scale > 8
		Assume.assumeFalse(idType.equals("byte") || idType.equals("nativeByte"));

		String expected = "\n\n" +
			"triplet count: 9276207, triangle count: 1439454, global clustering coefficient: 0.15517700[0-9]+\n" +
			"vertex count: 3349, average clustering coefficient: 0.33029442[0-9]+\n";

		expectedOutput(parameters(12, "directed", "undirected", "hash"),
			"\n" + new Checksum(3349, 0x00000681fad1587eL) + expected);
		expectedOutput(parameters(12, "undirected", "undirected", "hash"),
			"\n" + new Checksum(3349, 0x0000068713b3b7f1L) + expected);
	}
}
