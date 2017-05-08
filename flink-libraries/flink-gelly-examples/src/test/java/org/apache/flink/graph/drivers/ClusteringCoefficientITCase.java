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
		long checksum;
		switch (idType) {
			case "byte":
			case "short":
			case "char":
			case "integer":
				checksum = 0x0000003621c62ca1L;
				break;

			case "long":
				checksum = 0x0000003b74c6719bL;
				break;

			case "string":
				checksum = 0x0000003ab67abea8L;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			new Checksum(117, checksum) + "\n" +
			"triplet count: 29286, triangle count: 11466, global clustering coefficient: 0.39151813[0-9]+\n" +
			"vertex count: 117, average clustering coefficient: 0.45125697[0-9]+\n";

		expectedOutput(parameters(7, "directed", "directed", "hash"), expected);
	}

	@Test
	public void testHashWithSmallUndirectedRMatGraph() throws Exception {
		long directed_checksum;
		long undirected_checksum;
		switch (idType) {
			case "byte":
			case "short":
			case "char":
			case "integer":
				directed_checksum = 0x0000003875b38c43L;
				undirected_checksum = 0x0000003c20344c75L;
				break;

			case "long":
				directed_checksum = 0x0000003671970c59L;
				undirected_checksum = 0x0000003939645d8cL;
				break;

			case "string":
				directed_checksum = 0x0000003be109a770L;
				undirected_checksum = 0x0000003b8c98d14aL;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			"triplet count: 29286, triangle count: 11466, global clustering coefficient: 0.39151813[0-9]+\n" +
			"vertex count: 117, average clustering coefficient: 0.57438679[0-9]+\n";

		expectedOutput(parameters(7, "directed", "undirected", "hash"),
			"\n" + new Checksum(117, directed_checksum) + expected);
		expectedOutput(parameters(7, "undirected", "undirected", "hash"),
			"\n" + new Checksum(117, undirected_checksum) + expected);
	}

	@Test
	public void testHashWithLargeDirectedRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		long checksum;
		switch (idType) {
			case "byte":
				return;

			case "short":
			case "char":
			case "integer":
				checksum = 0x0000067a9d18e7f3L;
				break;

			case "long":
				checksum = 0x00000694a90ee6d4L;
				break;

			case "string":
				checksum = 0x000006893e3b314fL;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			new Checksum(3349, checksum) + "\n" +
			"triplet count: 9276207, triangle count: 1439454, global clustering coefficient: 0.15517700[0-9]+\n" +
			"vertex count: 3349, average clustering coefficient: 0.24571815[0-9]+\n";

		expectedOutput(parameters(12, "directed", "directed", "hash"), expected);
	}

	@Test
	public void testHashWithLargeUndirectedRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		long directed_checksum;
		long undirected_checksum;
		switch (idType) {
			case "byte":
				return;

			case "short":
			case "char":
			case "integer":
				directed_checksum = 0x00000681fad1587eL;
				undirected_checksum = 0x0000068713b3b7f1L;
				break;

			case "long":
				directed_checksum = 0x000006928a6301b1L;
				undirected_checksum = 0x000006a399edf0e6L;
				break;

			case "string":
				directed_checksum = 0x000006749670a2f7L;
				undirected_checksum = 0x0000067f19c6c4d5L;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			"triplet count: 9276207, triangle count: 1439454, global clustering coefficient: 0.15517700[0-9]+\n" +
			"vertex count: 3349, average clustering coefficient: 0.33029442[0-9]+\n";

		expectedOutput(parameters(12, "directed", "undirected", "hash"),
			"\n" + new Checksum(3349, directed_checksum) + expected);
		expectedOutput(parameters(12, "undirected", "undirected", "hash"),
			"\n" + new Checksum(3349, undirected_checksum) + expected);
	}
}
