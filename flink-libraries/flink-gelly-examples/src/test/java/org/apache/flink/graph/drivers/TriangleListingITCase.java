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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TriangleListingITCase
extends CopyableValueDriverBaseITCase {

	public TriangleListingITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String order, String output) {
		String[] parameters =  new String[] {
			"--algorithm", "TriangleListing", "--order", order,
			"--input", "RMatGraph", "--scale", Integer.toString(scale), "--type", idType, "--simplify", order,
			"--output", output};

		if (output.equals("hash")) {
			return ArrayUtils.addAll(parameters, "--sort_triangle_vertices", "--triadic_census");
		} else {
			return parameters;
		}
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new TriangleListing().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "TriangleListing"},
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
				checksum = 0x000000003d2f0a9aL;
				break;

			case "long":
				checksum = 0x000000016aba3720L;
				break;

			case "string":
				checksum = 0x0000005bfef84facL;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			new Checksum(3822, checksum) + "\n" +
			"Triadic census:\n" +
			"  003: 178,989\n" +
			"  012: 47,736\n" +
			"  102: 11,763\n" +
			"  021d: 2,258\n" +
			"  021u: 2,064\n" +
			"  021c: 4,426\n" +
			"  111d: 3,359\n" +
			"  111u: 3,747\n" +
			"  030t: 624\n" +
			"  030c: 220\n" +
			"  201: 1,966\n" +
			"  120d: 352\n" +
			"  120u: 394\n" +
			"  120c: 704\n" +
			"  210: 1,120\n" +
			"  300: 408\n";

		expectedOutput(parameters(7, "directed", "hash"), expected);
	}

	@Test
	public void testHashWithSmallUndirectedRMatGraph() throws Exception {
		long checksum;
		switch (idType) {
			case "byte":
			case "short":
			case "char":
			case "integer":
				checksum = 0x0000000001f92b0cL;
				break;

			case "long":
				checksum = 0x000000000bb355c6L;
				break;

			case "string":
				checksum = 0x00000002f7b5576aL;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			new Checksum(3822, checksum) + "\n" +
			"Triadic census:\n" +
			"  03: 178,989\n" +
			"  12: 59,499\n" +
			"  21: 17,820\n" +
			"  30: 3,822\n";

		expectedOutput(parameters(7, "undirected", "hash"), expected);
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
				checksum = 0x00000248fef26209L;
				break;

			case "long":
				checksum = 0x000002dcdf0fbb1bL;
				break;

			case "string":
				checksum = 0x00035b760ab9da74L;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			new Checksum(479818, checksum) + "\n" +
			"Triadic census:\n" +
			"  003: 6,101,196,568\n" +
			"  012: 132,051,207\n" +
			"  102: 13,115,128\n" +
			"  021d: 1,330,423\n" +
			"  021u: 1,336,897\n" +
			"  021c: 2,669,285\n" +
			"  111d: 1,112,144\n" +
			"  111u: 1,097,452\n" +
			"  030t: 132,048\n" +
			"  030c: 44,127\n" +
			"  201: 290,552\n" +
			"  120d: 47,734\n" +
			"  120u: 47,780\n" +
			"  120c: 95,855\n" +
			"  210: 90,618\n" +
			"  300: 21,656\n";

		expectedOutput(parameters(12, "directed", "hash"), expected);
	}

	@Test
	public void testHashWithLargeUndirectedRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		long checksum;
		switch (idType) {
			case "byte":
				return;

			case "short":
			case "char":
			case "integer":
				checksum = 0x00000012dee4bf2cL;
				break;

			case "long":
				checksum = 0x00000017a40efbdaL;
				break;

			case "string":
				checksum = 0x000159e8be3e370bL;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			new Checksum(479818, checksum) + "\n" +
			"Triadic census:\n" +
			"  03: 6,101,196,568\n" +
			"  12: 145,166,335\n" +
			"  21: 7,836,753\n" +
			"  30: 479,818\n";

		expectedOutput(parameters(12, "undirected", "hash"), expected);
	}


	@Test
	public void testPrintWithSmallDirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		long checksum;
		switch (idType) {
			case "byte":
			case "short":
			case "integer":
			case "long":
				checksum = 0x00000764227995aaL;
				break;

			case "string":
				checksum = 0x000007643d93c30aL;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		expectedOutputChecksum(parameters(7, "directed", "print"), new Checksum(3822, checksum));
	}


	@Test
	public void testPrintWithSmallUndirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		long checksum;
		switch (idType) {
			case "byte":
			case "short":
			case "integer":
			case "long":
				checksum = 0x0000077d1582e206L;
				break;

			case "string":
				checksum = 0x0000077a49cb5268L;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		expectedOutputChecksum(parameters(7, "undirected", "print"), new Checksum(3822, checksum));
	}
}
