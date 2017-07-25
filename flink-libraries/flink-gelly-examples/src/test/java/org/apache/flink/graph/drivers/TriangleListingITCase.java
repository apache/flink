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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link TriangleListing}.
 */
@RunWith(Parameterized.class)
public class TriangleListingITCase
extends CopyableValueDriverBaseITCase {

	public TriangleListingITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String order, String output) {
		String[] parameters =  new String[] {
			"--algorithm", "TriangleListing", "--order", order, "--permute_results",
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
		String expected = "\n" +
			new Checksum(22932, 0x00002cdfb573650dL) + "\n\n" +
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
		String expected = "\n" +
			new Checksum(22932, 0x00002c7fb95ec78eL) + "\n\n" +
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

		// skip 'byte' which cannot store vertex IDs for scale > 8
		Assume.assumeFalse(idType.equals("byte") || idType.equals("nativeByte"));

		String expected = "\n" +
			new Checksum(2878908, 0x0015f77b8984c7caL) + "\n\n" +
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

		// skip 'byte' which cannot store vertex IDs for scale > 8
		Assume.assumeFalse(idType.equals("byte") || idType.equals("nativeByte"));

		String expected = "\n" +
			new Checksum(2878908, 0x0015f51b1336df61L) + "\n\n" +
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

		expectedOutputChecksum(parameters(7, "directed", "print"), new Checksum(22932, 0x00002ce58634cf58L));
	}

	@Test
	public void testPrintWithSmallUndirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(parameters(7, "undirected", "print"), new Checksum(22932, 0x00002d208304e7c4L));
	}
}
