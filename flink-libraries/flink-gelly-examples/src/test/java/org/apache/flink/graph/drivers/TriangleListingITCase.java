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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TriangleListingITCase
extends DriverBaseITCase {

	public TriangleListingITCase(TestExecutionMode mode) {
		super(mode);
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
	public void testDirectedHashWithRMatIntegerGraph() throws Exception {
		String expected = "\n" +
			"ChecksumHashCode 0x0000001beffe6edd, count 75049\n" +
			"Triadic census:\n" +
			"  003: 113,435,893\n" +
			"  012: 6,632,528\n" +
			"  102: 983,535\n" +
			"  021d: 118,574\n" +
			"  021u: 118,566\n" +
			"  021c: 237,767\n" +
			"  111d: 129,773\n" +
			"  111u: 130,041\n" +
			"  030t: 16,981\n" +
			"  030c: 5,535\n" +
			"  201: 43,574\n" +
			"  120d: 7,449\n" +
			"  120u: 7,587\n" +
			"  120c: 15,178\n" +
			"  210: 17,368\n" +
			"  300: 4,951\n";

		expectedOutput(
			new String[]{"--algorithm", "TriangleListing", "--order", "directed", "--sort_triangle_vertices", "--triadic_census",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "directed",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testDirectedPrintWithRMatIntegerGraph() throws Exception {
		expectedCount(
			new String[]{"--algorithm", "TriangleListing", "--order", "directed",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "directed",
				"--output", "print"},
			75049);
	}

	@Test
	public void testUndirectedHashWithRMatIntegerGraph() throws Exception {
		String expected = "\n" +
			"ChecksumHashCode 0x00000000e6b3f32c, count 75049\n" +
			"Triadic census:\n" +
			"  03: 113,435,893\n" +
			"  12: 7,616,063\n" +
			"  21: 778,295\n" +
			"  30: 75,049\n";

		expectedOutput(
			new String[]{"--algorithm", "TriangleListing", "--order", "undirected", "--sort_triangle_vertices", "--triadic_census",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testUndirectedPrintWithRMatIntegerGraph() throws Exception {
		expectedCount(
			new String[]{"--algorithm", "TriangleListing", "--order", "undirected",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output", "print"},
			75049);
	}
}
