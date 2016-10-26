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
public class ClusteringCoefficientITCase
extends DriverBaseITCase {

	public ClusteringCoefficientITCase(TestExecutionMode mode) {
		super(mode);
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
	public void testDirectedHashWithRMatIntegerGraph() throws Exception {
		String expected = "\n" +
			"ChecksumHashCode 0x000001c0409df6c0, count 902\n" +
			"triplet count: 1003442, triangle count: 225147, global clustering coefficient: 0.22437470[0-9]+\n" +
			"vertex count: 902, average clustering coefficient: 0.32943748[0-9]+\n";

		expectedOutput(
			new String[]{"--algorithm", "ClusteringCoefficient", "--order", "directed",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "directed",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testDirectedPrintWithRMatIntegerGraph() throws Exception {
		expectedCount(
			new String[]{"--algorithm", "ClusteringCoefficient", "--order", "directed",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "directed",
				"--output", "print"},
			904);
	}

	@Test
	public void testUndirectedHashWithRMatIntegerGraph() throws Exception {
		String expected = "\n" +
			"ChecksumHashCode 0x000001ccf8c45fdb, count 902\n" +
			"triplet count: 1003442, triangle count: 225147, global clustering coefficient: 0.22437470[0-9]+\n" +
			"vertex count: 902, average clustering coefficient: 0.42173070[0-9]+\n";

		expectedOutput(
			new String[]{"--algorithm", "ClusteringCoefficient", "--order", "undirected",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testUndirectedPrintWithRMatIntegerGraph() throws Exception {
		expectedCount(
			new String[]{"--algorithm", "ClusteringCoefficient", "--order", "undirected",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output", "print"},
			904);
	}
}
