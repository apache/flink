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
public class EdgeListITCase
extends DriverBaseITCase {

	public EdgeListITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new EdgeList().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "EdgeList"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testHashWithCompleteGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x0000000006788c22, count 1722\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "CompleteGraph", "--vertex_count", "42",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithCycleGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x000000000050cea4, count 84\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "CycleGraph", "--vertex_count", "42",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithEmptyGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x0000000000000000, count 0\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "EmptyGraph", "--vertex_count", "42",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithGridGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x00000000357d33a6, count 2990\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "GridGraph", "--dim0", "5:true", "--dim1", "8:false", "--dim2", "13:true",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithHypercubeGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x0000000014a72800, count 2048\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "HypercubeGraph", "--dimensions", "8",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithPathGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x00000000004ee21a, count 82\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "PathGraph", "--vertex_count", "42",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatIntegerGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x00000000ed469103, count 16384\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "integer",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatIntegerDirectedGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x00000000c53bfc9b, count 12009\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "directed",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatIntegerUndirectedGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x00000001664eb9e4, count 20884\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatLongGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x0000000116ee9103, count 16384\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "long",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testPrintWithRMatLongGraph() throws Exception {

		expectedCount(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "long",
				"--output", "print"},
			16384);
	}

	@Test
	public void testHashWithRMatLongDirectedGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x00000000e3c4643b, count 12009\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "long", "--simplify", "directed",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatLongUndirectedGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x000000019b67ae64, count 20884\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "long", "--simplify", "undirected",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatStringGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x00000071dc80a623, count 16384\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "string",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatStringDirectedGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x0000005d58b3fa7d, count 12009\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "string", "--simplify", "directed",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithRMatStringUndirectedGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x000000aa54987304, count 20884\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph", "--type", "string", "--simplify", "undirected",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithSingletonEdgeGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x0000000001af8ee8, count 200\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "SingletonEdgeGraph", "--vertex_pair_count", "100",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testHashWithStarGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x000000000042789a, count 82\n";

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "StarGraph", "--vertex_count", "42",
				"--output", "hash"},
			expected);
	}
}
