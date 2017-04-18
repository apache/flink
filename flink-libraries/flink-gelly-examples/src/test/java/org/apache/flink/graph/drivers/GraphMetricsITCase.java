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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphMetricsITCase
extends DriverBaseITCase {

	public GraphMetricsITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new GraphMetrics().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "GraphMetrics"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testWithDirectedRMatIntegerGraph() throws Exception {
		String expected = "\n" +
			"Vertex metrics:\n" +
			"  vertex count: 902\n" +
			"  edge count: 12,009\n" +
			"  unidirectional edge count: 8,875\n" +
			"  bidirectional edge count: 1,567\n" +
			"  average degree: 13.314\n" +
			"  density: 0.01477663\n" +
			"  triplet count: 1,003,442\n" +
			"  maximum degree: 463\n" +
			"  maximum out degree: 334\n" +
			"  maximum in degree: 342\n" +
			"  maximum triplets: 106,953\n" +
			"\n" +
			"Edge metrics:\n" +
			"  triangle triplet count: 107,817\n" +
			"  rectangle triplet count: 315,537\n" +
			"  maximum triangle triplets: 820\n" +
			"  maximum rectangle triplets: 3,822\n";

		String[] arguments = new String[]{"--algorithm", "GraphMetrics", "--order", "directed",
			"--input", "RMatGraph", "--type", "integer", "--simplify", "directed",
			"--output"};

		expectedOutput(ArrayUtils.addAll(arguments, "hash"), expected);
		expectedOutput(ArrayUtils.addAll(arguments, "print"), expected);
	}

	@Test
	public void testWithUndirectedRMatIntegerGraph() throws Exception {
		String expected = "\n" +
			"Vertex metrics:\n" +
			"  vertex count: 902\n" +
			"  edge count: 10,442\n" +
			"  average degree: 23.153\n" +
			"  density: 0.025697\n" +
			"  triplet count: 1,003,442\n" +
			"  maximum degree: 463\n" +
			"  maximum triplets: 106,953\n" +
			"\n" +
			"Edge metrics:\n" +
			"  triangle triplet count: 107,817\n" +
			"  rectangle triplet count: 315,537\n" +
			"  maximum triangle triplets: 820\n" +
			"  maximum rectangle triplets: 3,822\n";

		String[] arguments = new String[]{"--algorithm", "GraphMetrics", "--order", "undirected",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output"};

		expectedOutput(ArrayUtils.addAll(arguments, "hash"), expected);
		expectedOutput(ArrayUtils.addAll(arguments, "print"), expected);
	}
}
