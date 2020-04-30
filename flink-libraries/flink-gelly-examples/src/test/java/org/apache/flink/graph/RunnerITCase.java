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

package org.apache.flink.graph;

import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.drivers.DriverBaseITCase;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link Runner}.
 */
@RunWith(Parameterized.class)
public class RunnerITCase
extends DriverBaseITCase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	public RunnerITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	@Test
	public void testWithoutAlgorithm() throws Exception {
		String expected = "Select an algorithm to view usage:";

		thrown.expect(ProgramParametrizationException.class);
		thrown.expectMessage(expected);

		expectedOutput(new String[]{}, expected);
	}

	@Test
	public void testWithUnknownAlgorithm() throws Exception {
		String expected = "Unknown algorithm name: NotAnAlgorithm";

		thrown.expect(ProgramParametrizationException.class);
		thrown.expectMessage(expected);

		expectedOutput(new String[]{"--algorithm", "NotAnAlgorithm"}, expected);
	}

	@Test
	public void testAlgorithmUsage() throws Exception {
		String expected = "Pass-through of the graph's edge list.";

		thrown.expect(ProgramParametrizationException.class);
		thrown.expectMessage(expected);

		expectedOutput(new String[]{"--algorithm", "EdgeList"}, expected);
	}

	@Test
	public void testWithoutInput() throws Exception {
		String expected = "No input given";

		thrown.expect(ProgramParametrizationException.class);
		thrown.expectMessage(expected);

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--output", "NotAnOutput"},
			expected);
	}

	@Test
	public void testWithUnknownInput() throws Exception {
		String expected = "Unknown input type: NotAnInput";

		thrown.expect(ProgramParametrizationException.class);
		thrown.expectMessage(expected);

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "NotAnInput"},
			expected);
	}

	@Test
	public void testWithoutOutput() throws Exception {
		String expected = "No output given";

		thrown.expect(ProgramParametrizationException.class);
		thrown.expectMessage(expected);

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph"},
			expected);
	}

	@Test
	public void testWithUnknownOutput() throws Exception {
		String expected = "Unknown output type: NotAnOutput";

		thrown.expect(ProgramParametrizationException.class);
		thrown.expectMessage(expected);

		expectedOutput(
			new String[]{"--algorithm", "EdgeList",
				"--input", "RMatGraph",
				"--output", "NotAnOutput"},
			expected);
	}
}
