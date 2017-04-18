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
public class JaccardIndexITCase
extends DriverBaseITCase {

	public JaccardIndexITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new JaccardIndex().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "JaccardIndex"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testHashWithRMatIntegerGraph() throws Exception {
		String expected = "\nChecksumHashCode 0x0001b188570b2572, count 221628\n";

		expectedOutput(
			new String[]{"--algorithm", "JaccardIndex",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testPrintWithRMatIntegerGraph() throws Exception {
		expectedCount(
			new String[]{"--algorithm", "JaccardIndex",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected",
				"--output", "print"},
			221628);
	}
}
