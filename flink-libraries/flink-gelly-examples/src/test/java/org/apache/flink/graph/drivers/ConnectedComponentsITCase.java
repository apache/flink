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
public class ConnectedComponentsITCase
extends DriverBaseITCase {

	public ConnectedComponentsITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new ConnectedComponents().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "ConnectedComponents"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testHashWithRMatIntegerGraph() throws Exception {
		String expected = "\\nChecksumHashCode 0x0000000000cdc7e7, count 838\\n";

		expectedOutput(
			new String[]{"--algorithm", "ConnectedComponents",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected", "--edge_factor", "1",
					"--a", "0.25", "--b", "0.25", "--c", "0.25", "--noise_enabled", "--noise", "1.0",
				"--output", "hash"},
			expected);
	}

	@Test
	public void testPrintWithRMatIntegerGraph() throws Exception {
		expectedCount(
			new String[]{"--algorithm", "ConnectedComponents",
				"--input", "RMatGraph", "--type", "integer", "--simplify", "undirected", "--edge_factor", "1",
					"--a", "0.25", "--b", "0.25", "--c", "0.25", "--noise_enabled", "--noise", "1.0",
				"--output", "print"},
			838);
	}
}
