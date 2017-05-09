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
public class ConnectedComponentsITCase
extends DriverBaseITCase {

	public ConnectedComponentsITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String output) {
		return new String[] {
			"--algorithm", "ConnectedComponents",
			"--input", "RMatGraph", "--scale", Integer.toString(scale), "--type", idType, "--simplify", "undirected",
				"--edge_factor", "1", "--a", "0.25", "--b", "0.25", "--c", "0.25", "--noise_enabled", "--noise", "1.0",
			"--output", output};
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
	public void testHashWithSmallRMatGraph() throws Exception {
		long checksum;
		switch (idType) {
			case "byte":
			case "nativeByte":
			case "short":
			case "nativeShort":
			case "char":
			case "nativeChar":
			case "integer":
			case "nativeInteger":
			case "nativeLong":
				checksum = 0x0000000000033e88L;
				break;

			case "long":
				checksum = 0x0000000000057848L;
				break;

			case "string":
			case "nativeString":
				checksum = 0x000000000254a4c3L;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		expectedChecksum(parameters(7, "hash"), 106, checksum);
	}

	@Test
	public void testHashWithLargeRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		long checksum;
		switch (idType) {
			case "byte":
			case "nativeByte":
				return;

			case "short":
			case "nativeShort":
			case "char":
			case "nativeChar":
			case "integer":
			case "nativeInteger":
			case "nativeLong":
				checksum = 0x00000003094ffba2L;
				break;

			case "long":
				checksum = 0x000000030b68e522L;
				break;

			case "string":
			case "nativeString":
				checksum = 0x00001839ad14edb1L;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		expectedChecksum(parameters(15, "hash"), 25572, checksum);
	}

	@Test
	public void testPrintWithSmallRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		long checksum;
		switch (idType) {
			case "byte":
			case "nativeByte":
			case "short":
			case "nativeShort":
			case "integer":
			case "nativeInteger":
			case "long":
			case "nativeLong":
				checksum = 0x00000024edd0568dL;
				break;

			case "string":
			case "nativeString":
				checksum = 0x000000232d8bf58dL;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		expectedOutputChecksum(parameters(7, "print"), new Checksum(106, checksum));
	}
}
