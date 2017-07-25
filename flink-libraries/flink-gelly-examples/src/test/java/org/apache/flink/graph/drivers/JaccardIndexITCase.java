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
 * Tests for {@link JaccardIndex}.
 */
@RunWith(Parameterized.class)
public class JaccardIndexITCase
extends CopyableValueDriverBaseITCase {

	public JaccardIndexITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String output, String... additionalParameters) {
		String[] parameters = new String[] {
			"--algorithm", "JaccardIndex", "--mirror_results",
			"--input", "RMatGraph", "--scale", Integer.toString(scale), "--type", idType, "--simplify", "undirected",
			"--output", output};

		return ArrayUtils.addAll(parameters, additionalParameters);
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
	public void testHashWithSmallRMatGraph() throws Exception {
		expectedChecksum(parameters(7, "hash"), 11388, 0x0000164757052eebL);
	}

	@Test
	public void testHashWithLargeRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		// skip 'byte' which cannot store vertex IDs for scale > 8
		Assume.assumeFalse(idType.equals("byte") || idType.equals("nativeByte"));

		expectedChecksum(parameters(12, "hash"), 4432058, 0x0021ce158d811c4eL);
	}

	@Test
	public void testPrintWithSmallRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(parameters(7, "print"), new Checksum(11388, 0x0000163b17088256L));
	}
}
