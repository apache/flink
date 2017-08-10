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
 * Tests for {@link EdgeList}.
 */
@RunWith(Parameterized.class)
public class EdgeListITCase extends NonTransformableDriverBaseITCase {

	public EdgeListITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(String input, String output, String... additionalParameters) {
		String[] parameters = new String[] {
			"--algorithm", "EdgeList",
			"--input", input, "--type", idType,
			"--output", output};

		return ArrayUtils.addAll(parameters, additionalParameters);
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
	public void testHashWithCirculantGraph() throws Exception {
		expectedChecksum(
			parameters("CirculantGraph", "hash", "--vertex_count", "42", "--range0", "13:4"),
			168, 0x000000000001ae80);
	}

	@Test
	public void testPrintWithCirculantGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("CirculantGraph", "print", "--vertex_count", "42", "--range0", "13:4"),
			new Checksum(168, 0x0000004bdcc52cbcL));
	}

	@Test
	public void testHashWithCompleteGraph() throws Exception {
		expectedChecksum(
			parameters("CompleteGraph", "hash", "--vertex_count", "42"),
			1722, 0x0000000000113ca0L);
	}

	@Test
	public void testPrintWithCompleteGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("CompleteGraph", "print", "--vertex_count", "42"),
			new Checksum(1722, 0x0000031109a0c398L));
	}

	@Test
	public void testHashWithCycleGraph() throws Exception {
		expectedChecksum(
			parameters("CycleGraph", "hash", "--vertex_count", "42"),
			84, 0x000000000000d740L);
	}

	@Test
	public void testPrintWithCycleGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("CycleGraph", "print", "--vertex_count", "42"),
			new Checksum(84, 0x000000272a136fcaL));
	}

	@Test
	public void testHashWithEchoGraph() throws Exception {
		expectedChecksum(
			parameters("EchoGraph", "hash", "--vertex_count", "42", "--vertex_degree", "13"),
			546, 0x0000000000057720L);
	}

	@Test
	public void testPrintWithEchoGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("EchoGraph", "print", "--vertex_count", "42", "--vertex_degree", "13"),
			new Checksum(546, 0x000000f7190b8fcaL));
	}

	@Test
	public void testHashWithEmptyGraph() throws Exception {
		expectedChecksum(
			parameters("EmptyGraph", "hash", "--vertex_count", "42"),
			0, 0x0000000000000000L);
	}

	@Test
	public void testHashWithGridGraph() throws Exception {
		expectedChecksum(
			parameters("GridGraph", "hash", "--dim0", "2:true", "--dim1", "3:false", "--dim2", "5:true"),
			130, 0x000000000000eba0L);
	}

	@Test
	public void testPrintWithGridGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("GridGraph", "print", "--dim0", "2:true", "--dim1", "3:false", "--dim2", "5:true"),
			new Checksum(130, 0x00000033237d24eeL));
	}

	@Test
	public void testHashWithHypercubeGraph() throws Exception {
		expectedChecksum(
			parameters("HypercubeGraph", "hash", "--dimensions", "7"),
			896, 0x00000000001bc800L);
	}

	@Test
	public void testPrintWithHypercubeGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("HypercubeGraph", "print", "--dimensions", "7"),
			new Checksum(896, 0x000001f243ee33b2L));
	}

	@Test
	public void testHashWithPathGraph() throws Exception {
		expectedChecksum(
			parameters("PathGraph", "hash", "--vertex_count", "42"),
			82, 0x000000000000d220L);
	}

	@Test
	public void testPrintWithPathGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("PathGraph", "print", "--vertex_count", "42"),
			new Checksum(82, 0x000000269be2d4c2L));
	}

	@Test
	public void testHashWithRMatGraph() throws Exception {
		expectedChecksum(
			parameters("RMatGraph", "hash", "--scale", "7"),
			2048, 0x00000000001ee529);
	}

	@Test
	public void testPrintWithRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("RMatGraph", "print", "--scale", "7"),
			new Checksum(2048, 0x000002f737939f05L));
	}

	@Test
	public void testHashWithDirectedRMatGraph() throws Exception {
		expectedChecksum(
			parameters("RMatGraph", "hash", "--scale", "7", "--simplify", "directed"),
			1168, 0x00000000001579bdL);
	}

	@Test
	public void testPrintWithDirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("RMatGraph", "print", "--scale", "7", "--simplify", "directed"),
			new Checksum(1168, 0x0000020e35b0f35dL));
	}

	@Test
	public void testHashWithUndirectedRMatGraph() throws Exception {
		expectedChecksum(
			parameters("RMatGraph", "hash", "--scale", "7", "--simplify", "undirected"),
			1854, 0x0000000000242920L);
	}

	@Test
	public void testPrintWithUndirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("RMatGraph", "print", "--scale", "7", "--simplify", "undirected"),
			new Checksum(1854, 0x0000036fe5802162L));
	}

	@Test
	public void testHashWithSingletonEdgeGraph() throws Exception {
		expectedChecksum(
			parameters("SingletonEdgeGraph", "hash", "--vertex_pair_count", "42"),
			84, 0x000000000001b3c0L);
	}

	@Test
	public void testPrintWithSingletonEdgeGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("SingletonEdgeGraph", "print", "--vertex_pair_count", "42"),
			new Checksum(84, 0x0000002e59e10d9aL));
	}

	@Test
	public void testHashWithStarGraph() throws Exception {
		expectedChecksum(
			parameters("StarGraph", "hash", "--vertex_count", "42"),
			82, 0x0000000000006ba0L);
	}

	@Test
	public void testPrintWithStarGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(
			parameters("StarGraph", "print", "--vertex_count", "42"),
			new Checksum(82, 0x00000011ec3faee8L));
	}
}
