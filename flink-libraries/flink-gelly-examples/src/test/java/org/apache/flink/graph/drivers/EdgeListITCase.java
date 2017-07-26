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

	// CirculantGraph

	private String[] getCirculantGraphParamters(String output) {
		return parameters("CirculantGraph", output, "--vertex_count", "42", "--range0", "13:4");
	}

	@Test
	public void testHashWithCirculantGraph() throws Exception {
		expectedChecksum(getCirculantGraphParamters("hash"), 168, 0x000000000001ae80);
	}

	@Test
	public void testPrintWithCirculantGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getCirculantGraphParamters("print"), new Checksum(168, 0x0000004bdcc52cbcL));
	}

	@Test
	public void testParallelismWithCirculantGraph() throws Exception {
		TestUtils.verifyParallelism(getCirculantGraphParamters("print"));
	}

	// CompleteGraph

	private String[] getCompleteGraphParamters(String output) {
		return parameters("CompleteGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithCompleteGraph() throws Exception {
		expectedChecksum(getCompleteGraphParamters("hash"), 1722, 0x0000000000113ca0L);
	}

	@Test
	public void testPrintWithCompleteGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getCompleteGraphParamters("print"), new Checksum(1722, 0x0000031109a0c398L));
	}

	@Test
	public void testParallelismWithCompleteGraph() throws Exception {
		TestUtils.verifyParallelism(getCompleteGraphParamters("print"));
	}

	// CycleGraph

	private String[] getCycleGraphParamters(String output) {
		return parameters("CycleGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithCycleGraph() throws Exception {
		expectedChecksum(getCycleGraphParamters("hash"), 84, 0x000000000000d740L);
	}

	@Test
	public void testPrintWithCycleGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getCycleGraphParamters("print"), new Checksum(84, 0x000000272a136fcaL));
	}

	@Test
	public void testParallelismWithCycleGraph() throws Exception {
		TestUtils.verifyParallelism(getCycleGraphParamters("print"));
	}

	// EchoGraph

	private String[] getEchoGraphParamters(String output) {
		return parameters("EchoGraph", output, "--vertex_count", "42", "--vertex_degree", "13");
	}

	@Test
	public void testHashWithEchoGraph() throws Exception {
		expectedChecksum(getEchoGraphParamters("hash"), 546, 0x0000000000057720L);
	}

	@Test
	public void testPrintWithEchoGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getEchoGraphParamters("print"), new Checksum(546, 0x000000f7190b8fcaL));
	}

	@Test
	public void testParallelismWithEchoGraph() throws Exception {
		TestUtils.verifyParallelism(getEchoGraphParamters("print"));
	}

	// EmptyGraph

	private String[] getEmptyGraphParamters(String output) {
		return parameters("EmptyGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithEmptyGraph() throws Exception {
		expectedChecksum(getEmptyGraphParamters("hash"), 0, 0x0000000000000000L);
	}

	@Test
	public void testPrintWithEmptyGraph() throws Exception {
		expectedOutputChecksum(getEmptyGraphParamters("print"), new Checksum(0, 0x0000000000000000L));
	}

	@Test
	public void testParallelismWithEmptyGraph() throws Exception {
		TestUtils.verifyParallelism(getEmptyGraphParamters("print"));
	}

	// GridGraph

	private String[] getGridGraphParamters(String output) {
		return parameters("GridGraph", output, "--dim0", "2:true", "--dim1", "3:false", "--dim2", "5:true");
	}

	@Test
	public void testHashWithGridGraph() throws Exception {
		expectedChecksum(getGridGraphParamters("hash"), 130, 0x000000000000eba0L);
	}

	@Test
	public void testPrintWithGridGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getGridGraphParamters("print"), new Checksum(130, 0x00000033237d24eeL));
	}

	@Test
	public void testParallelismWithGridGraph() throws Exception {
		TestUtils.verifyParallelism(getGridGraphParamters("print"));
	}

	// HypercubeGraph

	private String[] getHypercubeGraphParamters(String output) {
		return parameters("HypercubeGraph", output, "--dimensions", "7");
	}

	@Test
	public void testHashWithHypercubeGraph() throws Exception {
		expectedChecksum(getHypercubeGraphParamters("hash"), 896, 0x00000000001bc800L);
	}

	@Test
	public void testPrintWithHypercubeGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getHypercubeGraphParamters("print"), new Checksum(896, 0x000001f243ee33b2L));
	}

	@Test
	public void testParallelismWithHypercubeGraph() throws Exception {
		TestUtils.verifyParallelism(getHypercubeGraphParamters("print"));
	}

	// PathGraph

	private String[] getPathGraphParamters(String output) {
		return parameters("PathGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithPathGraph() throws Exception {
		expectedChecksum(getPathGraphParamters("hash"), 82, 0x000000000000d220L);
	}

	@Test
	public void testPrintWithPathGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getPathGraphParamters("print"), new Checksum(82, 0x000000269be2d4c2L));
	}

	@Test
	public void testParallelismWithPathGraph() throws Exception {
		TestUtils.verifyParallelism(getPathGraphParamters("print"));
	}

	// RMatGraph

	private String[] getRMatGraphParamters(String output, String simplify) {
		if (simplify == null) {
			return parameters("RMatGraph", output, "--scale", "7");
		} else {
			return parameters("RMatGraph", output, "--scale", "7","--simplify", simplify);
		}
	}

	@Test
	public void testHashWithRMatGraph() throws Exception {
		expectedChecksum(getRMatGraphParamters("hash", null), 2048, 0x00000000001ee529);
	}

	@Test
	public void testPrintWithRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getRMatGraphParamters("print", null), new Checksum(2048, 0x000002f737939f05L));
	}

	@Test
	public void testParallelismWithRMatGraph() throws Exception {
		TestUtils.verifyParallelism(getRMatGraphParamters("print", null));
	}

	@Test
	public void testHashWithDirectedRMatGraph() throws Exception {
		expectedChecksum(getRMatGraphParamters("hash", "directed"), 1168, 0x00000000001579bdL);
	}

	@Test
	public void testPrintWithDirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getRMatGraphParamters("print", "directed"), new Checksum(1168, 0x0000020e35b0f35dL));
	}

	@Test
	public void testParallelismWithDirectedRMatGraph() throws Exception {
		TestUtils.verifyParallelism(getRMatGraphParamters("print", "directed"));
	}

	@Test
	public void testHashWithUndirectedRMatGraph() throws Exception {
		expectedChecksum(getRMatGraphParamters("hash", "undirected"), 1854, 0x0000000000242920L);
	}

	@Test
	public void testPrintWithUndirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getRMatGraphParamters("print", "undirected"), new Checksum(1854, 0x0000036fe5802162L));
	}

	@Test
	public void testParallelismWithUndirectedRMatGraph() throws Exception {
		TestUtils.verifyParallelism(getRMatGraphParamters("print", "undirected"));
	}

	// SingletonEdgeGraph

	private String[] getSingletonEdgeGraphParamters(String output) {
		return parameters("SingletonEdgeGraph", output, "--vertex_pair_count", "42");
	}

	@Test
	public void testHashWithSingletonEdgeGraph() throws Exception {
		expectedChecksum(getSingletonEdgeGraphParamters("hash"), 84, 0x000000000001b3c0L);
	}

	@Test
	public void testPrintWithSingletonEdgeGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getSingletonEdgeGraphParamters("print"), new Checksum(84, 0x0000002e59e10d9aL));
	}

	@Test
	public void testParallelismWithSingletonEdgeGraph() throws Exception {
		TestUtils.verifyParallelism(getSingletonEdgeGraphParamters("print"));
	}

	// StarGraph

	private String[] getStarGraphParamters(String output) {
		return parameters("StarGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithStarGraph() throws Exception {
		expectedChecksum(getStarGraphParamters("hash"), 82, 0x0000000000006ba0L);
	}

	@Test
	public void testPrintWithStarGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getStarGraphParamters("print"), new Checksum(82, 0x00000011ec3faee8L));
	}

	@Test
	public void testParallelismWithStarGraph() throws Exception {
		TestUtils.verifyParallelism(getStarGraphParamters("print"));
	}
}
