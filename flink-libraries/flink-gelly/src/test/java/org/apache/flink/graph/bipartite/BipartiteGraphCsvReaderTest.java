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

package org.apache.flink.graph.bipartite;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.NullValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class BipartiteGraphCsvReaderTest {
	private static final String TOP_VERTICES_FILE_NAME = "topVertices.csv";
	private static final String BOTTOM_VERTICES_FILE_NAME = "bottomVertices.csv";
	private static final String EDGES_FILE_NAME = "edges.csv";

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private File topVerticesFile;
	private File bottomVerticesFile;
	private File edgesFile;

	private ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();

	@Before
	public void before() throws IOException {
		topVerticesFile = tempFolder.newFile(TOP_VERTICES_FILE_NAME);
		bottomVerticesFile = tempFolder.newFile(BOTTOM_VERTICES_FILE_NAME);
		edgesFile = tempFolder.newFile(EDGES_FILE_NAME);
	}

	@Test
	public void testReadBipartiteGraphFromThreeFiles() throws Exception {
		writeTopVerticesFile(
			"1,top1\n" +
			"2,top2");
		writeBottomVerticesFile(
			"3,bottom3\n" +
			"4,bottom4");
		writeEdgesFile(
			"1,3,1-3\n" +
			"2,4,2-4"
		);

		BipartiteGraphCsvReader reader = BipartiteGraph.fromCsvReader(
			topVerticesFile.getAbsolutePath(),
			bottomVerticesFile.getAbsolutePath(),
			edgesFile.getAbsolutePath(),
			executionEnvironment
		);

		BipartiteGraph<Integer, Integer, String, String, String> graph
			= reader.types(Integer.class, Integer.class, String.class, String.class, String.class);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,top1)\n" +
			"(2,top2)");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(3,bottom3)\n" +
			"(4,bottom4)");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,3,1-3)\n" +
			"(2,4,2-4)");
	}

	@Test
	public void testReadBipartiteGraphFromEdgesFileWithMappers() throws Exception {
		writeEdgesFile(
			"1,3,1-3\n" +
			"2,4,2-4"
		);

		BipartiteGraphCsvReader reader = BipartiteGraph.fromCsvReader(
			edgesFile.getAbsolutePath(),
			new MultiplyIdBy(10),
			new MultiplyIdBy(100),
			executionEnvironment
		);

		BipartiteGraph<Integer, Integer, String, String, String> graph
			= reader.types(Integer.class, Integer.class, String.class, String.class, String.class);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,10)\n" +
			"(2,20)");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(3,300)\n" +
			"(4,400)");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,3,1-3)\n" +
			"(2,4,2-4)");
	}

	@Test
	public void testReadBipartiteGraphFromEdgesFileWithoutMappers() throws Exception {
		writeEdgesFile(
			"1,3,1-3\n" +
			"2,4,2-4"
		);

		BipartiteGraphCsvReader reader = BipartiteGraph.fromCsvReader(
			edgesFile.getAbsolutePath(),
			executionEnvironment
		);

		BipartiteGraph<Integer, Integer, NullValue, NullValue, String> graph
			= reader.edgeTypes(Integer.class, Integer.class, String.class);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,(null))\n" +
			"(2,(null))");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(3,(null))\n" +
			"(4,(null))");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,3,1-3)\n" +
			"(2,4,2-4)");
	}

	@Test
	public void testReadBipartiteGraphFromEdgesFileWithoutValueTypes() throws Exception {
		writeEdgesFile(
			"1,3\n" +
			"2,4"
		);

		BipartiteGraphCsvReader reader = BipartiteGraph.fromCsvReader(
			edgesFile.getAbsolutePath(),
			executionEnvironment
		);

		BipartiteGraph<Integer, Integer, NullValue, NullValue, NullValue> graph
			= reader.keyType(Integer.class, Integer.class);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,(null))\n" +
			"(2,(null))");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(3,(null))\n" +
			"(4,(null))");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,3,(null))\n" +
			"(2,4,(null))");
	}

	@Test
	public void testReadBipartiteGraphFromThreeFilesWithoutEdgeValues() throws Exception {
		writeTopVerticesFile(
			"1,top1\n" +
			"2,top2");
		writeBottomVerticesFile(
			"3,bottom3\n" +
			"4,bottom4");
		writeEdgesFile(
			"1,3\n" +
			"2,4"
		);

		BipartiteGraphCsvReader reader = BipartiteGraph.fromCsvReader(
			topVerticesFile.getAbsolutePath(),
			bottomVerticesFile.getAbsolutePath(),
			edgesFile.getAbsolutePath(),
			executionEnvironment
		);

		BipartiteGraph<Integer, Integer, String, String, NullValue> graph
			= reader.vertexTypes(Integer.class, Integer.class, String.class, String.class);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,top1)\n" +
			"(2,top2)");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(3,bottom3)\n" +
			"(4,bottom4)");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,3,(null))\n" +
			"(2,4,(null))");
	}

	@Test
	public void testReadBipartiteGraphFromEdgesFileWithNoEdgeTypesWithMappers() throws Exception {
		writeEdgesFile(
			"1,3\n" +
			"2,4"
		);

		BipartiteGraphCsvReader reader = BipartiteGraph.fromCsvReader(
			edgesFile.getAbsolutePath(),
			new MultiplyIdBy(10),
			new MultiplyIdBy(100),
			executionEnvironment
		);

		BipartiteGraph<Integer, Integer, String, String, NullValue> graph
			= reader.vertexTypes(Integer.class, Integer.class, String.class, String.class);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,10)\n" +
			"(2,20)");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(3,300)\n" +
			"(4,400)");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,3,(null))\n" +
			"(2,4,(null))");
	}

	private void writeTopVerticesFile(String s) throws IOException {
		write(s, topVerticesFile.getAbsolutePath());
	}

	private void writeBottomVerticesFile(String s) throws IOException {
		write(s, bottomVerticesFile.getAbsolutePath());
	}

	private void writeEdgesFile(String s) throws IOException {
		write(s, edgesFile.getAbsolutePath());
	}

	private void write(String content, String fileName) throws IOException {
		try (PrintWriter out = new PrintWriter(fileName)) {
			out.write(content);
		}
	}

	private static class MultiplyIdBy implements MapFunction<Integer, String> {
		private final int multiplier;

		public MultiplyIdBy(int multiplier) {
			this.multiplier = multiplier;
		}

		@Override
		public String map(Integer value) throws Exception {
			return String.valueOf(value * multiplier);
		}
	}
}
