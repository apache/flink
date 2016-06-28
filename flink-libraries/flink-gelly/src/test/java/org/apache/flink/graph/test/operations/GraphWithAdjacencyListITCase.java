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

package org.apache.flink.graph.test.operations;

import com.google.common.base.Charsets;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.List;

@RunWith(Parameterized.class)
public class GraphWithAdjacencyListITCase extends MultipleProgramsTestBase {

	public GraphWithAdjacencyListITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testGraphAdjacencyListReaderNoVertexEdgeValues() throws Exception {
    /*
     * Test with an Adjacency List formatted text file without vertex or edge values.
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String fileContent = "0 1,4,5,8\n" +
				"1 0,2\n" +
				"2 3\n" +
				"3\n" +
				"4\n" +
				"5 6\n" +
				"6 3,7\n" +
				"7 4\n" +
				"8 0\n";
		final FileInputSplit split = createTempFile(fileContent);

		Graph myGraph = Graph.fromAdjacencyListFile(split.getPath().toString(), env).keyType(Long.class);

		String expectedResult = "0,1,(null),(null),(null)\n" +
				"0,4,(null),(null),(null)\n" +
				"0,5,(null),(null),(null)\n" +
				"0,8,(null),(null),(null)\n" +
				"1,0,(null),(null),(null)\n" +
				"1,2,(null),(null),(null)\n" +
				"2,3,(null),(null),(null)\n" +
				"5,6,(null),(null),(null)\n" +
				"6,3,(null),(null),(null)\n" +
				"6,7,(null),(null),(null)\n" +
				"7,4,(null),(null),(null)\n" +
				"8,0,(null),(null),(null)\n" ;

		List<Triplet<Long, Double, Double>> result = myGraph.getTriplets().collect();

		Assert.assertEquals(9, myGraph.numberOfVertices());

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testGraphAdjacencyListReaderNullEdge() throws Exception {
    /*
     * Test with an Adjacency List formatted text file without edge values.
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String fileContent = "0-Node0 1,4,5,8\n" +
				"1-Node1 0,2\n" +
				"2-Node2 3\n" +
				"3-Node3\n" +
				"4-Node4\n" +
				"5-Node5 6\n" +
				"6-Node6 3,7\n" +
				"7-Node7 4\n" +
				"8-Node8 0\n";
		final FileInputSplit split = createTempFile(fileContent);

		Graph myGraph = Graph.fromAdjacencyListFile(split.getPath().toString(), env).
				vertexTypes(Long.class, String.class);

		String expectedResult = "0,1,Node0,Node1,(null)\n" +
				"0,4,Node0,Node4,(null)\n" +
				"0,5,Node0,Node5,(null)\n" +
				"0,8,Node0,Node8,(null)\n" +
				"1,0,Node1,Node0,(null)\n" +
				"1,2,Node1,Node2,(null)\n" +
				"2,3,Node2,Node3,(null)\n" +
				"5,6,Node5,Node6,(null)\n" +
				"6,3,Node6,Node3,(null)\n" +
				"6,7,Node6,Node7,(null)\n" +
				"7,4,Node7,Node4,(null)\n" +
				"8,0,Node8,Node0,(null)\n" ;

		List<Triplet<Long, Double, Double>> result = myGraph.getTriplets().collect();

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testGraphAdjacencyListReaderNullVertex() throws Exception {
    /*
     * Test with an Adjacency List formatted text file without vertex values.
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String fileContent = "0 1-0.1,4-0.2,5-0.3,8-0.1\n" +
				"1 0-0.8,2-0.3\n" +
				"2 3-0.3\n" +
				"3\n" +
				"4\n" +
				"5 6-0.7\n" +
				"6 3-0.2,7-0.5\n" +
				"7 4-0.1\n" +
				"8 0-0.2\n";
		final FileInputSplit split = createTempFile(fileContent);

		Graph myGraph = Graph.fromAdjacencyListFile(split.getPath().toString(), env).
				edgeTypes(Long.class, Double.class);

		String expectedResult = "0,1,(null),(null),0.1\n" +
				"0,4,(null),(null),0.2\n" +
				"0,5,(null),(null),0.3\n" +
				"0,8,(null),(null),0.1\n" +
				"1,0,(null),(null),0.8\n" +
				"1,2,(null),(null),0.3\n" +
				"2,3,(null),(null),0.3\n" +
				"5,6,(null),(null),0.7\n" +
				"6,3,(null),(null),0.2\n" +
				"6,7,(null),(null),0.5\n" +
				"7,4,(null),(null),0.1\n" +
				"8,0,(null),(null),0.2\n" ;

		List<Triplet<Long, Double, Double>> result = myGraph.getTriplets().collect();

		Assert.assertEquals(9, myGraph.numberOfVertices());
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testGraphAdjacencyListReader() throws Exception {
    /*
     * Test with an Adjacency List formatted text file with vertex and edge values.
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String fileContent = "0-Node0 1-0.1,4-0.2,5-0.3,8-0.1\n" +
				"1-Node1 0-0.8,2-0.3\n" +
				"2-Node2 3-0.3\n" +
				"3-Node3 \n" +
				"4-Node4\n" +
				"5-Node5 6-0.7\n" +
				"6-Node6 3-0.2,7-0.5\n" +
				"7-Node7 4-0.1\n" +
				"8-Node8 0-0.2\n";
		final FileInputSplit split = createTempFile(fileContent);

		Graph myGraph = Graph.fromAdjacencyListFile(split.getPath().toString(), env).
				types(Long.class, String.class, Double.class);

		String expectedResult = "0,1,Node0,Node1,0.1\n" +
				"0,4,Node0,Node4,0.2\n" +
				"0,5,Node0,Node5,0.3\n" +
				"0,8,Node0,Node8,0.1\n" +
				"1,0,Node1,Node0,0.8\n" +
				"1,2,Node1,Node2,0.3\n" +
				"2,3,Node2,Node3,0.3\n" +
				"5,6,Node5,Node6,0.7\n" +
				"6,3,Node6,Node3,0.2\n" +
				"6,7,Node6,Node7,0.5\n" +
				"7,4,Node7,Node4,0.1\n" +
				"8,0,Node8,Node0,0.2\n" ;

		List<Triplet<Long, Double, Double>> result = myGraph.getTriplets().collect();

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testCreateCsvFileDelimiterConfiguration() throws Exception {
    /*
     * Test with an Adjacency List formatted text file. Tests the configuration methods vertexValueDelimiter and
     * verticesDelimiter
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final String fileContent = "0;0.0 1;0.1|4;0.2|5;0.3\n" +
				"1;1.0 0;0.8|2;0.3\n" +
				"2;2.0 3;0.3\n" +
				"3;3.0 \n" +
				"4;4.0\n" +
				"5;5.0\n";

		final FileInputSplit split = createTempFile(fileContent);

		Graph myGraph = Graph.fromAdjacencyListFile(split.getPath().toString(), env).
				vertexValueDelimiter(";").
				verticesDelimiter("|").
				types(Long.class, Double.class, Double.class);

		List<Triplet<Long, Double, Double>> result = myGraph.getTriplets().collect();

		String expectedResult = "0,1,0.0,1.0,0.1\n" +
				"0,4,0.0,4.0,0.2\n" +
				"0,5,0.0,5.0,0.3\n" +
				"1,0,1.0,0.0,0.8\n" +
				"1,2,1.0,2.0,0.3\n" +
				"2,3,2.0,3.0,0.3\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testWriteAsAdjacencyList() throws Exception {
    /*
     * Tests the writeAsAdjacencyList method, which writes a graph in an Adjacency List formatted text file.
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final String fileContent = "0;0.0 1;0.1|4;0.2|5;0.3\n" +
				"1;1.0 0;0.8|2;0.3\n" +
				"2;2.0 3;0.3\n" +
				"3;3.0 \n" +
				"4;4.0\n" +
				"5;5.0\n";
		final FileInputSplit split = createTempFile(fileContent);

		Graph myGraph = Graph.fromAdjacencyListFile(split.getPath().toString(), env).
				vertexValueDelimiter(";").
				verticesDelimiter("|").
				types(Long.class, Double.class, Double.class);

		java.nio.file.Path tempDir = Files.createTempDirectory("testDir");

		myGraph.writeAsAdjacencyList(tempDir + "out1.txt", "\t", "|", ";");

		Graph.fromAdjacencyListFile(tempDir + "out1.txt", env).
				vertexValueDelimiter(";").
				verticesDelimiter("|").
				types(Long.class, Double.class, Double.class);

		String expectedResult = "0,1,0.0,1.0,0.1\n" +
				"0,4,0.0,4.0,0.2\n" +
				"0,5,0.0,5.0,0.3\n" +
				"1,0,1.0,0.0,0.8\n" +
				"1,2,1.0,2.0,0.3\n" +
				"2,3,2.0,3.0,0.3\n";

		List<Triplet<Long, Double, Double>> result = myGraph.getTriplets().collect();

		compareResultAsTuples(result, expectedResult);
	}

	/*------------------------------------------------------------------------------------------*/
	private FileInputSplit createTempFile(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();

		OutputStreamWriter wrt = new OutputStreamWriter(
				new FileOutputStream(tempFile), Charsets.UTF_8
		);
		wrt.write(content);
		wrt.close();

		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0,
				tempFile.length(), new String[]{"localhost"});
	}
}

