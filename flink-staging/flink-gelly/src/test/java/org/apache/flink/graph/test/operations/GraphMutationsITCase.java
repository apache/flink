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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphMutationsITCase extends MultipleProgramsTestBase {

	public GraphMutationsITCase(TestExecutionMode mode){
		super(mode);
	}

    private String resultPath;
    private String expectedResult;

    @Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testAddVertex() throws Exception {
		/*
		 * Test addVertex() -- simple case
		 */	

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		graph = graph.addVertex(new Vertex<Long, Long>(6L, 6L));
		graph.getVertices().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,1\n" +
				"2,2\n" +
				"3,3\n" +
				"4,4\n" +
				"5,5\n" +
				"6,6\n";
	}

	@Test
	public void testAddVertices() throws Exception {
		/*
		 * Test addVertices() -- simple case
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(6L, 6L));
		vertices.add(new Vertex<Long, Long>(7L, 7L));

		graph = graph.addVertices(vertices);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,1\n" +
				"2,2\n" +
				"3,3\n" +
				"4,4\n" +
				"5,5\n" +
				"6,6\n" +
				"7,7\n";
	}

	@Test
	public void testAddVertexExisting() throws Exception {
		/*
		 * Test addVertex() -- add an existing vertex
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		graph = graph.addVertex(new Vertex<Long, Long>(1L, 1L));
		graph.getVertices().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,1\n" +
				"2,2\n" +
				"3,3\n" +
				"4,4\n" +
				"5,5\n";
	}

	@Test
	public void testAddVerticesBothExisting() throws Exception {
		/*
		 * Test addVertices() -- add two existing vertices
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(1L, 1L));
		vertices.add(new Vertex<Long, Long>(3L, 3L));

		graph = graph.addVertices(vertices);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,1\n" +
				"2,2\n" +
				"3,3\n" +
				"4,4\n" +
				"5,5\n";
	}

	@Test
	public void testAddVerticesOneExisting() throws Exception {
		/*
		 * Test addVertices() -- add an existing vertex
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(1L, 1L));
		vertices.add(new Vertex<Long, Long>(6L, 6L));

		graph = graph.addVertices(vertices);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,1\n" +
				"2,2\n" +
				"3,3\n" +
				"4,4\n" +
				"5,5\n" +
				"6,6\n";
	}

	@Test
	public void testRemoveVertex() throws Exception {
		/*
		 * Test removeVertex() -- simple case
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		graph = graph.removeVertex(new Vertex<Long, Long>(5L, 5L));
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n";
	}

	@Test
	public void testRemoveVertices() throws Exception {
		/*
		 * Test removeVertices() -- simple case
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		List<Vertex<Long, Long>> verticesToBeRemoved = new ArrayList<Vertex<Long, Long>>();
		verticesToBeRemoved.add(new Vertex<Long, Long>(1L, 1L));
		verticesToBeRemoved.add(new Vertex<Long, Long>(2L, 2L));

		graph = graph.removeVertices(verticesToBeRemoved);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n";
	}

	@Test
	public void testRemoveInvalidVertex() throws Exception {
		/*
		 * Test removeVertex() -- remove an invalid vertex
		 */	
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		graph = graph.removeVertex(new Vertex<Long, Long>(6L, 6L));
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n";
	}

	@Test
	public void testRemoveOneValidOneInvalidVertex() throws Exception {
		/*
		 * Test removeVertices() -- remove one invalid vertex and a valid one
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		List<Vertex<Long, Long>> verticesToBeRemoved = new ArrayList<Vertex<Long, Long>>();
		verticesToBeRemoved.add(new Vertex<Long, Long>(1L, 1L));
		verticesToBeRemoved.add(new Vertex<Long, Long>(7L, 7L));

		graph = graph.removeVertices(verticesToBeRemoved);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n";
	}

	@Test
	public void testRemoveBothInvalidVertices() throws Exception {
		/*
		 * Test removeVertices() -- remove two invalid vertices
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		List<Vertex<Long, Long>> verticesToBeRemoved = new ArrayList<Vertex<Long, Long>>();
		verticesToBeRemoved.add(new Vertex<Long, Long>(6L, 6L));
		verticesToBeRemoved.add(new Vertex<Long, Long>(7L, 7L));

		graph = graph.removeVertices(verticesToBeRemoved);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n";
	}

	@Test
	public void testRemoveBothInvalidVerticesVertexResult() throws Exception {
		/*
		 * Test removeVertices() -- remove two invalid vertices and verify the data set of vertices
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		List<Vertex<Long, Long>> verticesToBeRemoved = new ArrayList<Vertex<Long, Long>>();
		verticesToBeRemoved.add(new Vertex<Long, Long>(6L, 6L));
		verticesToBeRemoved.add(new Vertex<Long, Long>(7L, 7L));

		graph = graph.removeVertices(verticesToBeRemoved);
		graph.getVertices().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,1\n" +
				"2,2\n" +
				"3,3\n" +
				"4,4\n" +
				"5,5\n";
	}
	
	@Test
	public void testAddEdge() throws Exception {
		/*
		 * Test addEdge() -- simple case
		 */
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		graph = graph.addEdge(new Vertex<Long, Long>(6L, 6L), new Vertex<Long, Long>(1L, 1L),
				61L);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n" +
				"6,1,61\n";	
	}

	@Test
	public void testAddEdges() throws Exception {
		/*
		 * Test addEdges() -- simple case
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		List<Edge<Long, Long>> edgesToBeAdded = new ArrayList<Edge<Long, Long>>();
		edgesToBeAdded.add(new Edge<Long, Long>(2L, 4L, 24L));
		edgesToBeAdded.add(new Edge<Long, Long>(4L, 1L, 41L));

		graph = graph.addEdges(edgesToBeAdded);

		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"2,4,24\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,1,41\n" +
				"4,5,45\n" +
				"5,1,51\n";
	}

	@Test
	public void testAddEdgesInvalidVertices() throws Exception {
		/*
		 * Test addEdges() -- the source and target vertices do not exist in the graph
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		List<Edge<Long, Long>> edgesToBeAdded = new ArrayList<Edge<Long, Long>>();
		edgesToBeAdded.add(new Edge<Long, Long>(6L, 1L, 61L));
		edgesToBeAdded.add(new Edge<Long, Long>(7L, 1L, 71L));

		graph = graph.addEdges(edgesToBeAdded);

		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n";
	}

	@Test
	public void testAddExistingEdge() throws Exception {
		/*
		 * Test addEdge() -- add already existing edge
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		graph = graph.addEdge(new Vertex<Long, Long>(1L, 1L), new Vertex<Long, Long>(2L, 2L),
				12L);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n";	
	}

	@Test
	public void testRemoveEdge() throws Exception {
		/*
		 * Test removeEdge() -- simple case
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		graph = graph.removeEdge(new Edge<Long, Long>(5L, 1L, 51L));
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n";
	}

	@Test
	public void testRemoveEdges() throws Exception {
		/*
		 * Test removeEdges() -- simple case
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		List<Edge<Long, Long>> edgesToBeRemoved = new ArrayList<Edge<Long, Long>>();
		edgesToBeRemoved.add(new Edge<Long, Long>(5L, 1L, 51L));
		edgesToBeRemoved.add(new Edge<Long, Long>(2L, 3L, 23L));

		graph = graph.removeEdges(edgesToBeRemoved);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n";
	}

	@Test
	public void testRemoveSameEdgeTwice() throws Exception {
		/*
		 * Test removeEdges() -- try to remove the same edge twice
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		List<Edge<Long, Long>> edgesToBeRemoved = new ArrayList<Edge<Long, Long>>();
		edgesToBeRemoved.add(new Edge<Long, Long>(5L, 1L, 51L));
		edgesToBeRemoved.add(new Edge<Long, Long>(5L, 1L, 51L));

		graph = graph.removeEdges(edgesToBeRemoved);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n";
	}

	@Test
	public void testRemoveInvalidEdge() throws Exception {
		/*
		 * Test removeEdge() -- invalid edge
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		graph = graph.removeEdge(new Edge<Long, Long>(6L, 1L, 61L));
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n";
	}

	@Test
	public void testRemoveOneValidOneInvalidEdge() throws Exception {
		/*
		 * Test removeEdges() -- one edge is valid, the other is invalid
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		List<Edge<Long, Long>> edgesToBeRemoved = new ArrayList<Edge<Long, Long>>();
		edgesToBeRemoved.add(new Edge<Long, Long>(1L, 1L, 51L));
		edgesToBeRemoved.add(new Edge<Long, Long>(6L, 1L, 61L));

		graph = graph.removeEdges(edgesToBeRemoved);
		graph.getEdges().writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n";
	}
}