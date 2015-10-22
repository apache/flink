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

package org.apache.flink.graph.test.example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.IncrementalSSSP;
import org.apache.flink.graph.example.utils.IncrementalSSSPData;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

@RunWith(Parameterized.class)
public class IncrementalSSSPITCase extends MultipleProgramsTestBase {

	private String verticesPath;

	private String edgesPath;

	private String edgesInSSSPPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public IncrementalSSSPITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
		File verticesFile = tempFolder.newFile();
		Files.write(IncrementalSSSPData.VERTICES, verticesFile, Charsets.UTF_8);

		File edgesFile = tempFolder.newFile();
		Files.write(IncrementalSSSPData.EDGES, edgesFile, Charsets.UTF_8);

		File edgesInSSSPFile = tempFolder.newFile();
		Files.write(IncrementalSSSPData.EDGES_IN_SSSP, edgesInSSSPFile, Charsets.UTF_8);

		verticesPath = verticesFile.toURI().toString();
		edgesPath = edgesFile.toURI().toString();
		edgesInSSSPPath = edgesInSSSPFile.toURI().toString();
	}

	@Test
	public void testIncrementalSSSP() throws Exception {
		IncrementalSSSP.main(new String[]{verticesPath, edgesPath, edgesInSSSPPath,
				IncrementalSSSPData.SRC_EDGE_TO_BE_REMOVED, IncrementalSSSPData.TRG_EDGE_TO_BE_REMOVED,
				IncrementalSSSPData.VAL_EDGE_TO_BE_REMOVED,resultPath, IncrementalSSSPData.NUM_VERTICES + ""});
		expected = IncrementalSSSPData.RESULTED_VERTICES;
	}

	@Test
	public void testIncrementalSSSPNonSPEdge() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Vertex<Long, Double>> vertices = IncrementalSSSPData.getDefaultVertexDataSet(env);
		DataSet<Edge<Long, Double>> edges = IncrementalSSSPData.getDefaultEdgeDataSet(env);
		DataSet<Edge<Long, Double>> edgesInSSSP = IncrementalSSSPData.getDefaultEdgesInSSSP(env);
		// the edge to be removed is a non-SP edge
		Edge<Long, Double> edgeToBeRemoved = new Edge<Long, Double>(3L, 5L, 5.0);

		Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);
		// Assumption: all minimum weight paths are kept
		Graph<Long, Double, Double> ssspGraph = Graph.fromDataSet(vertices, edgesInSSSP, env);
		// remove the edge
		graph.removeEdge(edgeToBeRemoved);

		// configure the iteration
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();

		if(IncrementalSSSP.isInSSSP(edgeToBeRemoved, edgesInSSSP)) {

			parameters.setDirection(EdgeDirection.IN);
			parameters.setOptDegrees(true);

			// run the vertex centric iteration to propagate info
			Graph<Long, Double, Double> result = ssspGraph.runVertexCentricIteration(
					new IncrementalSSSP.VertexDistanceUpdater(),
					new IncrementalSSSP.InvalidateMessenger(edgeToBeRemoved),
					IncrementalSSSPData.NUM_VERTICES, parameters);

			DataSet<Vertex<Long, Double>> resultedVertices = result.getVertices();

			resultedVertices.writeAsCsv(resultPath, "\n", ",");
			env.execute();
		} else {
			vertices.writeAsCsv(resultPath, "\n", ",");
			env.execute();
		}

		expected = IncrementalSSSPData.VERTICES;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}
