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

package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.IncrementalSSSPData;
import org.apache.flink.graph.spargel.IterationConfiguration;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

/**
 * Incremental Single Sink Shortest Paths Example.
 *
 * The program takes as input the resulted graph after a SSSP computation,
 * an edge to be removed and the initial graph(i.e. before SSSP was computed).
 *
 * - If the removed edge does not belong to the SP-graph, no computation is necessary.
 * The edge is simply removed from the graph.
 * - If the removed edge is an SP-edge, then all nodes, whose shortest path contains the removed edge,
 * potentially require re-computation.
 * When the edge <u, v> is removed, v checks if it has another out-going SP-edge.
 * If yes, no further computation is required.
 * If v has no other out-going SP-edge, it invalidates its current value, by setting it to INF.
 * Then, it informs all its SP-in-neighbors by sending them an INVALIDATE message.
 * When a vertex u receives an INVALIDATE message from v, it checks whether it has another out-going SP-edge.
 * If not, it invalidates its current value and propagates the INVALIDATE message.
 * The propagation stops when a vertex with an alternative shortest path is reached
 * or when we reach a vertex with no SP-in-neighbors.
 *
 * Usage <code>IncrementalSSSPExample &lt;vertex path&gt; &lt;edge path&gt; &lt;edges in SSSP&gt;
 * &lt;edge to be removed&gt; &lt;result path&gt; &lt;number of iterations&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.IncrementalSSSPData}
 */
@SuppressWarnings("serial")
public class IncrementalSSSPExample implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Double>> vertices = getVerticesDataSet(env);

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		DataSet<Edge<Long, Double>> edgesInSSSP = getEdgesinSSSPDataSet(env);

		Edge<Long, Double> edgeToBeRemoved = getEdgeToBeRemoved();

		Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

		// Assumption: all minimum weight paths are kept
		Graph<Long, Double, Double> ssspGraph = Graph.fromDataSet(vertices, edgesInSSSP, env);

		// remove the edge
		graph.removeEdge(edgeToBeRemoved);

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		if(isInSSSP(edgeToBeRemoved, edgesInSSSP)) {

			parameters.setDirection(EdgeDirection.IN);
			parameters.setOptDegrees(true);

			// run the vertex centric iteration to propagate info
			Graph<Long, Double, Double> result = ssspGraph.runVertexCentricIteration(new VertexDistanceUpdater(),
					new InvalidateMessenger(edgeToBeRemoved), maxIterations, parameters);

			DataSet<Vertex<Long, Double>> resultedVertices = result.getVertices();

			// Emit results
			if(fileOutput) {
				resultedVertices.writeAsCsv(outputPath, "\n", ",");
			} else {
				resultedVertices.print();
			}

			env.execute("Incremental SSSP Example");
		} else {
			// print the vertices
			if(fileOutput) {
				vertices.writeAsCsv(outputPath, "\n", ",");
			} else {
				vertices.print();
			}

			env.execute("Incremental SSSP Example");
		}
	}

	@Override
	public String getDescription() {
		return "Incremental Single Sink Shortest Paths Example";
	}

	// ******************************************************************************************************************
	// IncrementalSSSP METHODS
	// ******************************************************************************************************************

	/**
	 * Function that verifies whether the edge to be removed is part of the SSSP or not.
	 * If it is, the src vertex will be invalidated.
	 *
	 * @param edgeToBeRemoved
	 * @param edgesInSSSP
	 * @return
	 */
	private static boolean isInSSSP(final Edge<Long, Double> edgeToBeRemoved, DataSet<Edge<Long, Double>> edgesInSSSP) throws Exception {

		return edgesInSSSP.filter(new FilterFunction<Edge<Long, Double>>() {
			@Override
			public boolean filter(Edge<Long, Double> edge) throws Exception {
				return edge.equals(edgeToBeRemoved);
			}
		}).count() > 0;
	}

	public static final class VertexDistanceUpdater extends VertexUpdateFunction<Long, Double, Double> {

		@Override
		public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages) throws Exception {
			if (inMessages.hasNext()) {
				Long outDegree = vertex.getOutDegree() - 1;
				// check if the vertex has another SP-Edge
				if (outDegree > 0) {
					// there is another shortest path from the source to this vertex
				} else {
					// set own value to infinity
					setNewVertexValue(Double.MAX_VALUE);
				}
			}
		}
	}

	public static final class InvalidateMessenger extends MessagingFunction<Long, Double, Double, Double> {

		private Edge<Long, Double> edgeToBeRemoved;

		public InvalidateMessenger(Edge<Long, Double> edgeToBeRemoved) {
			this.edgeToBeRemoved = edgeToBeRemoved;
		}

		@Override
		public void sendMessages(Vertex<Long, Double> vertex) throws Exception {


			if(getSuperstepNumber() == 1) {
				if(vertex.getId().equals(edgeToBeRemoved.getSource())) {
					// activate the edge target
					sendMessageTo(edgeToBeRemoved.getSource(), Double.MAX_VALUE);
				}
			}

			if(getSuperstepNumber() > 1) {
				// invalidate all edges
				for(Edge<Long, Double> edge : getEdges()) {
					sendMessageTo(edge.getSource(), Double.MAX_VALUE);
				}
			}
		}
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String verticesInputPath = null;

	private static String edgesInputPath = null;

	private static String edgesInSSSPInputPath = null;

	private static String edgeToBeRemoved = null;

	private static String outputPath = null;

	private static int maxIterations = 5;

	private static boolean parseParameters(String[] args) {
		if (args.length > 0) {
			if (args.length == 6) {
				fileOutput = true;
				verticesInputPath = args[0];
				edgesInputPath = args[1];
				edgesInSSSPInputPath = args[2];
				edgeToBeRemoved = args[3];
				outputPath = args[4];
				maxIterations = Integer.parseInt(args[5]);
			} else {
				System.out.println("Executing IncrementalSSSP example with default parameters and built-in default data.");
				System.out.println("Provide parameters to read input data from files.");
				System.out.println("See the documentation for the correct format of input files.");
				System.out.println("Usage: IncrementalSSSP <vertex path> <edge path> <edges in SSSP> <edge to be removed> <output path> <max iterations>");

				return false;
			}
		}
		return true;
	}

	private static DataSet<Vertex<Long, Double>> getVerticesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(verticesInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Double.class)
					.map(new MapFunction<Tuple2<Long, Double>, Vertex<Long, Double>>() {

						@Override
						public Vertex<Long, Double> map(Tuple2<Long, Double> tuple2) throws Exception {
							return new Vertex<Long, Double>(tuple2.f0, tuple2.f1);
						}
					});
		} else {
			System.err.println("Usage: IncrementalSSSP <vertex path> <edge path> <edges in SSSP> <edge to be removed> " +
					"<output path> <max iterations>");
			return IncrementalSSSPData.getDefaultVertexDataSet(env);
		}
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>() {

						@Override
						public Edge<Long, Double> map(Tuple3<Long, Long, Double> tuple3) throws Exception {
							return new Edge(tuple3.f0, tuple3.f1, tuple3.f2);
						}
					});
		} else {
			System.err.println("Usage: IncrementalSSSP <vertex path> <edge path> <edges in SSSP> <edge to be removed> " +
					"<output path> <max iterations>");
			return IncrementalSSSPData.getDefaultEdgeDataSet(env);
		}
	}

	private static DataSet<Edge<Long, Double>> getEdgesinSSSPDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInSSSPInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>() {

						@Override
						public Edge<Long, Double> map(Tuple3<Long, Long, Double> tuple3) throws Exception {
							return new Edge(tuple3.f0, tuple3.f1, tuple3.f2);
						}
					});
		} else {
			System.err.println("Usage: IncrementalSSSP <vertex path> <edge path> <edges in SSSP> <edge to be removed> " +
					"<output path> <max iterations>");
			return IncrementalSSSPData.getDefaultEdgesInSSSP(env);
		}
	}

	private static Edge<Long, Double> getEdgeToBeRemoved() {
		if (fileOutput) {
			String [] edgeComponents =  edgeToBeRemoved.split(",");

			return new Edge<Long, Double>(Long.parseLong(edgeComponents[0]), Long.parseLong(edgeComponents[1]),
					Double.parseDouble(edgeComponents[2]));
		} else {
			System.err.println("Usage: IncrementalSSSP <vertex path> <edge path> <edges in SSSP> <edge to be removed> " +
					"<output path> <max iterations>");
			return IncrementalSSSPData.getDefaultEdgeToBeRemoved();
		}
	}
}
