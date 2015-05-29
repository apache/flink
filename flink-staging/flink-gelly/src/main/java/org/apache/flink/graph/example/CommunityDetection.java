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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.CommunityDetectionData;
import org.apache.flink.graph.library.CommunityDetectionAlgorithm;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;

/**
 * This example shows how to use the {@link org.apache.flink.graph.library.CommunityDetectionAlgorithm}
 * library method:
 * <ul>
 * 	<li> with the edge data set given as a parameter
 * 	<li> with default data
 * </ul>
 *
 * The input file is a plain text file and must be formatted as follows:
 * Edges are represented by tuples of srcVertexId, trgVertexId, weight which are
 * separated by tabs. Edges themselves are separated by newlines.
 * For example: <code>1\t2\t1.0\n1\t3\t2.0\n</code> defines two edges,
 * 1-2 with weight 1.0 and 1-3 with weight 2.0.
 *
 * Usage <code>CommunityDetection &lt;edge path&gt; &lt;result path&gt;
 * &lt;number of iterations&gt; &lt;delta&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.CommunityDetectionData}
 */
public class CommunityDetection implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// set up the graph
		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);
		Graph<Long, Long, Double> graph = Graph.fromDataSet(edges,
				new MapFunction<Long, Long>() {

					public Long map(Long label) {
						return label;
					}
				}, env);

		// the result is in the form of <vertexId, communityId>, where the communityId is the label
		// which the vertex converged to
		DataSet<Vertex<Long, Long>> communityVertices =
				graph.run(new CommunityDetectionAlgorithm(maxIterations, delta)).getVertices();

		// emit result
		if (fileOutput) {
			communityVertices.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Executing Community Detection Example");
		} else {
			communityVertices.print();
		}

	}

	@Override
	public String getDescription() {
		return "Community Detection";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = CommunityDetectionData.MAX_ITERATIONS;
	private static Double delta = CommunityDetectionData.DELTA;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage CommunityDetection <edge path> <output path> " +
						"<num iterations> <delta>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			delta = Double.parseDouble(args[3]);

		} else {
			System.out.println("Executing SimpleCommunityDetection example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage CommunityDetection <edge path> <output path> " +
					"<num iterations> <delta>");
		}

		return true;
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new Tuple3ToEdgeMap<Long, Double>());
		} else {
			return CommunityDetectionData.getDefaultEdgeDataSet(env);
		}
	}
}
