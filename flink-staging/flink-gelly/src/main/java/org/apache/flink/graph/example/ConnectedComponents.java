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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.ConnectedComponentsDefaultData;
import org.apache.flink.graph.library.ConnectedComponentsAlgorithm;
import org.apache.flink.types.NullValue;

/**
 * This example shows how to use the {@link org.apache.flink.graph.library.ConnectedComponentsAlgorithm}
 * library method:
 * <ul>
 * 	<li> with the edge data set given as a parameter
 * 	<li> with default data
 * </ul>
 *
 * The input file is a plain text file and must be formatted as follows:
 * Edges are represented by tuples of srcVertexId, trgVertexId which are
 * separated by tabs. Edges themselves are separated by newlines.
 * For example: <code>1\t2\n1\t3\n</code> defines two edges,
 * 1-2 with and 1-3.
 *
 * Usage <code>ConnectedComponents &lt;edge path&gt; &lt;result path&gt;
 * &lt;number of iterations&gt; </code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.ConnectedComponentsDefaultData}
 */
public class ConnectedComponents implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return value;
			}
		}, env);

		DataSet<Vertex<Long, Long>> verticesWithMinIds = graph
				.run(new ConnectedComponentsAlgorithm(maxIterations)).getVertices();

		// emit result
		if (fileOutput) {
			verticesWithMinIds.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Connected Components Example");
		} else {
			verticesWithMinIds.print();
		}


	}

	@Override
	public String getDescription() {
		return "Connected Components Example";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = ConnectedComponentsDefaultData.MAX_ITERATIONS;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage ConnectedComponents <edge path> <output path> " +
						"<num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);

		} else {
			System.out.println("Executing ConnectedComponents example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage ConnectedComponents <edge path> <output path> " +
					"<num iterations>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(Tuple2<Long, Long> value) throws Exception {
							return new Edge<Long, NullValue>(value.f0, value.f1, NullValue.getInstance());
						}
					});
		} else {
			return ConnectedComponentsDefaultData.getDefaultEdgeDataSet(env);
		}
	}
}
