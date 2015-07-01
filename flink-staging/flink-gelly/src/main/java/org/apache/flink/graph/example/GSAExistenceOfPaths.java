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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.PathExistenceData;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;


import java.util.HashSet;

/**
 * This example implements a program in which we find out the vertices for which there exists a path from given vertex
 *
 * The edges input file is expected to contain one edge per line, with long IDs and long values
 * The vertices input file is expected to contain one vertex per line with long IDs and no value
 * If no arguments are provided, the example runs with default data for this example
 */
public class GSAExistenceOfPaths implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Long, Long, Long>> edges = getEdgesDataSet(env);
		DataSet<Tuple2<Long, HashSet<Long>>> vertices = getVerticesDataSet(env);

		Graph<Long, HashSet<Long>, Long> graph = Graph.fromTupleDataSet(vertices, edges, env);

		GSAConfiguration parameters = new GSAConfiguration();
		parameters.setDirection(EdgeDirection.IN);
		// Execute the GSA iteration
		Graph<Long, HashSet<Long>, Long> result = graph.runGatherSumApplyIteration(new GetReachableVertices(),
																			new FindAllReachableVertices(),
																			new UpdateReachableVertices(),
																			maxIterations, parameters);

		// Extract the vertices as the result
		DataSet<Vertex<Long, HashSet<Long>>> reachableVertices = result.getVertices();

		// emit result
		if (fileOutput) {
			reachableVertices.writeAsCsv(outputPath, "\n", ",");

			env.execute("GSA Path Existence");
		} else {
			reachableVertices.print();
		}

	}

	// --------------------------------------------------------------------------------------------
	//  Path Existence UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GetReachableVertices extends GatherFunction<HashSet<Long>, Long, HashSet<Long>> {

		@Override
		public HashSet<Long> gather(Neighbor<HashSet<Long>, Long> neighbor) {
			return neighbor.getNeighborValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class FindAllReachableVertices extends SumFunction<HashSet<Long>, Long, HashSet<Long>> {
		@Override
		public HashSet<Long> sum(HashSet<Long> newSet, HashSet<Long> currentSet) {
			HashSet<Long> set = currentSet;
			for(Long l : newSet) {
				set.add(l);
			}
			return set;
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateReachableVertices extends ApplyFunction<Long, HashSet<Long>, HashSet<Long>> {

		@Override
		public void apply(HashSet<Long> newValue, HashSet<Long> currentValue) {
			newValue.addAll(currentValue);
			if(newValue.size()>currentValue.size()) {
				setResult(newValue);
			}
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String vertexInputPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage: GSAExistenceOfPaths <input vertices path> <input edges path> <output path>" +
						" <num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[1];
			vertexInputPath = args[0];
			outputPath = args[2];
			maxIterations = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing GSAExistenceOfPaths example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: GSAExistenceOfPaths <input vertices path> <input edges path> <output path> <num iterations>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Tuple3<Long, Long, Long>> getEdgesDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return(env.readCsvFile(edgeInputPath).fieldDelimiter(",")
					.lineDelimiter("\n").
					types(Long.class, Long.class, Long.class));
		}
		return PathExistenceData.getDefaultEdgeDataSet(env);
	}

	@SuppressWarnings("serial")
	private static DataSet<Tuple2<Long, HashSet<Long>>> getVerticesDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return env.readCsvFile(vertexInputPath).fieldDelimiter(",")
					.lineDelimiter("\n").
							types(Long.class).map(
							new MapFunction<Tuple1<Long>, Tuple2<Long, HashSet<Long>>>() {
								@Override
								public Tuple2<Long, HashSet<Long>> map(Tuple1<Long> value) throws Exception {
									HashSet<Long> h = new HashSet<Long>();
									h.add(value.f0);
									return new Tuple2<Long, HashSet<Long>>(value.f0, h);
								}
							}
					);
		}
		return PathExistenceData.getDefaultVertexDataSet(env);
	}

	@Override
	public String getDescription() {
		return "GSA Path Existence";
	}
}
