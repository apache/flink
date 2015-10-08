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
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.example.utils.JaccardSimilarityMeasureData;

import java.util.HashSet;

/**
 * This example shows how to use
 * <ul>
 *  <li> neighborhood methods
 *  <li> join with vertices
 *  <li> triplets
 * </ul>
 * 
 * Given a directed, unweighted graph, return a weighted graph where the edge values are equal
 * to the Jaccard similarity coefficient - the number of common neighbors divided by the the size
 * of the union of neighbor sets - for the src and target vertices.
 *
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <br>
 * 	Edges are represented by pairs of srcVertexId, trgVertexId separated by tabs.
 * 	Edges themselves are separated by newlines.
 * 	For example: <code>1	2\n1	3\n</code> defines two edges 1-2 and 1-3.
 * </p>
 *
 * Usage <code> JaccardSimilarityMeasure &lt;edge path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.JaccardSimilarityMeasureData}
 */
@SuppressWarnings("serial")
public class JaccardSimilarityMeasure implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		Graph<Long, HashSet<Long>, Double> graph = Graph.fromDataSet(edges,
				new MapFunction<Long, HashSet<Long>>() {

					@Override
					public HashSet<Long> map(Long id) throws Exception {
						HashSet<Long> neighbors = new HashSet<Long>();
						neighbors.add(id);

						return new HashSet<Long>(neighbors);
					}
				}, env);

		// create the set of neighbors
		DataSet<Tuple2<Long, HashSet<Long>>> computedNeighbors =
				graph.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);

		// join with the vertices to update the node values
		Graph<Long, HashSet<Long>, Double> graphWithVertexValues =
				graph.joinWithVertices(computedNeighbors, new MapFunction<Tuple2<HashSet<Long>, HashSet<Long>>,
						HashSet<Long>>() {

					@Override
					public HashSet<Long> map(Tuple2<HashSet<Long>, HashSet<Long>> tuple2) throws Exception {
						return tuple2.f1;
					}
				});

		// compare neighbors, compute Jaccard
		DataSet<Edge<Long, Double>> edgesWithJaccardValues =
				graphWithVertexValues.getTriplets().map(new ComputeJaccard());

		// emit result
		if (fileOutput) {
			edgesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Executing Jaccard Similarity Measure");
		} else {
			edgesWithJaccardValues.print();
		}

	}

	@Override
	public String getDescription() {
		return "Vertex Jaccard Similarity Measure";
	}

	/**
	 * Each vertex will have a HashSet containing its neighbor ids as value.
	 */
	private static final class GatherNeighbors implements ReduceNeighborsFunction<HashSet<Long>> {

		@Override
		public HashSet<Long> reduceNeighbors(HashSet<Long> first, HashSet<Long> second) {
			first.addAll(second);
			return new HashSet<Long>(first);
		}
	}

	/**
	 * The edge weight will be the Jaccard coefficient, which is computed as follows:
	 *
	 * Consider the edge x-y
	 * We denote by sizeX and sizeY, the neighbors hash set size of x and y respectively.
	 * sizeX+sizeY = union + intersection of neighborhoods
	 * size(hashSetX.addAll(hashSetY)).distinct = union of neighborhoods
	 * The intersection can then be deduced.
	 *
	 * The Jaccard similarity coefficient is then, the intersection/union.
	 */
	private static final class ComputeJaccard implements
			MapFunction<Triplet<Long, HashSet<Long>, Double>, Edge<Long, Double>> {

		@Override
		public Edge<Long, Double> map(Triplet<Long, HashSet<Long>, Double> triplet) throws Exception {

			Vertex<Long, HashSet<Long>> srcVertex = triplet.getSrcVertex();
			Vertex<Long, HashSet<Long>> trgVertex = triplet.getTrgVertex();

			Long x = srcVertex.getId();
			Long y = trgVertex.getId();
			HashSet<Long> neighborSetY = trgVertex.getValue();

			double unionPlusIntersection = srcVertex.getValue().size() + neighborSetY.size();
			// within a HashSet, all elements are distinct
			HashSet<Long> unionSet = new HashSet<Long>();
			unionSet.addAll(srcVertex.getValue());
			unionSet.addAll(neighborSetY);
			double union = unionSet.size();
			double intersection = unionPlusIntersection - union;

			return new Edge<Long, Double>(x, y, intersection/union);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage JaccardSimilarityMeasure <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing JaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage JaccardSimilarityMeasure <edge path> <output path>");
		}

		return true;
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, Double>>() {
						@Override
						public Edge<Long, Double> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<Long, Double>(tuple2.f0, tuple2.f1, new Double(0));
						}
					});
		} else {
			return JaccardSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}
