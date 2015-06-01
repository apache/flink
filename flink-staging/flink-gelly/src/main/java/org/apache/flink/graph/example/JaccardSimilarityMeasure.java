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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.example.utils.JaccardSimilarityMeasureData;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
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

		Graph<Long, NullValue, Double> graph = Graph.fromDataSet(edges, env);

		DataSet<Vertex<Long, HashSet<Long>>> verticesWithNeighbors =
				graph.groupReduceOnEdges(new GatherNeighbors(), EdgeDirection.ALL);

		Graph<Long, HashSet<Long>, Double> graphWithVertexValues = Graph.fromDataSet(verticesWithNeighbors, edges, env);

		// the edge value will be the Jaccard similarity coefficient(number of common neighbors/ all neighbors)
		DataSet<Tuple3<Long, Long, Double>> edgesWithJaccardWeight = graphWithVertexValues.getTriplets()
				.map(new WeighEdgesMapper());

		DataSet<Edge<Long, Double>> result = graphWithVertexValues.joinWithEdges(edgesWithJaccardWeight,
				new MapFunction<Tuple2<Double, Double>, Double>() {

					@Override
					public Double map(Tuple2<Double, Double> value) throws Exception {
						return value.f1;
					}
				}).getEdges();

		// emit result
		if (fileOutput) {
			result.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Executing Jaccard Similarity Measure");
		} else {
			result.print();
		}

	}

	@Override
	public String getDescription() {
		return "Vertex Jaccard Similarity Measure";
	}

	/**
	 * Each vertex will have a HashSet containing its neighbor ids as value.
	 */
	private static final class GatherNeighbors implements EdgesFunction<Long, Double, Vertex<Long, HashSet<Long>>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Double>>> edges,
														Collector<Vertex<Long, HashSet<Long>>> out) throws Exception {

			HashSet<Long> neighborsHashSet = new HashSet<Long>();
			long vertexId = -1;

			for(Tuple2<Long, Edge<Long, Double>> edge : edges) {
				neighborsHashSet.add(getNeighborID(edge));
				vertexId = edge.f0;
			}
			out.collect(new Vertex<Long, HashSet<Long>>(vertexId, neighborsHashSet));
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
	private static class WeighEdgesMapper implements MapFunction<Triplet<Long, HashSet<Long>, Double>,
			Tuple3<Long, Long, Double>> {

		@Override
		public Tuple3<Long, Long, Double> map(Triplet<Long, HashSet<Long>, Double> triplet)
				throws Exception {

			Vertex<Long, HashSet<Long>> source = triplet.getSrcVertex();
			Vertex<Long, HashSet<Long>> target = triplet.getTrgVertex();

			long unionPlusIntersection = source.getValue().size() + target.getValue().size();
			// within a HashSet, all elements are distinct
			source.getValue().addAll(target.getValue());
			// the source value contains the union
			long union = source.getValue().size();
			long intersection = unionPlusIntersection - union;

			return new Tuple3<Long, Long, Double>(source.getId(), target.getId(), (double) intersection/union);
		}
	}

	/**
	 * Helper method that extracts the neighborId given an edge.
	 * @param edge
	 * @return
	 */
	private static Long getNeighborID(Tuple2<Long, Edge<Long, Double>> edge) {
		if(edge.f1.getSource() == edge.f0) {
			return edge.f1.getTarget();
		} else {
			return edge.f1.getSource();
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
