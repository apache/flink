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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.example.utils.JaccardSimilarityMeasureData;
import org.apache.flink.graph.utils.SplitVertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;


/**
 * This example shows how to use the Node Splitting Technique. More precisely, it introduces the following methods:
 * <ul>
 *  <li> determineSkewedVertices
 *  <li> treeDeAggregate - splitting
 *  <li> treeAggregate - merging
 *  <li> propagateValuesToSplitVertices
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
 * In addition to the regular set of parameters, the user must also pass three extra parameters: alpha, level and
 * threshold. Alpha is the number of subnodes in which a high - degree vertex will be split at each level. Level is the
 * maximum depth of the aggregation tree used to divide the node and threshold represents the lower margin for
 * distinguishing high degrees.
 *
 * Usage <code> JaccardSimilarityMeasure &lt;edge path&gt; &lt;result path&gt; &lt;alpha&gt; &lt;level&gt;
 * &lt;threshold&gt; </code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.JaccardSimilarityMeasureData}
 */
@SuppressWarnings("serial")
public class NodeSplittingJaccardSimilarityMeasure implements ProgramDescription {

	public static void main (String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		// initialize the vertex values with hash sets containing their own ids
		Graph<String, HashSet<String>, NullValue> graph = Graph.fromDataSet(edges,
				new MapFunction<String, HashSet<String>>() {

					@Override
					public HashSet<String> map(String id) throws Exception {
						HashSet<String> neighbors = new HashSet<String>();
						neighbors.add(id);

						return new HashSet<String>(neighbors);
					}
				}, env);

		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(threshold,
				verticesWithDegrees);

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithSplitVertices = SplitVertex
				.treeDeAggregate(skewedVertices, graph, alpha, level, threshold);

		// First Step: create the set of neighbors
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> updatedSplitVertices =
				gatherNeighborIds(graphWithSplitVertices);

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithNeighbors =
				Graph.fromDataSet(updatedSplitVertices, graphWithSplitVertices.getEdges(), env);

		// Second Step: compute Jaccard
		DataSet<Edge<String, Double>> edgesWithJaccardValues = compareNeighborSets(graphWithNeighbors);

		// emit result
		if (fileOutput) {
			edgesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Node Splitting GSA Jaccard Similarity Measure");
		} else {
			edgesWithJaccardValues.print();
		}
	}

	public static DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> gatherNeighborIds (
			Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithSplitVertices) {

		DataSet<Tuple2<String, Tuple2<String, HashSet<String>>>> computedNeighbors =
				graphWithSplitVertices.reduceOnNeighbors(new GatherNeighborsForSplitVertices(), EdgeDirection.ALL);

		// if the given subvertex has no value, joinWithVertices will keep the initial value,
		// yielding erroneous results
		graphWithSplitVertices = graphWithSplitVertices.mapVertices(new MapFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Tuple2<String, HashSet<String>>>() {
			@Override
			public Tuple2<String, HashSet<String>> map(Vertex<String, Tuple2<String, HashSet<String>>> vertex) throws Exception {
				// get the initial ID and a clean hash set
				return new Tuple2<String, HashSet<String>>(vertex.getValue().f0, new HashSet<String>());
			}
		});

		// joinWithVertices to update the node values
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> verticesWithNeighbors =
				graphWithSplitVertices.joinWithVertices(computedNeighbors, new MapFunction<Tuple2<Tuple2<String, HashSet<String>>, Tuple2<String, HashSet<String>>>, Tuple2<String, HashSet<String>>>() {
					@Override
					public Tuple2<String, HashSet<String>> map(Tuple2<Tuple2<String, HashSet<String>>,
							Tuple2<String, HashSet<String>>> tuple2) throws Exception {
						return tuple2.f1;
					}
				}).getVertices();

		DataSet<Vertex<String, HashSet<String>>> aggregatedVertices =
				SplitVertex.treeAggregate(verticesWithNeighbors, level, new AggregateNeighborSets())
						.map(new MapFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Vertex<String, HashSet<String>>>() {

							@Override
							public Vertex<String, HashSet<String>> map(Vertex<String, Tuple2<String, HashSet<String>>> vertex) throws Exception {
								return new Vertex<String, HashSet<String>>(vertex.getId(), vertex.getValue().f1);
							}
						});

		// propagate aggregated values to the split vertices
		// avoid a second treeDeAggregate
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> updatedSplitVertices =
				SplitVertex.propagateValuesToSplitVertices(graphWithSplitVertices.getVertices(),
						aggregatedVertices);

		return updatedSplitVertices;
	}

	public static DataSet<Edge<String, Double>> compareNeighborSets(
			Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithNeighbors) {

		DataSet<Edge<String, Double>> splitEdgesWithJaccardValues =
				graphWithNeighbors.getTriplets().map(new ComputeJaccardForSplitVertices());

		DataSet<Edge<String, Double>> edgesWithJaccardValues =
				SplitVertex.cleanupEdges(splitEdgesWithJaccardValues);

		return  edgesWithJaccardValues;
	}

	@Override
	public String getDescription() {
		return "Node Splitting Jaccard Similarity Measure";
	}

	private static final class GatherNeighborsForSplitVertices implements
			ReduceNeighborsFunction<Tuple2<String, HashSet<String>>> {

		@Override
		public Tuple2<String, HashSet<String>> reduceNeighbors(Tuple2<String, HashSet<String>> first,
															Tuple2<String, HashSet<String>> second) {
			first.f1.addAll(second.f1);
			return first;
		}
	}

	/**
	 * UDF that describes the combining strategy: the partial hash sets containing neighbor values will be reduced
	 * to a single hash set per vertex.
	 */
	private static final class AggregateNeighborSets implements
			GroupReduceFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Vertex<String, Tuple2<String, HashSet<String>>>> {

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, HashSet<String>>>> vertex,
						Collector<Vertex<String, Tuple2<String, HashSet<String>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, HashSet<String>>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, HashSet<String>>> next = null;
			HashSet<String> neighborsPerVertex = new HashSet<>();
			String id = null;

			while (vertexIterator.hasNext()) {
				next = vertexIterator.next();
				id = next.getId();
				neighborsPerVertex.addAll(next.getValue().f1);
			}

			collector.collect(new Vertex<String, Tuple2<String, HashSet<String>>>(id,
					new Tuple2<String, HashSet<String>>(id, neighborsPerVertex)));
		}
	}

	private static final class ComputeJaccardForSplitVertices implements
			MapFunction<Triplet<String, Tuple2<String, HashSet<String>>, NullValue>, Edge<String, Double>> {

		@Override
		public Edge<String, Double> map(Triplet<String, Tuple2<String, HashSet<String>>, NullValue> triplet) throws Exception {

			Vertex<String, Tuple2<String, HashSet<String>>> srcVertex = triplet.getSrcVertex();
			Vertex<String, Tuple2<String, HashSet<String>>> trgVertex = triplet.getTrgVertex();

			String x = srcVertex.getId();
			String y = trgVertex.getId();
			HashSet<String> neighborSetY = trgVertex.getValue().f1;

			double unionPlusIntersection = srcVertex.getValue().f1.size() + neighborSetY.size();
			// within a HashSet, all elements are distinct
			HashSet<String> unionSet = new HashSet<String>();
			unionSet.addAll(srcVertex.getValue().f1);
			unionSet.addAll(neighborSetY);
			double union = unionSet.size();
			double intersection = unionPlusIntersection - union;

			return new Edge<String, Double>(x, y, intersection/union);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static Integer alpha = JaccardSimilarityMeasureData.ALPHA;
	private static Integer level = JaccardSimilarityMeasureData.LEVEL;
	private static Integer threshold = JaccardSimilarityMeasureData.THRESHOLD;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 5) {
				System.err.println("Usage NodeSplittingJaccardSimilarityMeasure <edge path> <output path>" +
						"<alpha> <level> <threshold>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			alpha = Integer.parseInt(args[2]);
			level = Integer.parseInt(args[3]);
			threshold = Integer.parseInt(args[4]);
		} else {
			System.out.println("Executing NodeSplittingJaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage NodeSplittingJaccardSimilarityMeasure <edge path> <output path> <alpha>" +
					"<level> <threshold>");
		}

		return true;
	}

	private static DataSet<Edge<String, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(String.class, String.class)
					.map(new MapFunction<Tuple2<String, String>, Edge<String, NullValue>>() {
						@Override
						public Edge<String, NullValue> map(Tuple2<String, String> tuple2) throws Exception {
							return new Edge<String, NullValue>(tuple2.f0, tuple2.f1, NullValue.getInstance());
						}
					});
		} else {
			return JaccardSimilarityMeasureData.getDefaultStringNullValueEdgeDataSet(env);
		}
	}

}
