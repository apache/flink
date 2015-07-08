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
import org.apache.flink.graph.example.utils.AdamicAdarSimilarityMeasureData;
import java.util.HashSet;

/**
 * Given a directed, unweighted graph, return a weighted graph where the edge values are equal
 * to the Adamic Acard similarity coefficient which is given as
 * Summation of weights of common neighbors of the source and destination vertex
 *The weights are given as 1/log(nK) nK is the degree  or the vertex
 *@see <a href="http://social.cs.uiuc.edu/class/cs591kgk/friendsadamic.pdf">Friends and neighbors on the Web</a>
 *
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <br>
 * 	Edges are represented by pairs of srcVertexId, trgVertexId separated by tabs.
 * 	Edges themselves are separated by newlines.
 * 	For example: <code>1	2\n1	3\n</code> defines two edges 1-2 and 1-3.
 * </p>
 *
 * Usage <code> AdamicAdarSimilarityMeasure &lt;edge path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.AdamicAdarSimilarityMeasureData}
 */
@SuppressWarnings("serial")
public class AdamicAdarSimilarityMeasure implements ProgramDescription {


	public static void main(String[] args) throws  Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = AdamicAdarSimilarityMeasure.getEdgesDataSet(env);

		/*Graph is generated without vertex weights. Vertices will have a Tuple2 as value
		where first field will be the weight of the vertex as 1/log(kn) kn is the degree of the vertex
		Second field is a HashSet whose elements are again Tuple2, of which first field is the VertexID of the neighbor
		and second field is the weight of that neighbor
		*/
		Graph<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>, Double> graph =
				Graph.fromDataSet(edges,new MapVertices(), env);

		DataSet<Tuple2<Long, Long>> degrees = graph.getDegrees();

		//vertices are given weights in graph
		graph = graph.joinWithVertices(degrees,new AssignWeightToVertices());

		//neighbors are computed for all the vertices
		DataSet<Tuple2<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>>> computedNeighbors =
				graph.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);

		//graph is updated with the new vertex values
		Graph<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>, Double> graphWithVertexValues =
				graph.joinWithVertices(computedNeighbors, new UpdateGraphVertices());

		//edges are given Adamic Adar coefficient as value
		DataSet<Edge<Long, Double>> edgesWithAdamicValues = graphWithVertexValues.getTriplets().map(new ComputeAdamic());

		// emit result
		if (fileOutput) {
			edgesWithAdamicValues.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Executing Adamic Adar Similarity Measure");
		} else {
			edgesWithAdamicValues.print();
		}
	}

	@Override
	public String getDescription() {
		return "Adamic Adar Similarity Measure";
	}

	/*
	 * Each vertex has a Tuple2 as value. The first field of the Tuple is the weight of the vertex
	 * The second field is the HashSet containing of all the neighbors and their weights
	 */
	private static final class GatherNeighbors implements ReduceNeighborsFunction<Tuple2<Double,
																HashSet<Tuple2<Long, Double>>>> {
		@Override
		public Tuple2<Double, HashSet<Tuple2<Long, Double>>>
		reduceNeighbors(Tuple2<Double, HashSet<Tuple2<Long, Double>>> first,
						Tuple2<Double, HashSet<Tuple2<Long, Double>>> second) {
			first.f1.addAll(second.f1);
			return first;
		}
	}

	/**
	 * mapfunction to initialise Vertex values from edge dataset
	 */
	private static class MapVertices implements MapFunction<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>> {
		@Override
		public Tuple2<Double, HashSet<Tuple2<Long, Double>>> map(Long value) {
			HashSet<Tuple2<Long, Double>> neighbors = new HashSet<Tuple2<Long, Double>>();
			neighbors.add(new Tuple2<Long, Double>(value, 0.0));
			Double val = Double.MIN_VALUE;
			return new Tuple2<Double, HashSet<Tuple2<Long, Double>>>(val, neighbors);
		}
	}

	/**
	 * mapfunction to assign weight for each vertex as 1/log(kn) where kn is the degree of the vertex
	 */
	private static class AssignWeightToVertices implements
			MapFunction<Tuple2<Tuple2<Double, HashSet<Tuple2<Long, Double>>>, Long>,
					Tuple2<Double, HashSet<Tuple2<Long, Double>>>> {
		@Override
		public Tuple2<Double, HashSet<Tuple2<Long, Double>>>
		map(Tuple2<Tuple2<Double, HashSet<Tuple2<Long, Double>>>, Long> value) throws Exception {

			value.f0.f0 = 1.0/Math.log(value.f1);

			for(Tuple2<Long, Double> t : value.f0.f1) {
				t = new Tuple2<Long, Double> (t.f0, value.f0.f0);
				value.f0.f1.clear();
				value.f0.f1.add(t);
			}
			return value.f0;
		}
	}

	/**
	 * mapfunction to update values of vertices in the Graph
	 */
	private static class UpdateGraphVertices implements MapFunction<Tuple2<Tuple2<Double, HashSet<Tuple2<Long, Double>>>,
			Tuple2<Double, HashSet<Tuple2<Long, Double>>>>, Tuple2<Double, HashSet<Tuple2<Long, Double>>>> {
		@Override
		public Tuple2<Double, HashSet<Tuple2<Long, Double>>> map(Tuple2<Tuple2<Double,
				HashSet<Tuple2<Long, Double>>>, Tuple2<Double, HashSet<Tuple2<Long, Double>>>> tuple2)
				throws Exception {
			return tuple2.f1;
		}
	}

	/**
	 * Adamic Adar coefficients are calculated for each edge
	 * sourceSet is the HashSet of neighbors of source vertex
	 * targetSet is the HashSet of the neighbors of the target vertex
	 * intersection is the HashSet calculated from the sourceSet and targetSet which contains the
	 * common neighbors of source and target vertex
	 *
	 * The Adamic Adar coefficient is then the sum of the weights of these common neighbors
	 */
	private static final class ComputeAdamic implements
			MapFunction<Triplet<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>, Double>, Edge<Long, Double>> {

		@Override
		public Edge<Long, Double> map(Triplet<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>, Double> triplet)
				throws Exception {

			Vertex<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>> srcVertex = triplet.getSrcVertex();
			Vertex<Long, Tuple2<Double, HashSet<Tuple2<Long, Double>>>> trgVertex = triplet.getTrgVertex();

			Long x = srcVertex.getId();
			Long y = trgVertex.getId();
			HashSet<Tuple2<Long, Double>> sourceSet = srcVertex.getValue().f1;
			HashSet<Tuple2<Long, Double>> targetSet = trgVertex.getValue().f1;

			HashSet<Tuple2<Long, Double>> intersection = new HashSet<Tuple2<Long, Double>>(sourceSet);
			intersection.retainAll(targetSet);

			Double val = 0.0;
			for(Tuple2<Long, Double> t : intersection) {
				val = val+t.f1;
			}
			return new Edge<Long, Double>(x, y, val);
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
				System.err.println("Usage AdamicAdarSimilarityMeasure <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing AdamicAdarSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage AdamicAdarSimilarityMeasure <edge path> <output path>");
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
							return new Edge<Long, Double>(tuple2.f0, tuple2.f1, 0.0);
						}
					});
		} else {
			return AdamicAdarSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}