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
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.example.utils.AdamicAdarSimilarityMeasureData;
import org.apache.flink.util.Collector;
import java.util.HashSet;

/**
 * Given a directed, unweighted graph, return a weighted graph where the edge values are equal
 * to the Adamic Adar similarity coefficient which is given as
 * Summation of weights of common neighbors of the source and destination vertex
 * The Adamic Adar weights are given as 1/log(nK) nK is the degree  or the vertex
 *
 * @see <a href="http://social.cs.uiuc.edu/class/cs591kgk/friendsadamic.pdf">Friends and neighbors on the Web</a>
 * <p/>
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <br>
 * Edges are represented by pairs of srcVertexId, trgVertexId separated by tabs.
 * Edges themselves are separated by newlines.
 * For example: <code>1	2\n1	3\n</code> defines two edges 1-2 and 1-3.
 * </p>
 * <p>
 * This implementation makes use of BloomFilters from the library provided by Google Guava.
 * In this implementation, default probability of False Positives is used (3%)
 * </p>
 * <p/>
 * Usage <code> AdamicAdarApproxSimilarityMeasure &lt;edge path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.AdamicAdarSimilarityMeasureData}
 */

@SuppressWarnings("serial")
public class AdamicAdarApproxSimilarityMeasure implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = AdamicAdarApproxSimilarityMeasure.getEdgesDataSet(env);

		/*Graph is generated without Adamic Adar weights for vertices. Vertices will have a Tuple2 as value
		where first field will be the weight of the vertex as 1/log(kn) kn is the degree of the vertex
		Second field is a HashSet whose elements are the VertexIDs of the neighbor
		*/
		Graph<Long, Tuple2<Double, HashSet<Long>>, Double> graph = Graph.fromDataSet(edges, new MapVertices(), env);

		DataSet<Tuple2<Long, Long>> degrees = graph.getDegrees();

		//neighbors are computed for all the vertices
			DataSet<Tuple2<Long, Tuple2<Double, HashSet<Long>>>> computedNeighbors = graph.reduceOnNeighbors(new GatherNeighbors(),
																										EdgeDirection.ALL);
		
			DataSet<Tuple2<Long, BloomFilter<Long>>> verticesWithBloomFilters = computedNeighbors.map(new HashToBloom());
		
		//graph is updated with the new vertex values
			Graph<Long, Tuple2<Double, HashSet<Long>>, Double> graphWithVertexValues = 
																	graph.joinWithVertices(computedNeighbors, new UpdateGraphVertices());
			
		//vertices are given values in graph in accordance with the Adamic Adar rule
			graphWithVertexValues = graphWithVertexValues.joinWithVertices(degrees, new AssignWeightToVertices());
		
		//Partial Adamic Adar Edges are calculated
			DataSet<Tuple3<Long, Long, Double>> edgesWithAdamicValues= graphWithVertexValues.getTriplets().join(verticesWithBloomFilters).where(1).equalTo(0).with(new ComputeAdamic());
	
		//Partial weights for the edges are added
			edgesWithAdamicValues = edgesWithAdamicValues.groupBy(0,1).reduce(new AdamGroup());

		//Graph is updated with the Adamic Adar Edges
			graph = graph.joinWithEdges(edgesWithAdamicValues, new JoinEdge());

		// emit result
				if (fileOutput) {
					graph.getEdges().writeAsCsv(outputPath, "\n", ",");

					// since file sinks are lazy, we trigger the execution explicitly
					env.execute("Executing Adamic Adar Similarity Measure");
				} else {
					graph.getEdges().print();
				}
	}
	
	@Override
	public String getDescription() {
		return "Vertex Adamic Adar Similarity Measure";
	}
	
	/**
	 * Funnel for the BloomFilters
	 */
	public static final class LongFunnel implements Funnel<Long> {
	
		@Override
			public void funnel(Long from, PrimitiveSink into) {	
				into.putLong(from);
		}	
	}
	
	//Predicted number of insertions in Neighborhood BloomFilters
	private static final int insertionNumber=1000; 
	
	/**
	 * MapFunction to get a DataSet with Vertex IDs and a BloomFilter containing their neighbors.
	 */
	private static final class HashToBloom implements MapFunction<Tuple2<Long, Tuple2<Double, HashSet<Long>>>,
	Tuple2<Long, BloomFilter<Long>>>{
	@Override
		public Tuple2<Long, BloomFilter<Long>> map(Tuple2<Long, Tuple2<Double, HashSet<Long>>> value) throws Exception {
					
			BloomFilter<Long> bloom = BloomFilter.create(new LongFunnel(), insertionNumber);
					
			for(long id: value.f1.f1) {
				bloom.put(id);
			}
		return new Tuple2<Long, BloomFilter<Long>>(value.f0, bloom);	
		}						
	}								
	
	/**
	 * MapFunction to initialise Vertex values from edge dataset
	 */
	private static class MapVertices implements MapFunction<Long, Tuple2<Double, HashSet<Long> >> {
		@Override
		public Tuple2<Double, HashSet<Long>> map(Long value) {
			HashSet<Long> neighbors = new HashSet<Long>();
			neighbors.add(value);
			Double val = Double.MIN_VALUE;
			return new Tuple2<Double, HashSet<Long>>(val, neighbors);
		}
	}
	
	/**
	 * Each vertex has a Tuple2 as value. The first field of the Tuple is the Adamic Adar weight of the vertex
	 * The second field is the HashSet containing of all the neighbor IDs
	 * Neighbors are found for eah vertex
	 */
	private static final class GatherNeighbors implements ReduceNeighborsFunction<Tuple2<Double, HashSet<Long>>> {
		@Override
		public Tuple2<Double, HashSet<Long>>
		reduceNeighbors(Tuple2<Double, HashSet<Long>> first, Tuple2<Double, HashSet<Long>> second) {
			first.f1.addAll(second.f1);
			return first;
		}
	}

	/**
	 * MapFunction to update values of vertices in the Graph
	 */
	private static class UpdateGraphVertices implements MapFunction<Tuple2<Tuple2<Double, HashSet<Long>>,
			Tuple2<Double, HashSet<Long>>>, Tuple2<Double, HashSet<Long>>> {
		@Override
		public Tuple2<Double, HashSet<Long>> map(Tuple2<Tuple2<Double, HashSet<Long>>, Tuple2<Double, HashSet<Long>>> tuple2)
				throws Exception {
			return tuple2.f1;
		}
	}
	
	/**
	 * MapFunction to assign Adamic Adar weight for each vertex as 1/log(kn) where kn is the degree of the vertex
	 */
	private static class AssignWeightToVertices implements
			MapFunction<Tuple2<Tuple2<Double, HashSet<Long>>, Long>, Tuple2<Double, HashSet<Long>>> {
		@Override
		public Tuple2<Double, HashSet<Long>>
		map(Tuple2<Tuple2<Double, HashSet<Long>>, Long> value) throws Exception {

			value.f0.f0 = 1.0 / Math.log(value.f1);
			return value.f0;
		}
	}

	/**
	 * Adamic Adar partial edges are found from the edges
	 * bloom is the BloomFilter for the elements in the neighborhood of Target Vertex
	 * <p/>
	 * Partial Adamic Adar Edges are collected using FlatJoinFunction
	 */
	private static final class ComputeAdamic implements FlatJoinFunction<Triplet<Long, Tuple2<Double, HashSet<Long>>, Double>,
																		Tuple2<Long, BloomFilter<Long>>, Tuple3<Long, Long, Double>> {

		@Override
		public void join(
				Triplet<Long, Tuple2<Double, HashSet<Long>>, Double> first,
				Tuple2<Long, BloomFilter<Long>> second,
				Collector<Tuple3<Long, Long, Double>> out) throws Exception {
			Vertex<Long, Tuple2<Double, HashSet<Long>>> srcVertex = first.getSrcVertex();
			Vertex<Long, Tuple2<Double, HashSet<Long>>> trgVertex = first.getTrgVertex();
			BloomFilter<Long> bloom = second.f1;
			for(long t: srcVertex.f1.f1) {
				if(bloom.mightContain(t)) {
					out.collect(new Tuple3<Long, Long, Double>(second.f0, t, srcVertex.f1.f0));
					out.collect(new Tuple3<Long, Long, Double>(srcVertex.f0, t, trgVertex.f1.f0));
				}
			}
		}
	}
	
	/**
	 *MapFunction to reduce the Grouped DataSet by adding the weights of the partial Edges
	 */
	private static final class AdamGroup implements ReduceFunction<Tuple3<Long, Long, Double>> {

		@Override
		public Tuple3<Long, Long, Double> reduce(Tuple3<Long, Long, Double> value1, Tuple3<Long, Long, Double> value2)
																			throws Exception {
				return new Tuple3<Long, Long, Double>(value1.f0, value1.f1, value1.f2+value2.f2);
		}
	}
	
	/**
	 * MapFunction to calculate weights of Adamic Adar Edges and update the graph
	 */
	private static final class JoinEdge implements MapFunction<Tuple2<Double, Double>, Double> {
		@Override
		public Double map(Tuple2<Double, Double> value) {
			return (value.f0+value.f1);
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
							return new Edge<Long, Double>(tuple2.f0, tuple2.f1, new Double(0));
						}
					});
		} else {
			return AdamicAdarSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}