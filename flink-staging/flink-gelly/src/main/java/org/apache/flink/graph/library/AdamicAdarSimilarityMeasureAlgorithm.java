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

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Triplet;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * Given a directed, unweighted graph, return a weighted graph where the edge weights are equal
 * to the Adamic Adar similarity coefficient which is given as
 * Summation of Adamic Adar weights of common neighbors of the source and destination vertex
 * The Adamic Adar weights are given as 1/log(nK) nK is the degree  or the vertex
 *
 * @see <a href="http://social.cs.uiuc.edu/class/cs591kgk/friendsadamic.pdf">Friends and neighbors on the Web</a>
 * <p/>
 */
@SuppressWarnings("serial")
public class AdamicAdarSimilarityMeasureAlgorithm implements GraphAlgorithm<Long, Long, Double> {
	
	@Override
	public Graph<Long, Long, Double> run(Graph<Long, Long, Double> input) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = input.getEdges();
		
		DataSet<Vertex<Long, Tuple2<Double, HashSet<Long>>>> verts = edges.flatMap(new GetReverse()).groupBy(0).reduceGroup(new EdgeReduce());
		
		/*Graph is generated without Adamic Adar weights for vertices. Vertices will have a Tuple2 as value
		where first field will be the weight of the vertex as 1/log(kn) kn is the degree of the vertex
		Second field is a HashSet whose elements are the VertexIDs of the neighbor
		*/
		Graph<Long, Tuple2<Double, HashSet<Long>>, Double> graph = Graph.fromDataSet(verts, edges, env);
		
		//Partial weights for edges are collected
		DataSet<Tuple3<Long, Long, Double>> edgesWithAdamicValues;
		
		edgesWithAdamicValues = graph.getTriplets().flatMap(new ComputeAdamic());
		
		//Partial weights for the edges are added
		edgesWithAdamicValues = edgesWithAdamicValues.groupBy(0,1).reduce(new AdamGroup());
		
		//Graph is updated with the Adamic Adar Edges
		input = input.joinWithEdges(edgesWithAdamicValues, new JoinEdge());
		
		return input;
	}
		
	/**
	 * Adamic Adar partial edges are found from the edges
	 * sourceSet is the HashSet of neighbors of source vertex
	 * targetSet is the HashSet of the neighbors of the target vertex
	 * intersection is the HashSet calculated from the sourceSet and targetSet and contains the
	 * common neighbors of source and target vertex
	 * <p/>
	 * The Adamic Adar coefficient is then the sum of the weights of these common neighbors
	 */
	private static final class ComputeAdamic implements 
					FlatMapFunction<Triplet<Long, Tuple2<Double, HashSet<Long>>, Double>, Tuple3<Long, Long, Double>> {
		@Override
		public void flatMap(Triplet<Long, Tuple2<Double, HashSet<Long>>, Double> triplet, Collector<Tuple3<Long, Long, Double>> out)
																								throws Exception {
			Vertex<Long, Tuple2<Double, HashSet<Long>>> srcVertex = triplet.getSrcVertex();
			Vertex<Long, Tuple2<Double, HashSet<Long>>> trgVertex = triplet.getTrgVertex();
			HashSet<Long> sourceSet = srcVertex.getValue().f1;
			HashSet<Long> targetSet = trgVertex.getValue().f1;
			HashSet<Long> intersection = new HashSet<Long>(sourceSet);
			intersection.retainAll(targetSet);

			for (Long t : intersection) {
				out.collect(new Tuple3<Long, Long, Double>(trgVertex.f0, t, srcVertex.f1.f0));
				out.collect(new Tuple3<Long, Long, Double>(srcVertex.f0, t, trgVertex.f1.f0));
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
				return new Tuple3<Long, Long, Double>(value1.f0, value1.f1, (value1.f2+value2.f2));
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
	
	/**
	 * FlatMapFunction to get reverse edges for the edges dataset
	 */
	private static final class GetReverse implements FlatMapFunction<Edge<Long, Double>, Edge<Long, Double>> {

		@Override
		public void flatMap(Edge<Long, Double> value,
				Collector<Edge<Long, Double>> out) throws Exception {
			out.collect(new Edge<Long, Double>(value.getTarget(), value.getSource(), value.getValue()));
			out.collect(value);
		}
		
	}
	
	/**
	 * GroupReduceFunction to reduce on the edge DataSet in order to get the Vertex DataSet
	 * Vertices are also given Adamic Adar weights
	 */
	private static final class EdgeReduce implements GroupReduceFunction<Edge<Long, Double>, Vertex<Long, Tuple2<Double, HashSet<Long>>>> {

		@Override
		public void reduce(Iterable<Edge<Long, Double>> values,
				Collector<Vertex<Long, Tuple2<Double, HashSet<Long>>>> out) throws Exception {
			int count = 0;
			Long source = -1L;
			HashSet<Long> hash = new HashSet<Long>();
			for(Edge<Long, Double> edge : values) {
				if(source == -1) {
					source = edge.getSource();
				}
				hash.add(edge.getTarget());
			}
			count = hash.size();
			out.collect(new Vertex<Long, Tuple2<Double, HashSet<Long>>>(source, new Tuple2<Double, HashSet<Long>>(1.0 / Math.log(count), hash)));
		}
	}
}

