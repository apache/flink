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

import java.util.Collection;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.example.utils.ExampleUtils;
import org.apache.flink.types.NullValue;

/**
 * 
 * A simple example to illustrate the basic functionality of the graph-api.
 * The program creates a random graph and computes and prints
 * the following metrics:
 * - number of vertices
 * - number of edges
 * - average node degree
 * - the vertex ids with the max/min in- and out-degrees
 *
 */
public class GraphMetrics implements ProgramDescription {

	static final int NUM_VERTICES = 100;
	static final long SEED = 9876;
	

	@Override
	public String getDescription() {
		return "Graph Metrics Example";
	}

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/** create a random graph **/
		Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(ExampleUtils
				.getRandomEdges(env, NUM_VERTICES), env);
		
		/** get the number of vertices **/
		DataSet<Integer> numVertices = graph.numberOfVertices();
		
		/** get the number of edges **/
		DataSet<Integer> numEdges = graph.numberOfEdges();
		
		/** compute the average node degree **/
		DataSet<Tuple2<Long, Long>> verticesWithDegrees = graph.getDegrees();

		DataSet<Double> avgNodeDegree = verticesWithDegrees
				.aggregate(Aggregations.SUM, 1).map(new AvgNodeDegreeMapper())
				.withBroadcastSet(numVertices, "numberOfVertices");
		
		/** find the vertex with the maximum in-degree **/
		DataSet<Long> maxInDegreeVertex = graph.inDegrees().maxBy(1).map(new ProjectVertexId());

		/** find the vertex with the minimum in-degree **/
		DataSet<Long> minInDegreeVertex = graph.inDegrees().minBy(1).map(new ProjectVertexId());

		/** find the vertex with the maximum out-degree **/
		DataSet<Long> maxOutDegreeVertex = graph.outDegrees().maxBy(1).map(new ProjectVertexId());

		/** find the vertex with the minimum out-degree **/
		DataSet<Long> minOutDegreeVertex = graph.outDegrees().minBy(1).map(new ProjectVertexId());
		
		/** print the results **/
		ExampleUtils.printResult(numVertices, "Total number of vertices");
		ExampleUtils.printResult(numEdges, "Total number of edges");
		ExampleUtils.printResult(avgNodeDegree, "Average node degree");
		ExampleUtils.printResult(maxInDegreeVertex, "Vertex with Max in-degree");
		ExampleUtils.printResult(minInDegreeVertex, "Vertex with Min in-degree");
		ExampleUtils.printResult(maxOutDegreeVertex, "Vertex with Max out-degree");
		ExampleUtils.printResult(minOutDegreeVertex, "Vertex with Min out-degree");

		env.execute();
	}
	
	@SuppressWarnings("serial")
	private static final class AvgNodeDegreeMapper extends RichMapFunction<Tuple2<Long, Long>, Double> {

		private int numberOfVertices;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Integer> bCastSet = getRuntimeContext()
					.getBroadcastVariable("numberOfVertices");
			numberOfVertices = bCastSet.iterator().next();
		}
		
		public Double map(Tuple2<Long, Long> sumTuple) {
			return (double) (sumTuple.f1 / numberOfVertices) ;
		}
	}

	@SuppressWarnings("serial")
	private static final class ProjectVertexId implements MapFunction<Tuple2<Long,Long>, Long> {
		public Long map(Tuple2<Long, Long> value) { return value.f0; }
	}
}
