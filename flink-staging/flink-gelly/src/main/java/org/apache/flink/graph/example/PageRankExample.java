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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.PageRank;
import org.apache.flink.util.Collector;

public class PageRankExample implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Double>> pages = getPagesDataSet(env);

		DataSet<Edge<Long, Double>> links = getLinksDataSet(env);

		Graph<Long, Double, Double> network = Graph.fromDataSet(pages, links, env);

		DataSet<Tuple2<Long, Long>> vertexOutDegrees = network.outDegrees();

		// assign the transition probabilities as the edge weights
		Graph<Long, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees,
						new MapFunction<Tuple2<Double, Long>, Double>() {
							public Double map(Tuple2<Double, Long> value) {
								return value.f0 / value.f1;
							}
						});

		DataSet<Vertex<Long, Double>> pageRanks = networkWithWeights.run(
				new PageRank<Long>(numPages, DAMPENING_FACTOR, maxIterations))
				.getVertices();

		pageRanks.print();

		env.execute();
	}

	@Override
	public String getDescription() {
		return "PageRank";
	}

	private static final double DAMPENING_FACTOR = 0.85;
	private static long numPages = 10;
	private static int maxIterations = 10;

	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double>> getPagesDataSet(ExecutionEnvironment env) {
		return env.generateSequence(1, numPages).map(
				new MapFunction<Long, Vertex<Long, Double>>() {
					@Override
					public Vertex<Long, Double> map(Long l) throws Exception {
						return new Vertex<Long, Double>(l, 1.0 / numPages);
					}
				});

	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, Double>> getLinksDataSet(ExecutionEnvironment env) {
		return env.generateSequence(1, numPages).flatMap(
				new FlatMapFunction<Long, Edge<Long, Double>>() {
					@Override
					public void flatMap(Long key,
							Collector<Edge<Long, Double>> out) throws Exception {
						int numOutEdges = (int) (Math.random() * (numPages / 2));
						for (int i = 0; i < numOutEdges; i++) {
							long target = (long) (Math.random() * numPages) + 1;
							out.collect(new Edge<Long, Double>(key, target, 1.0));
						}
					}
				});
	}
}
