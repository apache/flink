/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.spargel.java.examples;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.spargel.java.MessageIterator;
import eu.stratosphere.spargel.java.MessagingFunction;
import eu.stratosphere.spargel.java.OutgoingEdge;
import eu.stratosphere.spargel.java.VertexCentricIteration;
import eu.stratosphere.spargel.java.VertexUpdateFunction;
import eu.stratosphere.util.Collector;

/**
 * An implementation of the basic PageRank algorithm in the vertex-centric API (spargel).
 * In this implementation, the edges carry a weight (the transition probability).
 */
@SuppressWarnings("serial")
public class SpargelPageRankCountingVertices {
	
	private static final double BETA = 0.85;

	
	public static void main(String[] args) throws Exception {
		final int NUM_VERTICES = 100;
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// a list of vertices
		DataSet<Long> vertices = env.generateSequence(1, NUM_VERTICES);
		
		// generate some random edges. the transition probability on each edge is 1/num-out-edges of the source vertex
		DataSet<Tuple3<Long, Long, Double>> edgesWithProbability = env.generateSequence(1, NUM_VERTICES)
								.flatMap(new FlatMapFunction<Long, Tuple3<Long, Long, Double>>() {
									public void flatMap(Long value, Collector<Tuple3<Long, Long, Double>> out) {
										int numOutEdges = (int) (Math.random() * (NUM_VERTICES / 2));
										for (int i = 0; i < numOutEdges; i++) {
											long target = (long) (Math.random() * NUM_VERTICES) + 1;
											out.collect(new Tuple3<Long, Long, Double>(value, target, 1.0/numOutEdges));
										}
									}
								});
		
		// ---------- start of the algorithm ---------------
		
		// count the number of vertices
		DataSet<Long> count = vertices
			.map(new MapFunction<Long, Long>() {
				public Long map(Long value) {
					return 1L;
				}
			})
			.reduce(new ReduceFunction<Long>() {
				public Long reduce(Long value1, Long value2) {
					return value1 + value2;
				}
			});
		
		// enumerate some sample edges and assign an initial uniform probability (rank)
		DataSet<Tuple2<Long, Double>> intialRanks = vertices
			.map(new MapFunction<Long, Tuple2<Long, Double>>() {
				
				private long numVertices;
				
				@Override
				public void open(Configuration parameters) {
					numVertices = getRuntimeContext().<Long>getBroadcastVariable("count").iterator().next();
				}
				
				public Tuple2<Long, Double> map(Long value) {
					return new Tuple2<Long, Double>(value, 1.0/numVertices);
				}
			}).withBroadcastSet(count, "count");
		

		VertexCentricIteration<Long, Double, Double, Double> iteration = VertexCentricIteration.withValuedEdges(edgesWithProbability,
				new VertexRankUpdater(BETA), new RankMessenger(), 20);
		iteration.addBroadcastSetForUpdateFunction("count", count);
		
		
		DataSet<Tuple2<Long, Double>> result = intialRanks.runOperation(iteration);
		
		result.print();
		env.execute("Spargel PageRank");
	}
	
	/**
	 * Function that updates the rank of a vertex by summing up the partial ranks from all incoming messages
	 * and then applying the dampening formula.
	 */
	public static final class VertexRankUpdater extends VertexUpdateFunction<Long, Double, Double> {
		
		private final double beta;
		private long numVertices;
		
		public VertexRankUpdater(double beta) {
			this.beta = beta;
		}
		
		@Override
		public void preSuperstep() {
			numVertices = this.<Long>getBroadcastSet("count").iterator().next();
		}

		@Override
		public void updateVertex(Long vertexKey, Double vertexValue, MessageIterator<Double> inMessages) {
			double rankSum = 0.0;
			for (double msg : inMessages) {
				rankSum += msg;
			}
			
			// apply the dampening factor / random jump
			double newRank = (beta * rankSum) + (1-BETA)/numVertices;
			setNewVertexValue(newRank);
		}
	}
	
	/**
	 * Distributes the rank of a vertex among all target vertices according to the transition probability,
	 * which is associated with an edge as the edge value.
	 */
	public static final class RankMessenger extends MessagingFunction<Long, Double, Double, Double> {
		
		@Override
		public void sendMessages(Long vertexId, Double newRank) {
			for (OutgoingEdge<Long, Double> edge : getOutgoingEdges()) {
				sendMessageTo(edge.target(), newRank * edge.edgeValue());
			}
		}
	}
}
