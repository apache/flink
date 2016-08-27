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

package org.apache.flink.graph.examples;

import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.util.Preconditions;

/**
 * This is an implementation of HITS algorithm, using a scatter-gather iteration.
 * The user can define the maximum number of iterations. HITS algorithm is determined by two parameters,
 * hubs and authorities. A good hub represents a page that points to many other pages, and a good authority
 * represented a page that is linked by many different hubs.
 * Each vertex has a value of Tuple2 type, the first field is hub score and the second field is authority score.
 * The implementation sets same score to every vertex and adds the reverse edge to every edge at the beginning. 
 * <p>
 *
 * @see <a href="https://en.wikipedia.org/wiki/HITS_algorithm">HITS Algorithm</a>
 */
public class HITSAlgorithm<K, VV, EV> implements GraphAlgorithm<K, VV, EV, DataSet<Vertex<K, Tuple2<DoubleValue, DoubleValue>>>> {

	private final static int MAXIMUMITERATION = (Integer.MAX_VALUE - 1) / 2;
	private final static double MINIMUMTHRESHOLD = Double.MIN_VALUE;

	private int maxIterations;
	private double convergeThreshold;

	/**
	 * Create an instance of HITS algorithm.
	 *
	 * @param maxIterations the maximum number of iterations
	 */
	public HITSAlgorithm(int maxIterations) {
		this(maxIterations, MINIMUMTHRESHOLD);
	}

	/**
	 * Create an instance of HITS algorithm.
	 *
	 * @param convergeThreshold convergence threshold for sum of scores to control whether the iteration should be stopped
	 */
	public HITSAlgorithm(double convergeThreshold) {
		this(MAXIMUMITERATION, convergeThreshold);
	}

	/**
	 * Creates an instance of HITS algorithm.
	 *
	 * @param maxIterations     the maximum number of iterations
	 * @param convergeThreshold convergence threshold for sum of scores to control whether the iteration should be stopped
	 */
	public HITSAlgorithm(int maxIterations, double convergeThreshold) {
		Preconditions.checkArgument(maxIterations > 0, "Number of iterations must be greater than zero.");
		Preconditions.checkArgument(convergeThreshold > 0.0, "Convergence threshold must be greater than zero.");
		this.maxIterations = maxIterations * 2 + 1;
		this.convergeThreshold = convergeThreshold;
	}

	@Override
	public DataSet<Vertex<K, Tuple2<DoubleValue, DoubleValue>>> run(Graph<K, VV, EV> graph) throws Exception {
		Graph<K, Tuple2<DoubleValue, DoubleValue>, Boolean> newGraph = graph
				.mapEdges(new AuthorityEdgeMapper<K, EV>())
				.union(graph.reverse().mapEdges(new HubEdgeMapper<K, EV>()))
				.mapVertices(new VertexInitMapper<K, VV>());

		ScatterGatherConfiguration parameter = new ScatterGatherConfiguration();
		parameter.setDirection(EdgeDirection.OUT);
		parameter.setOptNumVertices(true);
		parameter.registerAggregator("updatedValueSum", new DoubleSumAggregator());
		parameter.registerAggregator("authorityValueSum", new DoubleSumAggregator());
		parameter.registerAggregator("diffValueSum", new DoubleSumAggregator());

		return newGraph
				.runScatterGatherIteration(new MessageUpdate<K>(maxIterations),
						new VertexUpdate<K>(maxIterations, convergeThreshold), maxIterations, parameter)
				.getVertices();
	}

	/**
	 * Distributes the value of a vertex among all neighbor vertices and sum all the
	 * value in every superstep.
	 */
	private static final class MessageUpdate<K> extends ScatterFunction<K, Tuple2<DoubleValue, DoubleValue>, Double, Boolean> {
		private int maxIteration;

		public MessageUpdate(int maxIteration) {
			this.maxIteration = maxIteration;
		}

		@Override
		public void sendMessages(Vertex<K, Tuple2<DoubleValue, DoubleValue>> vertex) {
			// in the first iteration, no aggregation to call, init sum with value of vertex
			double iterationValueSum = 1.0;

			if (getSuperstepNumber() > 1) {
				iterationValueSum = Math.sqrt(((DoubleValue) getPreviousIterationAggregate("updatedValueSum")).getValue());
			}
			for (Edge<K, Boolean> edge : getEdges()) {
				if (getSuperstepNumber() != maxIteration) {
					if (getSuperstepNumber() % 2 == 1) {
						if (edge.getValue()) {
							sendMessageTo(edge.getTarget(), vertex.getValue().f0.getValue() / iterationValueSum);
						}
					} else {
						if (!edge.getValue()) {
							sendMessageTo(edge.getTarget(), vertex.getValue().f1.getValue() / iterationValueSum);
						}
					}
				} else {
					if (!edge.getValue()) {
						sendMessageTo(edge.getTarget(), iterationValueSum);
					}
				}
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by summing up the partial
	 * values from all messages and normalize the value.
	 */
	private static final class VertexUpdate<K> extends GatherFunction<K, Tuple2<DoubleValue, DoubleValue>, Double> {
		private int maxIteration;
		private double convergeThreshold;
		private DoubleSumAggregator updatedValueSumAggregator;
		private DoubleSumAggregator authoritySumAggregator;
		private DoubleSumAggregator diffSumAggregator;

		public VertexUpdate(int maxIteration, double convergeThreshold) {
			this.maxIteration = maxIteration;
			this.convergeThreshold = convergeThreshold;
		}

		@Override
		public void preSuperstep() {
			updatedValueSumAggregator = getIterationAggregator("updatedValueSum");
			authoritySumAggregator = getIterationAggregator("authorityValueSum");
			diffSumAggregator = getIterationAggregator("diffValueSum");
		}

		@Override
		public void updateVertex(Vertex<K, Tuple2<DoubleValue, DoubleValue>> vertex, MessageIterator<Double> inMessages) {
			double updateValue = 0;

			for (double element : inMessages) {
				if (getSuperstepNumber() == maxIteration) {
					updateValue = element;
					break;
				}
				updateValue += element;
			}
			updatedValueSumAggregator.aggregate(Math.pow(updateValue, 2));

			// in the first iteration, no aggregation to call, init sum with value of vertex
			double iterationValueSum = 1.0;

			DoubleValue newHubValue = vertex.getValue().f0;
			DoubleValue newAuthorityValue = vertex.getValue().f1;

			if (getSuperstepNumber() > 1) {
				iterationValueSum = Math.sqrt(((DoubleValue) getPreviousIterationAggregate("updatedValueSum")).getValue());
			}
			if (getSuperstepNumber() < maxIteration) {
				if (getSuperstepNumber() % 2 == 1) {

					//in the first iteration, the diff is the authority value of each vertex
					double previousAuthAverage = 1.0;
					double diffValueSum = 1.0 * getNumberOfVertices();
					if (getSuperstepNumber() > 1) {
						previousAuthAverage = ((DoubleValue) getPreviousIterationAggregate("authorityValueSum")).getValue() / getNumberOfVertices();
						diffValueSum = ((DoubleValue) getPreviousIterationAggregate("diffValueSum")).getValue();
					}
					authoritySumAggregator.aggregate(previousAuthAverage);

					if (diffValueSum > convergeThreshold) {
						newHubValue.setValue(newHubValue.getValue() / iterationValueSum);
						newAuthorityValue.setValue(updateValue);
					} else {

						//scores are converged and stop iteration
						maxIteration = getSuperstepNumber();
						newHubValue.setValue(newHubValue.getValue() / iterationValueSum);
					}
				} else {
					newHubValue.setValue(updateValue);
					newAuthorityValue.setValue(newAuthorityValue.getValue() / iterationValueSum);
					authoritySumAggregator.aggregate(newAuthorityValue.getValue());
					double previousAuthAverage = ((DoubleValue) getPreviousIterationAggregate("authorityValueSum")).getValue() / getNumberOfVertices();

					// count the diff value of sum of authority scores
					diffSumAggregator.aggregate(previousAuthAverage - newAuthorityValue.getValue());
				}
				setNewVertexValue(new Tuple2<>(newHubValue, newAuthorityValue));
			} else if (getSuperstepNumber() == maxIteration) {

				//final iteration to normalize hub score
				newHubValue.setValue(newHubValue.getValue() / iterationValueSum);
				setNewVertexValue(new Tuple2<>(newHubValue, newAuthorityValue));
			}
		}
	}

	private static class VertexInitMapper<K, VV> implements MapFunction<Vertex<K, VV>, Tuple2<DoubleValue, DoubleValue>> {
		private Tuple2<DoubleValue, DoubleValue> initVertexValue = new Tuple2<>(new DoubleValue(1.0), new DoubleValue(1.0));

		public Tuple2<DoubleValue, DoubleValue> map(Vertex<K, VV> value) {
			//init hub and authority value of each vertex
			return initVertexValue;
		}
	}

	private static class AuthorityEdgeMapper<K, EV> implements MapFunction<Edge<K, EV>, Boolean> {
		public Boolean map(Edge<K, EV> edge) {
			// mark edge as true for authority updating
			return true;
		}
	}

	private static class HubEdgeMapper<K, EV> implements MapFunction<Edge<K, EV>, Boolean> {
		public Boolean map(Edge<K, EV> edge) {
			// mark edge as false for hub updating
			return false;
		}
	}
}

