/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.example.utils.ConvergenceDetectionAggregator;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Given an undirected, weighed graph, the minumum spanning tree is a subgraph
 * that connects all the vertices together forming paths of minimum weight.
 *
 * <p>
 *     The basic algorithm works as follows:
 *     <ul>
 *         <li> select the minimum weight edge for each vertex
 *         <li> add the selected edges to the minimum spanning tree
 *         <li> collapse the vertices with the same root into a single vertex
 *         <li> reiterate until there is only one vertex left
 *     </ul>
 * </p>
 *
 * <p>
 *    Input files are plain text files and must be formatted as follows:
 *    <ul>
 *        <li> Vertices are represented by their vertexIds and are separated by newlines
 *        For example: <code>1\n2\n3\n</code> defines a data set of three vertices
 *        <li> Edges are represented by triples of srcVertexId, srcEdgeId, weight which are
 *        separated by commas. Edges themselves are separated by newlines.
 *        For example: <code>1,2,13.0\n1,3,6.0\n</code> defines two edges 1-2 and 1-3 having
 *        weights 13 and 6 respectively.
 *    </ul>
 * </p>
 *
 * Usage <code>MinSpanningTree &lt;vertex path&gt; &lt;edge path&gt; &lt;result path&gt;
 * &lt;number of iterations&gt;</code><br>
 *     If no parameters are provided, the program is ran with default data from
 *     {@link org.apache.flink.graph.example.utils.MinSpanningTreeData}
 */
@SuppressWarnings("serial")
public class MinSpanningTree implements GraphAlgorithm<Long, String, Double> {

	private final Integer maxIterations;

	private ConvergenceDetectionAggregator aggregator;

	private static final String AGGREGATOR_NAME = "pointerJumpingAggregator";

	public MinSpanningTree(Integer maxIterations) {
		this.maxIterations = maxIterations;
		aggregator = new ConvergenceDetectionAggregator();
	}

	@Override
	public Graph<Long, String, Double> run(Graph<Long, String, Double> input) {

		Graph<Long, String, Double> undirectedGraph = input.getUndirected();

		DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> initialSolutionSet =
				assignInitialSolutionSetRootIds(undirectedGraph);

		// open an iteration
		final IterativeDataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> iteration =
				initialSolutionSet.iterate(maxIterations)
						.registerAggregator(AGGREGATOR_NAME, aggregator);

		DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> minEdges = minEdgePicking(iteration);

		DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> minEdgesWithMinRootIds = selectMinRootId(minEdges);

		DataSet<Tuple2<Long, Long>> newRootIds = computeNewRootIds(minEdgesWithMinRootIds);

		DataSet<Tuple2<Long, Long>> allRootIds = computeNewRootIds(iteration).union(newRootIds).groupBy(0)
				.minBy(1);

		// update the solution set to contain the new found rootIds
		DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> solSetUpdatedSrcRootId =
				computeSolSetForSrc(allRootIds, iteration);
		DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> solSetUpdatedTrgRootId =
				computeSolSetForTrg(allRootIds, iteration);
		DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> solSetUpdated =
				mergeSolSets(solSetUpdatedSrcRootId, solSetUpdatedTrgRootId);

		// close the iteration
		DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> result =
				iteration.closeWith(finalSolution(minEdges, solSetUpdated), minEdges.union(iteration
						.filter(new RichFilterFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>>() {
							@Override
							public boolean filter(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6) throws Exception {

								return (getIterationRuntimeContext().getSuperstepNumber() == 1 ||
										getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME)
												.equals(new BooleanValue(true)));
							}
						})));

		DataSet<Edge<Long, Double>> resultedEdges = result.flatMap(new FlatMapFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>, Edge<Long, Double>>() {

			@Override
			public void flatMap(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6,
								Collector<Edge<Long, Double>> collector) throws Exception {

				if(tuple6.f5) {
					if(tuple6.f0 < tuple6.f1) {
						collector.collect(new Edge<Long, Double>(tuple6.f0, tuple6.f1, tuple6.f2));
					}
				}
			}
		});

		return Graph.fromDataSet(undirectedGraph.getVertices(), resultedEdges, result.getExecutionEnvironment());
	}


	/**
	 * Function that defines the initial solution set.
	 *
	 * @param undirectedGraph
	 * @return a DataSet of Tuple 6 containing: the edges along with their rootIds for the source
	 *		 and target respectively and a flag identifying whether this edge is in MST. In the beginning,
	 *		 the rootIds are equal to their corresponding vertexIds
	 */
	private DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> assignInitialSolutionSetRootIds
	(Graph<Long, String, Double> undirectedGraph) {

		return undirectedGraph.getEdges().map(new MapFunction<Edge<Long, Double>,
								Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

			@Override
			public Tuple6<Long, Long, Double, Long, Long, Boolean> map(Edge<Long, Double> edge) throws Exception {
				return new Tuple6<Long, Long, Double, Long, Long, Boolean>
						(edge.f0, edge.f1, edge.f2, edge.f0, edge.f1, false);
			}
		});
	}

	/**
	 * In parallel, each vertex picks its minimum weight edge.
	 * If convergence was detected, output all the edges inMST with rootSrc != rootTrg.
	 * Otherwise, fetch the minimum edges from the edges that were not inMST and mark them as inMST.
	 *
	 * @param solutionSet
	 * @return
	 */
	public DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> minEdgePicking(DataSet<Tuple6<Long, Long, Double,
			Long, Long, Boolean>> solutionSet) {

		return solutionSet.filter(new RichFilterFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

			@Override
			public boolean filter(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6) throws Exception {
				return tuple6.f3 != tuple6.f4;
			}
		}).groupBy(3).minBy(2, 4).filter(new RichFilterFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

			ConvergenceDetectionAggregator aggregator;
			private Boolean inMST = false;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);

				if (getIterationRuntimeContext().getSuperstepNumber() > 1) {
					if (getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME)
							.equals(new BooleanValue(false))) {
						inMST = false;
					} else {
						inMST = true;
					}
				}

				aggregator = getIterationRuntimeContext().getIterationAggregator(AGGREGATOR_NAME);
				aggregator.reset();
			}

			@Override
			public boolean filter(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6) throws Exception {
				if (tuple6.f5.equals(inMST)) {
					aggregator.aggregate(new BooleanValue(true));
				}
				return tuple6.f5.equals(inMST);
			}
		}).map(new MapFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>,
				Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

			@Override
			public Tuple6<Long, Long, Double, Long, Long, Boolean> map(
					Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6) throws Exception {

				return new Tuple6<Long, Long, Double, Long, Long, Boolean>(tuple6.f0, tuple6.f1,
						tuple6.f2, tuple6.f3, tuple6.f4, new Boolean(true));
			}
		});
	}

	/**
	 * Function that takes the intermediate result formed of minEdges inMST and selects the minimum root id(srcRootId,
	 * trgRootId). The srcRootId and trgRootId will then both be assigned that value.
	 *
	 * @param minEdges
	 * @return
	 */
	public DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> selectMinRootId (DataSet<Tuple6<Long, Long, Double,
			Long, Long, Boolean>> minEdges) {

		return minEdges.map(new MapFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>,
				Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

			@Override
			public Tuple6<Long, Long, Double, Long, Long, Boolean> map
					(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6) throws Exception {
				if(tuple6.f3.compareTo(tuple6.f4) <= 0) {
					return new Tuple6<Long, Long, Double, Long, Long, Boolean>(tuple6.f0, tuple6.f1, tuple6.f2,
							tuple6.f3, tuple6.f3, tuple6.f5);
				} else {
					return new Tuple6<Long, Long, Double, Long, Long, Boolean>(tuple6.f0, tuple6.f1, tuple6.f2,
							tuple6.f4, tuple6.f4, tuple6.f5);
				}
			}
		});
	}

	/**
	 * Function that updates the (vertexId, rootId) workset with the new rootId values
	 *
	 * @param minEdgesWithMinRootIds
	 * @return
	 */
	public DataSet<Tuple2<Long, Long>> computeNewRootIds(
			DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> minEdgesWithMinRootIds) {

		return minEdgesWithMinRootIds.flatMap(new FlatMapFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>,
				Tuple2<Long, Long>>() {

			@Override
			public void flatMap(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6,
								Collector<Tuple2<Long, Long>> collector) throws Exception {

				collector.collect(new Tuple2<Long, Long>(tuple6.f0, tuple6.f3));
				collector.collect(new Tuple2<Long, Long>(tuple6.f1, tuple6.f4));
			}
		}).distinct()
				// for a vertex Id, you may collect more than one rootId; keep the minimum
				.groupBy(0).minBy(1);
	}

	/**
	 * Function that updates the srcRootIds in the solution set with the new found values.
	 *
	 * @param newRootIds
	 * @param solutionSet
	 * @return
	 */
	public DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> computeSolSetForSrc(
			DataSet<Tuple2<Long, Long>> newRootIds, DataSet<Tuple6<Long, Long, Double,
			Long, Long, Boolean>> solutionSet) {

		return newRootIds.join(solutionSet).where(0).equalTo(0)
				.with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple6<Long, Long, Double, Long, Long, Boolean>,
										Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

					@Override
					public void join(Tuple2<Long, Long> tuple2,
									Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6,
									Collector<Tuple6<Long, Long, Double, Long, Long, Boolean>> collector) throws Exception {

						collector.collect(new Tuple6<Long, Long, Double, Long, Long, Boolean>(tuple6.f0, tuple6.f1,
								tuple6.f2, tuple2.f1, tuple6.f4, tuple6.f5));
					}
				});
	}

	/**
	 * Function that updates the trgRootIds in the solution set with the new found values.
	 *
	 * @param newRootIds
	 * @param solutionSet
	 * @return
	 */
	public DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> computeSolSetForTrg(
			DataSet<Tuple2<Long, Long>> newRootIds, DataSet<Tuple6<Long, Long, Double,
			Long, Long, Boolean>> solutionSet) {

		return newRootIds.join(solutionSet).where(0).equalTo(1)
				.with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple6<Long, Long, Double, Long, Long, Boolean>,
						Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

					@Override
					public void join(Tuple2<Long, Long> tuple2,
									Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6,
									Collector<Tuple6<Long, Long, Double, Long, Long, Boolean>> collector) throws Exception {

						collector.collect(new Tuple6<Long, Long, Double, Long, Long, Boolean>(tuple6.f0, tuple6.f1,
								tuple6.f2, tuple6.f3, tuple2.f1, tuple6.f5));
					}
				});
	}

	/**
	 * Function that merges the two solution sets received as parameters
	 *
	 * @param solutionSetSrc
	 * @param solutionSetTrg
	 * @return
	 */
	public DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> mergeSolSets(DataSet<Tuple6<Long, Long, Double,
			Long, Long, Boolean>> solutionSetSrc, DataSet<Tuple6<Long, Long, Double,
			Long, Long, Boolean>> solutionSetTrg) {

		return solutionSetSrc.join(solutionSetTrg).where(0,1,2).equalTo(0,1,2)
				.with(new FlatJoinFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>,
						Tuple6<Long, Long, Double, Long, Long, Boolean>,
						Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

					@Override
					public void join(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6Src,
									Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6Trg,
									Collector<Tuple6<Long, Long, Double, Long, Long, Boolean>> collector) throws Exception {

						collector.collect(new Tuple6<Long, Long, Double, Long, Long, Boolean>(tuple6Src.f0,
								tuple6Src.f1, tuple6Src.f2, tuple6Src.f3, tuple6Trg.f4, tuple6Src.f5));
					}
				});
	}

	/**
	 * Function that takes the src, trg and inMST from the minEdges, adds the opposite edges(trg, src, inMST)
	 * and coGroups the result with the previously computed mergedSolutionSets in order to produce
	 * the final solution.
	 *
	 * @param minEdges
	 * @param mergedSolSets
	 * @return
	 */
	DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> finalSolution(
			DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> minEdges,
			DataSet<Tuple6<Long, Long, Double, Long, Long, Boolean>> mergedSolSets) {

		DataSet<Tuple3<Long, Long, Boolean>> intermediateWithOppositeEdges = minEdges
				.flatMap(new FlatMapFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>,
						Tuple3<Long, Long, Boolean>>() {

					@Override
					public void flatMap(Tuple6<Long, Long, Double, Long, Long, Boolean> tuple6,
										Collector<Tuple3<Long, Long, Boolean>> collector) throws Exception {

						collector.collect(new Tuple3<Long, Long, Boolean>(tuple6.f0, tuple6.f1, tuple6.f5));
						collector.collect(new Tuple3<Long, Long, Boolean>(tuple6.f1, tuple6.f0, tuple6.f5));
					}
				});

		return mergedSolSets.coGroup(intermediateWithOppositeEdges).where(0,1).equalTo(0,1)
				.with(new CoGroupFunction<Tuple6<Long, Long, Double, Long, Long, Boolean>, Tuple3<Long, Long, Boolean>,
														Tuple6<Long, Long, Double, Long, Long, Boolean>>() {

					@Override
					public void coGroup(Iterable<Tuple6<Long, Long, Double, Long, Long, Boolean>> iterableSolSets,
										Iterable<Tuple3<Long, Long, Boolean>> iterableIntermWithOppositeEdges,
										Collector<Tuple6<Long, Long, Double, Long, Long, Boolean>> collector) throws Exception {

						Iterator<Tuple6<Long, Long, Double, Long, Long, Boolean>> iteratorSolSets =
								iterableSolSets.iterator();
						Iterator<Tuple3<Long, Long, Boolean>> iteratorIntermediateWithOppositeEdges =
								iterableIntermWithOppositeEdges.iterator();

						if (iteratorSolSets.hasNext()) {

							Tuple6<Long, Long, Double, Long, Long, Boolean> iteratorSolSetsNext =
									iteratorSolSets.next();

							if (!iteratorIntermediateWithOppositeEdges.hasNext()) {

								collector.collect(iteratorSolSetsNext);
							} else {

								collector.collect(new Tuple6<Long, Long, Double, Long, Long, Boolean>(
										iteratorSolSetsNext.f0, iteratorSolSetsNext.f1, iteratorSolSetsNext.f2,
										iteratorSolSetsNext.f3, iteratorSolSetsNext.f4,
										iteratorIntermediateWithOppositeEdges.next().f2
								));
							}

						}
					}
				});
	}
}
