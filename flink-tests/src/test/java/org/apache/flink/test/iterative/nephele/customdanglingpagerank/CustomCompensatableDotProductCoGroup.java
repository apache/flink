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

package org.apache.flink.test.iterative.nephele.customdanglingpagerank;

import java.util.Iterator;
import java.util.Set;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.iterative.nephele.ConfigUtils;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithRank;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import org.apache.flink.test.iterative.nephele.danglingpagerank.PageRankStats;
import org.apache.flink.test.iterative.nephele.danglingpagerank.PageRankStatsAggregator;
import org.apache.flink.util.Collector;

public class CustomCompensatableDotProductCoGroup extends AbstractRichFunction implements CoGroupFunction<VertexWithRankAndDangling, VertexWithRank, VertexWithRankAndDangling> {

	private static final long serialVersionUID = 1L;

	public static final String AGGREGATOR_NAME = "pagerank.aggregator";
	
	private VertexWithRankAndDangling accumulator = new VertexWithRankAndDangling();

	private PageRankStatsAggregator aggregator;

	private long numVertices;

	private long numDanglingVertices;

	private double dampingFactor;

	private double danglingRankFactor;

	private static final double BETA = 0.85;
	
	private int workerIndex;
	
	private int currentIteration;
	
	private int failingIteration;
	
	private Set<Integer> failingWorkers;

	@Override
	public void open(Configuration parameters) throws Exception {
		workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		
		failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		numDanglingVertices = ConfigUtils.asLong("pageRank.numDanglingVertices", parameters);
		
		dampingFactor = (1d - BETA) / (double) numVertices;

		aggregator = getIterationRuntimeContext().getIterationAggregator(AGGREGATOR_NAME);
		
		if (currentIteration == 1) {
			danglingRankFactor = BETA * (double) numDanglingVertices / ((double) numVertices * (double) numVertices);
		} else {
			PageRankStats previousAggregate = getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME);
			danglingRankFactor = BETA * previousAggregate.danglingRank() / (double) numVertices;
		}
	}

	@Override
	public void coGroup(Iterable<VertexWithRankAndDangling> currentPageRankIterable, Iterable<VertexWithRank> partialRanks,
			Collector<VertexWithRankAndDangling> collector)
	{
		final Iterator<VertexWithRankAndDangling> currentPageRankIterator = currentPageRankIterable.iterator();
		
		if (!currentPageRankIterator.hasNext()) {
			long missingVertex = partialRanks.iterator().next().getVertexID();
			throw new IllegalStateException("No current page rank for vertex [" + missingVertex + "]!");
		}

		VertexWithRankAndDangling currentPageRank = currentPageRankIterator.next();

		long edges = 0;
		double summedRank = 0;
		for (VertexWithRank pr :partialRanks) {
			summedRank += pr.getRank();
			edges++;
		}

		double rank = BETA * summedRank + dampingFactor + danglingRankFactor;

		double currentRank = currentPageRank.getRank();
		boolean isDangling = currentPageRank.isDangling();

		double danglingRankToAggregate = isDangling ? rank : 0;
		long danglingVerticesToAggregate = isDangling ? 1 : 0;

		double diff = Math.abs(currentRank - rank);

		aggregator.aggregate(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges, summedRank, 0);

		accumulator.setVertexID(currentPageRank.getVertexID());
		accumulator.setRank(rank);
		accumulator.setDangling(isDangling);

		collector.collect(accumulator);
	}

	@Override
	public void close() throws Exception {
		if (currentIteration == failingIteration && failingWorkers.contains(workerIndex)) {
			aggregator.reset();
		}
	}
	
}
