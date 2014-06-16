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

package eu.stratosphere.test.iterative.nephele.customdanglingpagerank;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithRank;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import eu.stratosphere.test.iterative.nephele.danglingpagerank.PageRankStats;
import eu.stratosphere.test.iterative.nephele.danglingpagerank.PageRankStatsAccumulator;
import eu.stratosphere.util.Collector;

import java.util.Iterator;
import java.util.Set;

public class CustomCompensatableDotProductCoGroup extends AbstractFunction implements GenericCoGrouper<VertexWithRankAndDangling, VertexWithRank, VertexWithRankAndDangling> {

	private static final long serialVersionUID = 1L;

	public static final String ACCUMULATOR_NAME = "pagerank.accumulator";
	
	private VertexWithRankAndDangling accumulator = new VertexWithRankAndDangling();

	private PageRankStatsAccumulator aggregator;

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

		aggregator = new PageRankStatsAccumulator();
		getIterationRuntimeContext().addIterationAccumulator(ACCUMULATOR_NAME, aggregator);
		
		if (currentIteration == 1) {
			danglingRankFactor = BETA * (double) numDanglingVertices / ((double) numVertices * (double) numVertices);
		} else {
			PageRankStats previousAggregate = (PageRankStats) getIterationRuntimeContext()
					.getPreviousIterationAccumulator(ACCUMULATOR_NAME)
					.getLocalValue();
			danglingRankFactor = BETA * previousAggregate.danglingRank() / (double) numVertices;
		}
	}

	@Override
	public void coGroup(Iterator<VertexWithRankAndDangling> currentPageRankIterator, Iterator<VertexWithRank> partialRanks,
			Collector<VertexWithRankAndDangling> collector)
	{
		if (!currentPageRankIterator.hasNext()) {
			long missingVertex = partialRanks.next().getVertexID();
			throw new IllegalStateException("No current page rank for vertex [" + missingVertex + "]!");
		}

		VertexWithRankAndDangling currentPageRank = currentPageRankIterator.next();

		long edges = 0;
		double summedRank = 0;
		while (partialRanks.hasNext()) {
			summedRank += partialRanks.next().getRank();
			edges++;
		}

		double rank = BETA * summedRank + dampingFactor + danglingRankFactor;

		double currentRank = currentPageRank.getRank();
		boolean isDangling = currentPageRank.isDangling();

		double danglingRankToAggregate = isDangling ? rank : 0;
		long danglingVerticesToAggregate = isDangling ? 1 : 0;

		double diff = Math.abs(currentRank - rank);

		aggregator.add(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges, summedRank, 0);

		accumulator.setVertexID(currentPageRank.getVertexID());
		accumulator.setRank(rank);
		accumulator.setDangling(isDangling);

		collector.collect(accumulator);
	}

	@Override
	public void close() throws Exception {
		if (currentIteration == failingIteration && failingWorkers.contains(workerIndex)) {
			aggregator.resetLocal();
		}
	}
	
}
