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

package eu.stratosphere.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

import java.util.Iterator;
import java.util.Set;

public class CompensatableDotProductCoGroup extends CoGroupFunction {
	
	public static final String AGGREGATOR_NAME = "pagerank.aggregator";

	private Record accumulator = new Record();

	private int workerIndex;

	private int currentIteration;

	private int failingIteration;

	private Set<Integer> failingWorkers;

	private PageRankStatsAggregator aggregator;

	private long numVertices;

	private long numDanglingVertices;

	private double dampingFactor;

	private double danglingRankFactor;

	private static final double BETA = 0.85;

	private final DoubleValue newRank = new DoubleValue();

	private BooleanValue isDangling = new BooleanValue();

	private LongValue vertexID = new LongValue();

	private DoubleValue doubleInstance = new DoubleValue();

	@Override
	public void open(Configuration parameters) throws Exception {
		workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		
		failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		numDanglingVertices = ConfigUtils.asLong("pageRank.numDanglingVertices", parameters);

		dampingFactor = (1d - BETA) / (double) numVertices;
		
		aggregator = (PageRankStatsAggregator) getIterationRuntimeContext().<PageRankStats>getIterationAggregator(AGGREGATOR_NAME);
		
		if (currentIteration == 1) {
			danglingRankFactor = BETA * (double) numDanglingVertices / ((double) numVertices * (double) numVertices);
		} else {
			PageRankStats previousAggregate = getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME);
			danglingRankFactor = BETA * previousAggregate.danglingRank() / (double) numVertices;
		}
	}

	@Override
	public void coGroup(Iterator<Record> currentPageRankIterator, Iterator<Record> partialRanks,
			Collector<Record> collector) {

		if (!currentPageRankIterator.hasNext()) {
			long missingVertex = partialRanks.next().getField(0, LongValue.class).getValue();
			throw new IllegalStateException("No current page rank for vertex [" + missingVertex + "]!");
		}

		Record currentPageRank = currentPageRankIterator.next();

		long edges = 0;
		double summedRank = 0;
		while (partialRanks.hasNext()) {
			summedRank += partialRanks.next().getField(1, doubleInstance).getValue();
			edges++;
		}

		double rank = BETA * summedRank + dampingFactor + danglingRankFactor;

		double currentRank = currentPageRank.getField(1, doubleInstance).getValue();
		isDangling = currentPageRank.getField(2, isDangling);

		double danglingRankToAggregate = isDangling.get() ? rank : 0;
		long danglingVerticesToAggregate = isDangling.get() ? 1 : 0;

		double diff = Math.abs(currentRank - rank);

		aggregator.aggregate(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges, summedRank, 0);

		newRank.setValue(rank);

		accumulator.setField(0, currentPageRank.getField(0, vertexID));
		accumulator.setField(1, newRank);
		accumulator.setField(2, isDangling);

		collector.collect(accumulator);
	}

	@Override
	public void close() throws Exception {
		if (currentIteration == failingIteration && failingWorkers.contains(workerIndex)) {
			aggregator.reset();
		}
	}
}
