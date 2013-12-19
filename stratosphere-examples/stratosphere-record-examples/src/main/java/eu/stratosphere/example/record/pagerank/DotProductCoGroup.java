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

package eu.stratosphere.example.record.pagerank;

import eu.stratosphere.api.record.functions.CoGroupFunction;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.util.ConfigUtils;
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;

/**
 * In schema is_
 * INPUT = (pageId, currentRank, dangling), (pageId, partialRank).
 * OUTPUT = (pageId, newRank, dangling)
 */
@ConstantFieldsFirst(0)
public class DotProductCoGroup extends CoGroupFunction implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public static final String NUM_VERTICES_PARAMETER = "pageRank.numVertices";
	
	public static final String NUM_DANGLING_VERTICES_PARAMETER = "pageRank.numDanglingVertices";
	
	public static final String AGGREGATOR_NAME = "pagerank.aggregator";
	
	private static final double BETA = 0.85;

	
	private PageRankStatsAggregator aggregator;

	private long numVertices;

	private long numDanglingVertices;

	private double dampingFactor;

	private double danglingRankFactor;
	
	
	private Record accumulator = new Record();

	private final DoubleValue newRank = new DoubleValue();

	private BooleanValue isDangling = new BooleanValue();

	private LongValue vertexID = new LongValue();

	private DoubleValue doubleInstance = new DoubleValue();

	@Override
	public void open(Configuration parameters) throws Exception {
		int currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		
		numVertices = ConfigUtils.asLong(NUM_VERTICES_PARAMETER, parameters);
		numDanglingVertices = ConfigUtils.asLong(NUM_DANGLING_VERTICES_PARAMETER, parameters);

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
			Collector<Record> collector)
	{
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
		
		// maintain statistics to compensate for probability loss on dangling nodes
		double danglingRankToAggregate = isDangling.get() ? rank : 0;
		long danglingVerticesToAggregate = isDangling.get() ? 1 : 0;
		double diff = Math.abs(currentRank - rank);
		aggregator.aggregate(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges);
		
		// return the new record
		newRank.setValue(rank);
		accumulator.setField(0, currentPageRank.getField(0, vertexID));
		accumulator.setField(1, newRank);
		accumulator.setField(2, isDangling);
		collector.collect(accumulator);
	}
}
