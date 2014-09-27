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


package org.apache.flink.test.recordJobs.graph.pageRankUtil;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.java.record.functions.CoGroupFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.recordJobs.util.ConfigUtils;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

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
		
		aggregator = getIterationRuntimeContext().getIterationAggregator(AGGREGATOR_NAME);
		
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
