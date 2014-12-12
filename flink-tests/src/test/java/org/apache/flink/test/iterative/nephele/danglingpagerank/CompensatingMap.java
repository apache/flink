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


package org.apache.flink.test.iterative.nephele.danglingpagerank;

import java.util.Set;

import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.iterative.nephele.ConfigUtils;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class CompensatingMap extends MapFunction {

	private static final long serialVersionUID = 1L;

	private int workerIndex;

	private int currentIteration;

	private long numVertices;

	private int failingIteration;

	private Set<Integer> failingWorkers;

	private double uniformRank;

	private double rescaleFactor;

	private DoubleValue rank = new DoubleValue();

	@Override
	public void open(Configuration parameters) {

		workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);

		if (currentIteration > 1) {
			PageRankStats stats = (PageRankStats) getIterationRuntimeContext().getPreviousIterationAggregate(
				CompensatableDotProductCoGroup.AGGREGATOR_NAME);

			uniformRank = 1d / (double) numVertices;
			double lostMassFactor = (numVertices - stats.numVertices()) / (double) numVertices;
			rescaleFactor = (1 - lostMassFactor) / stats.rank();
		}
	}

	@Override
	public void map(Record pageWithRank, Collector<Record> out) {

		if (currentIteration == failingIteration + 1) {

			rank = pageWithRank.getField(1, rank);

			if (failingWorkers.contains(workerIndex)) {
				rank.setValue(uniformRank);
			} else {
				rank.setValue(rank.getValue() * rescaleFactor);
			}
			pageWithRank.setField(1, rank);
		}
		
		out.collect(pageWithRank);
	}
}
