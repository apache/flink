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

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

import java.util.Set;

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
		
		getRuntimeContext().addAccumulator(CompensatableDotProductCoGroup.ACCUMULATOR_NAME, new PageRankStatsAccumulator());

		if (currentIteration > 1) {	
			PageRankStats stats = (PageRankStats) getIterationRuntimeContext()
					.getPreviousIterationAccumulator(CompensatableDotProductCoGroup.ACCUMULATOR_NAME)
					.getLocalValue();

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
