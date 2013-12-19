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
import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithAdjacencyList;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithRank;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import eu.stratosphere.util.Collector;

import java.util.Random;
import java.util.Set;

public class CustomCompensatableDotProductMatch extends AbstractFunction implements
		GenericJoiner<VertexWithRankAndDangling, VertexWithAdjacencyList, VertexWithRank> {

	private VertexWithRank record = new VertexWithRank();

	private Random random = new Random();
	
	private double messageLoss;
	
	private boolean isFailure;

	@Override
	public void open(Configuration parameters) throws Exception {
		int workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		int currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		int failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		Set<Integer> failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		isFailure = currentIteration == failingIteration && failingWorkers.contains(workerIndex);
		messageLoss = ConfigUtils.asDouble("compensation.messageLoss", parameters);
	}

	@Override
	public void match(VertexWithRankAndDangling pageWithRank, VertexWithAdjacencyList adjacencyList, Collector<VertexWithRank> collector)
	throws Exception
	{
		double rank = pageWithRank.getRank();
		long[] adjacentNeighbors = adjacencyList.getTargets();
		int numNeighbors = adjacencyList.getNumTargets();

		double rankToDistribute = rank / (double) numNeighbors;
		record.setRank(rankToDistribute);

		for (int n = 0; n < numNeighbors; n++) {
			record.setVertexID(adjacentNeighbors[n]);
			if (isFailure) {
				if (random.nextDouble() >= messageLoss) {
					collector.collect(record);
				}
			} else {
				collector.collect(record);
			}
		}
	}
}
