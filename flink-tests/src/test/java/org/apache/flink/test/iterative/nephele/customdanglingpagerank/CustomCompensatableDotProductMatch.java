/**
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

import java.util.Random;
import java.util.Set;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.iterative.nephele.ConfigUtils;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithAdjacencyList;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithRank;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import org.apache.flink.util.Collector;

public class CustomCompensatableDotProductMatch extends AbstractRichFunction implements
		FlatJoinFunction<VertexWithRankAndDangling, VertexWithAdjacencyList, VertexWithRank>
{
	private static final long serialVersionUID = 1L;
	
	
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
	public void join(VertexWithRankAndDangling pageWithRank, VertexWithAdjacencyList adjacencyList, Collector<VertexWithRank> collector)
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
