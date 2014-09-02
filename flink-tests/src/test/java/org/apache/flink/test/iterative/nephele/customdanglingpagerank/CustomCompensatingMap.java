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

import java.util.Set;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GenericCollectorMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.iterative.nephele.ConfigUtils;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import org.apache.flink.test.iterative.nephele.danglingpagerank.PageRankStats;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class CustomCompensatingMap extends AbstractRichFunction implements GenericCollectorMap<VertexWithRankAndDangling, VertexWithRankAndDangling> {
	
	private static final long serialVersionUID = 1L;
	
	
	private boolean isFailureIteration;

	private boolean isFailingWorker;

	private double uniformRank;

	private double rescaleFactor;

	@Override
	public void open(Configuration parameters) throws Exception {
		int currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		int failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		isFailureIteration = currentIteration == failingIteration + 1;
		
		int workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		Set<Integer> failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		isFailingWorker = failingWorkers.contains(workerIndex);
		
		long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);

		if (currentIteration > 1) {
			
			PageRankStats stats = (PageRankStats) getIterationRuntimeContext().getPreviousIterationAggregate(CustomCompensatableDotProductCoGroup.AGGREGATOR_NAME);

			uniformRank = 1d / (double) numVertices;
			double lostMassFactor = (numVertices - stats.numVertices()) / (double) numVertices;
			rescaleFactor = (1 - lostMassFactor) / stats.rank();
		}
	}

	@Override
	public void map(VertexWithRankAndDangling pageWithRank, Collector<VertexWithRankAndDangling> out) throws Exception {

		if (isFailureIteration) {
			double rank = pageWithRank.getRank();

			if (isFailingWorker) {
				pageWithRank.setRank(uniformRank);
			} else {
				pageWithRank.setRank(rank * rescaleFactor);
			}
		}
		out.collect(pageWithRank);
	}
}
