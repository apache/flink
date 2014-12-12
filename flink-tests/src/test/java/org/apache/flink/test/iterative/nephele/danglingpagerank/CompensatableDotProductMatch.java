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

import java.util.Random;
import java.util.Set;

import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.iterative.nephele.ConfigUtils;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class CompensatableDotProductMatch extends JoinFunction {

	private static final long serialVersionUID = 1L;

	private Record record;

	private LongValue vertexID;

	private DoubleValue partialRank;

	private DoubleValue rank = new DoubleValue();

	private LongArrayView adjacentNeighbors = new LongArrayView();

	private int workerIndex;

	private int currentIteration;

	private int failingIteration;

	private Set<Integer> failingWorkers;

	private double messageLoss;

	private Random random;

	@Override
	public void open(Configuration parameters) {
		record = new Record();
		vertexID = new LongValue();
		partialRank = new DoubleValue();

		workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		messageLoss = ConfigUtils.asDouble("compensation.messageLoss", parameters);

		random = new Random();
	}

	@Override
	public void join(Record pageWithRank, Record adjacencyList, Collector<Record> collector) {

		rank = pageWithRank.getField(1, rank);
		adjacentNeighbors = adjacencyList.getField(1, adjacentNeighbors);
		int numNeighbors = adjacentNeighbors.size();

		double rankToDistribute = rank.getValue() / (double) numNeighbors;

		partialRank.setValue(rankToDistribute);
		record.setField(1, partialRank);

		boolean isFailure = currentIteration == failingIteration && failingWorkers.contains(workerIndex);

		for (int n = 0; n < numNeighbors; n++) {
			vertexID.setValue(adjacentNeighbors.getQuick(n));
			record.setField(0, vertexID);

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
