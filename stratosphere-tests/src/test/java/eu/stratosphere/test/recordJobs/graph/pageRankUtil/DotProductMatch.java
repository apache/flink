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

package eu.stratosphere.test.recordJobs.graph.pageRankUtil;

import java.io.Serializable;

import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * In schema is_
 * INPUT = (pageId, rank, dangling), (pageId, neighbors-list).
 * OUTPUT = (targetPageId, partialRank)
 */
public class DotProductMatch extends JoinFunction implements Serializable {
	private static final long serialVersionUID = 1L;

	private Record record = new Record();
	private LongValue vertexID = new LongValue();
	private DoubleValue partialRank = new DoubleValue();
	private DoubleValue rank = new DoubleValue();

	private LongArrayView adjacentNeighbors = new LongArrayView();

	@Override
	public void join(Record pageWithRank, Record adjacencyList, Collector<Record> collector) throws Exception {

		rank = pageWithRank.getField(1, rank);
		adjacentNeighbors = adjacencyList.getField(1, adjacentNeighbors);
		int numNeighbors = adjacentNeighbors.size();

		double rankToDistribute = rank.getValue() / (double) numNeighbors;

		partialRank.setValue(rankToDistribute);
		record.setField(1, partialRank);

		for (int n = 0; n < numNeighbors; n++) {
			vertexID.setValue(adjacentNeighbors.getQuick(n));
			record.setField(0, vertexID);
			collector.collect(record);
		}
	}
}
