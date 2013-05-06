/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.pagerank;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

public class DotProductMatch extends MatchStub {

	private PactRecord record = new PactRecord();
	private PactLong vertexID = new PactLong();
	private PactDouble partialRank = new PactDouble();
	private PactDouble rank = new PactDouble();

	private LongArrayView adjacentNeighbors = new LongArrayView();

	@Override
	public void match(PactRecord pageWithRank, PactRecord adjacencyList, Collector<PactRecord> collector) throws Exception {

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
