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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.SimpleAccumulator;

public class PageRankStatsAccumulator implements SimpleAccumulator<PageRankStats> {

	private static final long serialVersionUID = 1L;

	private double diff = 0;

	private double rank = 0;

	private double danglingRank = 0;

	private long numDanglingVertices = 0;

	private long numVertices = 0;

	private long edges = 0;

	private double summedRank = 0;

	private double finalDiff = 0;

	public void add(double diffDelta, double rankDelta, double danglingRankDelta, long danglingVerticesDelta,
			long verticesDelta, long edgesDelta, double summedRankDelta, double finalDiffDelta) {
		diff += diffDelta;
		rank += rankDelta;
		danglingRank += danglingRankDelta;
		numDanglingVertices += danglingVerticesDelta;
		numVertices += verticesDelta;
		edges += edgesDelta;
		summedRank += summedRankDelta;
		finalDiff += finalDiffDelta;
	}

	@Override
	public void add(PageRankStats pageRankStats) {
		diff += pageRankStats.diff();
		rank += pageRankStats.rank();
		danglingRank += pageRankStats.danglingRank();
		numDanglingVertices += pageRankStats.numDanglingVertices();
		numVertices += pageRankStats.numVertices();
		edges += pageRankStats.edges();
		summedRank += pageRankStats.summedRank();
		finalDiff += pageRankStats.finalDiff();
	}

	@Override
	public PageRankStats getLocalValue() {
		return new PageRankStats(diff, rank, danglingRank, numDanglingVertices, numVertices, edges, summedRank,
				finalDiff);
	}

	@Override
	public void resetLocal() {
		diff = 0;
		rank = 0;
		danglingRank = 0;
		numDanglingVertices = 0;
		numVertices = 0;
		edges = 0;
		summedRank = 0;
		finalDiff = 0;
	}

	@Override
	public void merge(Accumulator<PageRankStats, PageRankStats> other) {
		this.add(other.getLocalValue());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(diff);
		out.writeDouble(rank);
		out.writeDouble(danglingRank);
		out.writeLong(numDanglingVertices);
		out.writeLong(numVertices);
		out.writeLong(edges);
		out.writeDouble(summedRank);
		out.writeDouble(finalDiff);
	}

	@Override
	public void read(DataInput in) throws IOException {
		diff = in.readDouble();
		rank = in.readDouble();
		danglingRank = in.readDouble();
		numDanglingVertices = in.readLong();
		numVertices = in.readLong();
		edges = in.readLong();
		summedRank = in.readDouble();
		finalDiff = in.readDouble();
	}
}
