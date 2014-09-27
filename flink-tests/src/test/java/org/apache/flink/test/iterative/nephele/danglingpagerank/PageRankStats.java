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

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

public class PageRankStats implements Value {
	private static final long serialVersionUID = 1L;

	private double diff;

	private double rank;

	private double danglingRank;

	private long numDanglingVertices;

	private long numVertices;

	private long edges;

	private double summedRank;

	private double finalDiff;

	public PageRankStats() {
	}

	public PageRankStats(double diff, double rank, double danglingRank, long numDanglingVertices, long numVertices,
			long edges, double summedRank, double finalDiff) {
		this.diff = diff;
		this.rank = rank;
		this.danglingRank = danglingRank;
		this.numDanglingVertices = numDanglingVertices;
		this.numVertices = numVertices;
		this.edges = edges;
		this.summedRank = summedRank;
		this.finalDiff = finalDiff;
	}

	public double diff() {
		return diff;
	}

	public double rank() {
		return rank;
	}

	public double danglingRank() {
		return danglingRank;
	}

	public long numDanglingVertices() {
		return numDanglingVertices;
	}

	public long numVertices() {
		return numVertices;
	}

	public long edges() {
		return edges;
	}

	public double summedRank() {
		return summedRank;
	}

	public double finalDiff() {
		return finalDiff;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
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
	public void read(DataInputView in) throws IOException {
		diff = in.readDouble();
		rank = in.readDouble();
		danglingRank = in.readDouble();
		numDanglingVertices = in.readLong();
		numVertices = in.readLong();
		edges = in.readLong();
		summedRank = in.readDouble();
		finalDiff = in.readDouble();
	}

	@Override
	public String toString() {
		return "PageRankStats: diff [" + diff + "], rank [" + rank + "], danglingRank [" + danglingRank +
			"], numDanglingVertices [" + numDanglingVertices + "], numVertices [" + numVertices + "], edges [" + edges +
			"], summedRank [" + summedRank + "], finalDiff [" + finalDiff + "]";
	}
}
