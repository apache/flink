package eu.stratosphere.pact.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

	@Override
	public String toString() {
		return "PageRankStats: diff [" + diff + "], rank [" + rank + "], danglingRank [" + danglingRank +
			"], numDanglingVertices [" + numDanglingVertices + "], numVertices [" + numVertices + "], edges [" + edges +
			"], summedRank [" + summedRank + "], finalDiff [" + finalDiff + "]";
	}
}
