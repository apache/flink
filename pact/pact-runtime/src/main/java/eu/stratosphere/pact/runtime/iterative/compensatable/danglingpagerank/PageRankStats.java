package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PageRankStats implements Value {

  private double diff;
  private double rank;
  private double danglingRank;
  private long numVertices;

  public PageRankStats() {}

  public PageRankStats(double diff, double rank, double danglingRank, long numVertices) {
    this.diff = diff;
    this.rank = rank;
    this.danglingRank = danglingRank;
    this.numVertices = numVertices;
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

  public long numVertices() {
    return numVertices;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(diff);
    out.writeDouble(rank);
    out.writeDouble(danglingRank);
    out.writeLong(numVertices);
  }

  @Override
  public void read(DataInput in) throws IOException {
    diff = in.readDouble();
    rank = in.readDouble();
    danglingRank = in.readDouble();
    numVertices = in.readLong();
  }

  @Override
  public String toString() {
    return "PageRankStats: diff [" + diff +"], rank [" + rank + "], danglingRank [" + danglingRank +
        "], numVertices [" + numVertices + "]";
  }
}
