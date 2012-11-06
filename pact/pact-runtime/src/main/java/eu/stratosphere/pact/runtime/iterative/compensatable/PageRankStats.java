package eu.stratosphere.pact.runtime.iterative.compensatable;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PageRankStats implements Value {

  private double diff;
  private double rank;
  private long numVertices;

  public PageRankStats() {}

  public PageRankStats(double diff, double rank, long numVertices) {
    this.diff = diff;
    this.rank = rank;
    this.numVertices = numVertices;
  }

  public double diff() {
    return diff;
  }

  public double rank() {
    return rank;
  }

  public long numVertices() {
    return numVertices;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(diff);
    out.writeDouble(rank);
    out.writeLong(numVertices);
  }

  @Override
  public void read(DataInput in) throws IOException {
    diff = in.readDouble();
    rank = in.readDouble();
    numVertices = in.readLong();
  }

  @Override
  public String toString() {
    return "PageRankStats: diff [" + diff +"], rank [" + rank + "], numVertices [" + numVertices + "]";
  }
}
