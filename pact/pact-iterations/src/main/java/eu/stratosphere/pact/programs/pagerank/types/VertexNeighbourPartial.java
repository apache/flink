package eu.stratosphere.pact.programs.pagerank.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class VertexNeighbourPartial implements Value {
  private static final VertexNeighbourPartialAccessor accessor =
      new VertexNeighbourPartialAccessor();
  protected long vid;
  protected long nid;
  private double partial;

  public VertexNeighbourPartial() {

  }


  @Override
  public void write(DataOutput out) throws IOException {
    accessor.serialize(this, out);
  }

  @Override
  public void read(DataInput in) throws IOException {
    accessor.deserialize(this, in);
  }


  public long getVid() {
    return vid;
  }


  public void setVid(long vid) {
    this.vid = vid;
  }


  public long getNid() {
    return nid;
  }


  public void setNid(long nid) {
    this.nid = nid;
  }


  public double getPartial() {
    return partial;
  }


  public void setPartial(double partial) {
    this.partial = partial;
  }

}
