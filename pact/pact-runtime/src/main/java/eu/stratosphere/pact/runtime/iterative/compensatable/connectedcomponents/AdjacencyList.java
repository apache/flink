package eu.stratosphere.pact.runtime.iterative.compensatable.connectedcomponents;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
public class AdjacencyList implements Value {

  private long[] neighbors;

  public AdjacencyList() {
  }

  public AdjacencyList(long[] neighbors) {
    this.neighbors = neighbors;
  }

  public long[] neighborIDs() {
    return neighbors;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(neighbors.length);
    for (int n = 0; n < neighbors.length; n++) {
      out.writeLong(neighbors[n]);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    neighbors = new long[in.readInt()];
    for (int n = 0; n < neighbors.length; n++) {
      neighbors[n] = in.readLong();
    }
  }
}
