package eu.stratosphere.pact.runtime.iterative.playing.scopedpagerank;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SequentialAccessSparseRowVector implements Value {

  private long[] indexes;
  private double[] values;

  public SequentialAccessSparseRowVector() {
  }

  public SequentialAccessSparseRowVector(long[] indexes, double[] values) {
    this.indexes = indexes;
    this.values = values;
  }

  public long[] indexes() {
    return indexes;
  }
  
  public double[] values() {
    return values;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(indexes.length);
    for (int n = 0; n < indexes.length; n++) {
      out.writeLong(indexes[n]);
      out.writeDouble(values[n]);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    int length = in.readInt();
    indexes = new long[length];
    values = new double[length];
    for (int n = 0; n < indexes.length; n++) {
      indexes[n] = in.readLong();
      values[n] = in.readDouble();
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    String sep = "";
    for (int n = 0; n < indexes.length; n++) {
      buffer.append(sep);
      buffer.append(indexes[n]);
      buffer.append(" ");
      buffer.append(values[n]);
      sep = " ";
    }

    return buffer.toString();
  }
}
