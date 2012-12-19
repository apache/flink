package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class PactLongArray implements Value {

  private long[] values;

  public PactLongArray() {
  }

  public PactLongArray(Set<Long> valueSet) {
    values = new long[valueSet.size()];
    int n = 0;
    for (Long value : valueSet) {
      values[n++] = value;
    }
  }

  public PactLongArray(long[] values) {
    this.values = values;
  }

  public long[] values() {
    return values;
  }

  public int length() {
    return values.length;
  }

  public void set(long[] values) {
    this.values = values;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);
    for (int n = 0; n < values.length; n++) {
      out.writeLong(values[n]);
    }
  }

  public void read(DataInput in) throws IOException {
    values = new long[in.readInt()];
    for (int n = 0; n < values.length; n++) {
      values[n] = in.readLong();
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    String sep = "";
    for (long value : values) {
      buffer.append(sep);
      buffer.append(value);
      sep = " ";
    }

    return buffer.toString();
  }
}