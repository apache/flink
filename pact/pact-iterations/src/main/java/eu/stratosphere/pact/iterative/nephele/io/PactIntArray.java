package eu.stratosphere.pact.iterative.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class PactIntArray implements Value {

  private int[] data;

  public PactIntArray() {

  }

  public PactIntArray(int[] data) {
    this.data = data;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(data.length);

    for (int i = 0; i < data.length; i++) {
      out.writeInt(data[i]);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    int length = in.readInt();

    data = new int[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = in.readInt();
    }

  }

  public int[] getValue() {
    return data;
  }

}
