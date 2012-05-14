package eu.stratosphere.pact.programs.preparation.tasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class LongList implements Value {

  private long[] list;
  private int length;

  public LongList() {

  }

  public void setList(long[] list, int length) {
    this.list = list;
    this.length = length;
  }

  public long[] getList() {
    return list;
  }

  public int getLength() {
    return length;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(length);
    for (int i = 0; i < length; i++) {
      out.writeLong(list[i]);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    length = in.readInt();
    if (list == null || list.length < length) {
      list = new long[length];
    }

    for (int i = 0; i < length; i++) {
      list[i] = in.readLong();
    }
  }

}
