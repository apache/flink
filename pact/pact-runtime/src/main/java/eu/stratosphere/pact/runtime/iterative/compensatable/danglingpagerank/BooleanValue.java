package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BooleanValue implements Value {

  private boolean value;

  public BooleanValue(boolean value) {
    this.value = value;
  }

  public BooleanValue() {
  }

  public boolean get() {
    return value;
  }

  public void set(boolean value) {
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(value);
  }

  @Override
  public void read(DataInput in) throws IOException {
    value = in.readBoolean();
  }
}
