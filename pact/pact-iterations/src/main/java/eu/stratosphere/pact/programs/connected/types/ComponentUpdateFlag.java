package eu.stratosphere.pact.programs.connected.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class ComponentUpdateFlag implements Value {
  private static final ComponentUpdateFlagAccessor accessor =
      new ComponentUpdateFlagAccessor();
  protected long vid;
  protected long cid;
  protected boolean updated;

  public ComponentUpdateFlag() {

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


  public long getCid() {
    return cid;
  }


  public void setCid(long cid) {
    this.cid = cid;
  }

  public boolean isUpdated() {
    return updated;
  }

  public void setUpdated(boolean updated) {
    this.updated = updated;
  }
}
