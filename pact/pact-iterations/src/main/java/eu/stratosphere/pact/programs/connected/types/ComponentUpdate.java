package eu.stratosphere.pact.programs.connected.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class ComponentUpdate implements Value {
  private static final ComponentUpdateAccessor accessor =
      new ComponentUpdateAccessor();
  protected long vid;
  protected long cid;

  public ComponentUpdate() {

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

}
