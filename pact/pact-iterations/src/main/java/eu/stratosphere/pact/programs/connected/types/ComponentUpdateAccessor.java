package eu.stratosphere.pact.programs.connected.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;

public class ComponentUpdateAccessor implements TypeAccessorsV2<ComponentUpdate> {

  @Override
  public ComponentUpdate createInstance() {
    return new ComponentUpdate();
  }

  @Override
  public ComponentUpdate createCopy(ComponentUpdate from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyTo(ComponentUpdate from, ComponentUpdate to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLength() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long serialize(ComponentUpdate record, DataOutputViewV2 target)
      throws IOException {
    return serialize(record, (DataOutput)target);
  }

  @Override
  public void deserialize(ComponentUpdate target, DataInputViewV2 source)
      throws IOException {
    deserialize(target, (DataInput)source);
  }

  public long serialize(ComponentUpdate record, DataOutput target)
      throws IOException {
    target.writeLong(record.getVid());
    target.writeLong(record.getCid());
    return 2*8;
  }

  public void deserialize(ComponentUpdate target, DataInput source)
      throws IOException {
    target.setVid(source.readLong());
    target.setCid(source.readLong());
  }

  @Override
  public void copy(DataInputViewV2 source, DataOutputViewV2 target)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hash(ComponentUpdate object) {
    final long vid = object.getVid();
    return ((int) (vid >>> 32)) ^ ((int) vid);
  }

  @Override
  public void setReferenceForEquality(ComponentUpdate toCompare) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equalToReference(ComponentUpdate candidate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(ComponentUpdate first, ComponentUpdate second,
      Comparator<Key> comparator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(DataInputViewV2 source1, DataInputViewV2 source2)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsNormalizedKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNormalizeKeyLen() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putNormalizedKey(ComponentUpdate record, byte[] target,
      int offset, int numBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeAccessorsV2<ComponentUpdate> duplicate() {
    throw new UnsupportedOperationException();
  }

}
