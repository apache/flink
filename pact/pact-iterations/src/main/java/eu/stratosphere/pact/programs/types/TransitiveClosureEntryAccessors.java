package eu.stratosphere.pact.programs.types;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;


/**
 * 
 */
public class TransitiveClosureEntryAccessors implements TypeAccessorsV2<TransitiveClosureEntry> {
  private long referenceKey;

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createInstance()
   */
  @Override
  public TransitiveClosureEntry createInstance()
  {
    return new TransitiveClosureEntry();
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createCopy(java.lang.Object)
   */
  @Override
  public TransitiveClosureEntry createCopy(TransitiveClosureEntry from)
  {
    return new TransitiveClosureEntry(
      from.getVid(), from.getCid(),
      Arrays.copyOf(from.getNeighbors(), from.getNumNeighbors()));
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copyTo(java.lang.Object, java.lang.Object)
   */
  @Override
  public void copyTo(TransitiveClosureEntry from, TransitiveClosureEntry to)
  {
    to.setVid(from.getVid());
    to.setCid(from.getCid());

    if (to.getNeighbors().length < from.getNumNeighbors()) {
      System.arraycopy(from.getNeighbors(), 0, to.getNeighbors(), 0, from.getNumNeighbors());
    } else {
      to.setNeighbors(Arrays.copyOf(from.getNeighbors(), from.getNumNeighbors()), from.getNumNeighbors());
    }
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getLength()
   */
  @Override
  public int getLength()
  {
    return -1;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
   */
  @Override
  public long serialize(TransitiveClosureEntry record, DataOutputViewV2 target) throws IOException
  {
    target.writeLong(record.getVid());
    target.writeLong(record.getCid());

    final long[] n = record.getNeighbors();
    final int num = record.getNumNeighbors();

    target.writeInt(num);
    for (int i = 0; i < num; i++) {
      target.writeLong(n[i]);
    }

    return (num * 8) + 20;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputView)
   */
  @Override
  public void deserialize(TransitiveClosureEntry target, DataInputViewV2 source) throws IOException
  {
    target.setVid(source.readLong());
    target.setCid(source.readLong());

    final int num = source.readInt();

    final long[] n;
    if (target.getNeighbors().length >= num) {
      n = target.getNeighbors();
      target.setNeighbors(n, num);
    } else {
      n = new long[num];
      target.setNeighbors(n, num);
    }
    for (int i = 0; i < num; i++) {
      n[i] = source.readLong();
    }
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copy(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
   */
  @Override
  public void copy(DataInputViewV2 source, DataOutputViewV2 target) throws IOException
  {
    // copy vid, cid
    for (int i = 0; i < 16; i++) {
      target.writeByte(source.readUnsignedByte());
    }

    int num = source.readInt();
    target.writeInt(num);
    for (int i = 8 * num; i > 0; --i) {
      target.writeByte(source.readUnsignedByte());
    }
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#hash(java.lang.Object)
   */
  @Override
  public int hash(TransitiveClosureEntry object) {
    final long vid = object.getVid();
    return ((int) (vid >>> 32)) ^ ((int) vid);
  }

  @Override
  public void setReferenceForEquality(TransitiveClosureEntry reference) {
    this.referenceKey = reference.getVid();
  }

  @Override
  public boolean equalToReference(TransitiveClosureEntry candidate) {
    return candidate.getVid() == this.referenceKey;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(java.lang.Object, java.lang.Object, java.util.Comparator)
   */
  @Override
  public int compare(TransitiveClosureEntry first, TransitiveClosureEntry second, Comparator<Key> comparator) {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataInputView)
   */
  @Override
  public int compare(DataInputViewV2 source1, DataInputViewV2 source2) throws IOException {
    long diff =  source1.readLong() - source2.readLong();
    return diff < 0 ? -1 : diff > 0 ? 1 : 0;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#supportsNormalizedKey()
   */
  @Override
  public boolean supportsNormalizedKey() {
    return true;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getNormalizeKeyLen()
   */
  @Override
  public int getNormalizeKeyLen() {
    return 8;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#isNormalizedKeyPrefixOnly(int)
   */
  @Override
  public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
    return false;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#putNormalizedKey(java.lang.Object, byte[], int, int)
   */
  @Override
  public void putNormalizedKey(TransitiveClosureEntry record, byte[] target, int offset, int len)
  {
    final long value = record.getVid();

    if (len == 8) {
      // default case, full normalized key
      long highByte = ((value >>> 56) & 0xff);
      highByte -= Byte.MIN_VALUE;
      target[offset    ] = (byte) highByte;
      target[offset + 1] = (byte) (value >>> 48);
      target[offset + 2] = (byte) (value >>> 40);
      target[offset + 3] = (byte) (value >>> 32);
      target[offset + 4] = (byte) (value >>> 24);
      target[offset + 5] = (byte) (value >>> 16);
      target[offset + 6] = (byte) (value >>>  8);
      target[offset + 7] = (byte) (value       );
    }
    else if (len <= 0) {
    }
    else if (len < 8) {
      long highByte = ((value >>> 56) & 0xff);
      highByte -= Byte.MIN_VALUE;
      target[offset] = (byte) highByte;
      len--;
      for (int i = 1; len > 0; len--, i++) {
        target[offset + i] = (byte) (value >>> ((7-i)<<3));
      }
    }
    else {
      long highByte = ((value >>> 56) & 0xff);
      highByte -= Byte.MIN_VALUE;
      target[offset    ] = (byte) highByte;
      target[offset + 1] = (byte) (value >>> 48);
      target[offset + 2] = (byte) (value >>> 40);
      target[offset + 3] = (byte) (value >>> 32);
      target[offset + 4] = (byte) (value >>> 24);
      target[offset + 5] = (byte) (value >>> 16);
      target[offset + 6] = (byte) (value >>>  8);
      target[offset + 7] = (byte) (value       );
      for (int i = 8; i < len; i++) {
        target[offset + i] = 0;
      }
    }
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#duplicate()
   */
  @Override
  public TransitiveClosureEntryAccessors duplicate() {
    return new TransitiveClosureEntryAccessors();
  }
}
