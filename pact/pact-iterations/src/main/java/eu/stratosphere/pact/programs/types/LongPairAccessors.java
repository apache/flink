package eu.stratosphere.pact.programs.types;

import java.io.IOException;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;


/**
 * 
 */
public class LongPairAccessors implements TypeAccessorsV2<LongPair>
{
	private long reference;
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createInstance()
	 */
	@Override
	public LongPair createInstance()
	{
		return new LongPair();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createCopy(java.lang.Object)
	 */
	@Override
	public LongPair createCopy(LongPair from)
	{
		return new LongPair(from.getKey(), from.getValue());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(LongPair from, LongPair to)
	{
		to.setKey(from.getKey());
		to.setValue(from.getValue());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getLength()
	 */
	@Override
	public int getLength()
	{
		return 16;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public long serialize(LongPair record, DataOutputViewV2 target) throws IOException
	{
		target.writeLong(record.getKey());
		target.writeLong(record.getValue());
		return 16;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public void deserialize(LongPair target, DataInputViewV2 source) throws IOException {
		target.setKey(source.readLong());
		target.setValue(source.readLong());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copy(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void copy(DataInputViewV2 source, DataOutputViewV2 target) throws IOException
	{
		for (int i = 0; i < 16; i++) {
			target.writeByte(source.readUnsignedByte());
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#hash(java.lang.Object)
	 */
	@Override
	public int hash(LongPair object) {
		final long key = object.getKey();
		return ((int) (key >>> 32)) ^ ((int) key);
	}
	
	public void setReferenceForEquality(LongPair reference) {
		this.reference = reference.getKey();
	}
	
	public boolean equalToReference(LongPair candidate) {
		return candidate.getKey() == this.reference;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(java.lang.Object, java.lang.Object, java.util.Comparator)
	 */
	@Override
	public int compare(LongPair first, LongPair second, Comparator<Key> comparator) {
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
	public void putNormalizedKey(LongPair record, byte[] target, int offset, int len)
	{
		final long value = record.getKey();
		
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
	public LongPairAccessors duplicate() {
		return new LongPairAccessors();
	}
}
