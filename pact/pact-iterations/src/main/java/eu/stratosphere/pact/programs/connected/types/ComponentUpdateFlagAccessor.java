package eu.stratosphere.pact.programs.connected.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;

public class ComponentUpdateFlagAccessor implements TypeAccessorsV2<ComponentUpdateFlag> {

	@Override
	public ComponentUpdateFlag createInstance() {
		return new ComponentUpdateFlag();
	}

	@Override
	public ComponentUpdateFlag createCopy(ComponentUpdateFlag from) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void copyTo(ComponentUpdateFlag from, ComponentUpdateFlag to) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getLength() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long serialize(ComponentUpdateFlag record, DataOutputViewV2 target)
			throws IOException {
		return serialize(record, (DataOutput)target);
	}

	@Override
	public void deserialize(ComponentUpdateFlag target, DataInputViewV2 source)
			throws IOException {
		deserialize(target, (DataInput)source);
	}
	
	public long serialize(ComponentUpdateFlag record, DataOutput target)
			throws IOException {
		target.writeLong(record.getVid());
		target.writeLong(record.getCid());
		target.writeBoolean(record.isUpdated());
		return 2*8+1;
	}

	public void deserialize(ComponentUpdateFlag target, DataInput source)
			throws IOException {
		target.setVid(source.readLong());
		target.setCid(source.readLong());
		target.setUpdated(source.readBoolean());
	}

	@Override
	public void copy(DataInputViewV2 source, DataOutputViewV2 target)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hash(ComponentUpdateFlag object) {
		final long vid = object.getVid();
		return ((int) (vid >>> 32)) ^ ((int) vid);
	}

	@Override
	public void setReferenceForEquality(ComponentUpdateFlag toCompare) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean equalToReference(ComponentUpdateFlag candidate) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compare(ComponentUpdateFlag first, ComponentUpdateFlag second,
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
	public void putNormalizedKey(ComponentUpdateFlag record, byte[] target,
			int offset, int numBytes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TypeAccessorsV2<ComponentUpdateFlag> duplicate() {
		return new ComponentUpdateFlagAccessor();
	}

}
