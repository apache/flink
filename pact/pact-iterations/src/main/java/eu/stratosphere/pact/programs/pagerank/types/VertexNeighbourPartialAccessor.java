package eu.stratosphere.pact.programs.pagerank.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;

public class VertexNeighbourPartialAccessor implements TypeAccessorsV2<VertexNeighbourPartial> {

	@Override
	public VertexNeighbourPartial createInstance() {
		return new VertexNeighbourPartial();
	}

	@Override
	public VertexNeighbourPartial createCopy(VertexNeighbourPartial from) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void copyTo(VertexNeighbourPartial from, VertexNeighbourPartial to) {
		to.setVid(from.getVid());
		to.setNid(from.getNid());
		to.setPartial(from.getPartial());
	}

	@Override
	public int getLength() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long serialize(VertexNeighbourPartial record, DataOutputViewV2 target)
			throws IOException {
		return serialize(record, (DataOutput)target);
	}

	@Override
	public void deserialize(VertexNeighbourPartial target, DataInputViewV2 source)
			throws IOException {
		deserialize(target, (DataInput)source);
	}
	
	public long serialize(VertexNeighbourPartial record, DataOutput target)
			throws IOException {
		target.writeLong(record.getVid());
		target.writeLong(record.getNid());
		target.writeDouble(record.getPartial());
		return 3*8;
	}

	public void deserialize(VertexNeighbourPartial target, DataInput source)
			throws IOException {
		target.setVid(source.readLong());
		target.setNid(source.readLong());
		target.setPartial(source.readDouble());
	}

	@Override
	public void copy(DataInputViewV2 source, DataOutputViewV2 target)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hash(VertexNeighbourPartial object) {
		final long vid = object.getVid();
		return ((int) (vid >>> 32)) ^ ((int) vid);
	}

	private long vid;
	private long nid;
	
	@Override
	public void setReferenceForEquality(VertexNeighbourPartial toCompare) {
		vid = toCompare.vid;
		nid = toCompare.nid;
	}

	@Override
	public boolean equalToReference(VertexNeighbourPartial candidate) {
		return vid == candidate.vid && nid == candidate.nid;
	}

	@Override
	public int compare(VertexNeighbourPartial first, VertexNeighbourPartial second,
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
	public void putNormalizedKey(VertexNeighbourPartial record, byte[] target,
			int offset, int numBytes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TypeAccessorsV2<VertexNeighbourPartial> duplicate() {
		return new VertexNeighbourPartialAccessor();
	}

}
