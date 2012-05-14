package eu.stratosphere.pact.programs.pagerank.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;

public class VertexPageRankAccessor implements TypeAccessorsV2<VertexPageRank> {

	@Override
	public VertexPageRank createInstance() {
		return new VertexPageRank();
	}

	@Override
	public VertexPageRank createCopy(VertexPageRank from) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void copyTo(VertexPageRank from, VertexPageRank to) {
		to.setVid(from.getVid());
		to.setRank(from.getRank());
	}

	@Override
	public int getLength() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long serialize(VertexPageRank record, DataOutputViewV2 target)
			throws IOException {
		return serialize(record, (DataOutput)target);
	}

	@Override
	public void deserialize(VertexPageRank target, DataInputViewV2 source)
			throws IOException {
		deserialize(target, (DataInput)source);
	}
	
	public long serialize(VertexPageRank record, DataOutput target)
			throws IOException {
		target.writeLong(record.getVid());
		target.writeDouble(record.getRank());
		return 2*8;
	}

	public void deserialize(VertexPageRank target, DataInput source)
			throws IOException {
		target.setVid(source.readLong());
		target.setRank(source.readDouble());
	}

	@Override
	public void copy(DataInputViewV2 source, DataOutputViewV2 target)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hash(VertexPageRank object) {
		final long vid = object.getVid();
		return ((int) (vid >>> 32)) ^ ((int) vid);
	}

	private long reference;
	
	@Override
	public void setReferenceForEquality(VertexPageRank toCompare) {
		reference = toCompare.getVid();
	}

	@Override
	public boolean equalToReference(VertexPageRank candidate) {
		return reference == candidate.getVid();
	}

	@Override
	public int compare(VertexPageRank first, VertexPageRank second,
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
	public void putNormalizedKey(VertexPageRank record, byte[] target,
			int offset, int numBytes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TypeAccessorsV2<VertexPageRank> duplicate() {
		return new VertexPageRankAccessor();
	}

}
