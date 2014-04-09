package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;
import java.util.List;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.util.MutableObjectIterator;

public abstract class AbstractMutableHashTable<T> {
	
	/**
	 * The utilities to serialize the build side data types.
	 */
	protected final TypeSerializer<T> buildSideSerializer;
	
	/**
	 * The utilities to hash and compare the build side data types.
	 */
	protected final TypeComparator<T> buildSideComparator;
	
	
	public AbstractMutableHashTable (TypeSerializer<T> buildSideSerializer, TypeComparator<T> buildSideComparator) {
		this.buildSideSerializer = buildSideSerializer;
		this.buildSideComparator = buildSideComparator;
	}
	
	public TypeSerializer<T> getBuildSideSerializer() {
		return this.buildSideSerializer;
	}
	
	public TypeComparator<T> getBuildSideComparator() {
		return this.buildSideComparator;
	}
	
	// ------------- Life-cycle functions -------------
	
	public abstract void open();
	
	public abstract void close();
	
	public abstract void abort();
	
	public abstract void buildTable(final MutableObjectIterator<T> input) throws IOException;
	
	public abstract List<MemorySegment> getFreeMemory();
	
	// ------------- Modifier -------------
	
	public abstract void insert(T record) throws IOException;
	
	public abstract void insertOrReplaceRecord(T record, T tempHolder) throws IOException;
	
	// ------------- Accessors -------------
	
	public abstract MutableObjectIterator<T> getEntryIterator();
	
	public abstract AbstractHashTableProber<?, T> getProber(TypeComparator<?> probeSideComparator, TypePairComparator<?, T> pairComparator);

}
