/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.hash;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.MutableObjectIterator;

public abstract class AbstractMutableHashTable<T> {
	
	/**
	 * The utilities to serialize the build side data types.
	 */
	protected final TypeSerializer<T> buildSideSerializer;
	
	/**
	 * The utilities to hash and compare the build side data types.
	 */
	protected final TypeComparator<T> buildSideComparator;

	/** The lock to synchronize state changes (open / close) on */
	protected final Object stateLock = new Object();

	/**
	 * Flag to mark the table as open / closed.
	 * Because we allow to open and close multiple times, the state is initially closed.
	 */
	protected boolean closed = true;



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

	/**
	 * Initialize the hash table
	 */
	public abstract void open();

	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them. The call to this method is valid both as a cleanup after the
	 * complete inputs were properly processed, and as a cancellation call, which cleans up
	 * all resources that are currently held by the hash table. If another process still accesses the hash
	 * table after close has been called, no operations will be performed.
	 */
	public abstract void close();
	
	public abstract void abort();
	
	public abstract List<MemorySegment> getFreeMemory();
	
	// ------------- Modifier -------------
	
	public abstract void insert(T record) throws IOException;
	
	public abstract void insertOrReplaceRecord(T record) throws IOException;
	
	// ------------- Accessors -------------
	
	public abstract MutableObjectIterator<T> getEntryIterator();

	public abstract <PT> AbstractHashTableProber<PT, T> getProber(TypeComparator<PT> probeSideComparator, TypePairComparator<PT, T> pairComparator);

}
