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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * This class represents a piece of heap memory managed by Flink.
 * The segment is backed by a byte array and features random put and get methods for the basic types,
 * as well as compare and swap methods.
 *
 * <p>This class specializes byte access and byte copy calls for heap memory, while reusing the
 * multi-byte type accesses and cross-segment operations from the MemorySegment.
 *
 * <p>Note that memory segments should usually not be allocated manually, but rather through the
 * {@link MemorySegmentFactory}.
 */
@SuppressWarnings("unused")
@Internal
public final class HeapMemorySegment extends MemorySegment {

	/**
	 * An extra reference to the heap memory, so we can let byte array checks fail by the built-in
	 * checks automatically without extra checks.
	 */
	private byte[] memory;

	/**
	 * Creates a new memory segment that represents the data in the given byte array.
	 * The owner of this memory segment is null.
	 *
	 * @param memory The byte array that holds the data.
	 */
	HeapMemorySegment(byte[] memory) {
		this(memory, null);
	}

	/**
	 * Creates a new memory segment that represents the data in the given byte array.
	 * The memory segment references the given owner.
	 *
	 * @param memory The byte array that holds the data.
	 * @param owner The owner referenced by the memory segment.
	 */
	HeapMemorySegment(byte[] memory, Object owner) {
		super(Objects.requireNonNull(memory), owner);
		this.memory = memory;
	}

	// -------------------------------------------------------------------------
	//  MemorySegment operations
	// -------------------------------------------------------------------------

	@Override
	public void free() {
		super.free();
		this.memory = null;
	}

	@Override
	public ByteBuffer wrap(int offset, int length) {
		try {
			return ByteBuffer.wrap(this.memory, offset, length);
		}
		catch (NullPointerException e) {
			throw new IllegalStateException("segment has been freed");
		}
	}

	/**
	 * Gets the byte array that backs this memory segment.
	 *
	 * @return The byte array that backs this memory segment, or null, if the segment has been freed.
	 */
	public byte[] getArray() {
		return this.heapMemory;
	}

	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------

	@Override
	public final byte get(int index) {
		return this.memory[index];
	}

	@Override
	public final void put(int index, byte b) {
		this.memory[index] = b;
	}

	@Override
	public final void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}

	@Override
	public final void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}

	@Override
	public final void get(int index, byte[] dst, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, index, dst, offset, length);
	}

	@Override
	public final void put(int index, byte[] src, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(src, offset, this.memory, index, length);
	}

	@Override
	public final boolean getBoolean(int index) {
		return this.memory[index] != 0;
	}

	@Override
	public final void putBoolean(int index, boolean value) {
		this.memory[index] = (byte) (value ? 1 : 0);
	}

	// -------------------------------------------------------------------------
	//                     Bulk Read and Write Methods
	// -------------------------------------------------------------------------

	@Override
	public final void get(DataOutput out, int offset, int length) throws IOException {
		out.write(this.memory, offset, length);
	}

	@Override
	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}

	@Override
	public final void get(int offset, ByteBuffer target, int numBytes) {
		// ByteBuffer performs the boundary checks
		target.put(this.memory, offset, numBytes);
	}

	@Override
	public final void put(int offset, ByteBuffer source, int numBytes) {
		// ByteBuffer performs the boundary checks
		source.get(this.memory, offset, numBytes);
	}

	// -------------------------------------------------------------------------
	//                             Factoring
	// -------------------------------------------------------------------------

	/**
	 * A memory segment factory that produces heap memory segments. Note that this factory does not
	 * support to allocate off-heap memory.
	 */
	public static final class HeapMemorySegmentFactory  {

		/**
		 * Creates a new memory segment that targets the given heap memory region.
		 *
		 * @param memory The heap memory region.
		 * @return A new memory segment that targets the given heap memory region.
		 */
		public HeapMemorySegment wrap(byte[] memory) {
			return new HeapMemorySegment(memory);
		}

		/**
		 * Allocates some unpooled memory and creates a new memory segment that represents
		 * that memory.
		 *
		 * @param size The size of the memory segment to allocate.
		 * @param owner The owner to associate with the memory segment.
		 * @return A new memory segment, backed by unpooled heap memory.
		 */
		public HeapMemorySegment allocateUnpooledSegment(int size, Object owner) {
			return new HeapMemorySegment(new byte[size], owner);
		}

		/**
		 * Creates a memory segment that wraps the given byte array.
		 *
		 * <p>This method is intended to be used for components which pool memory and create
		 * memory segments around long-lived memory regions.
		 *
		 * @param memory The heap memory to be represented by the memory segment.
		 * @param owner The owner to associate with the memory segment.
		 * @return A new memory segment representing the given heap memory.
		 */
		public HeapMemorySegment wrapPooledHeapMemory(byte[] memory, Object owner) {
			return new HeapMemorySegment(memory, owner);
		}

		/**
		 * Prevent external instantiation.
		 */
		HeapMemorySegmentFactory() {}
	}

	public static final HeapMemorySegmentFactory FACTORY = new HeapMemorySegmentFactory();
}
