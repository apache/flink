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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import static org.apache.flink.core.memory.MemoryUtils.getByteBufferAddress;

/**
 * This class represents a piece of memory managed by Flink.
 *
 * <p>The memory can be on-heap, off-heap direct or off-heap unsafe,
 * this is transparently handled by this class.
 *
 * <p>This class specializes byte access and byte copy calls for heap memory, while reusing the
 * multi-byte type accesses and cross-segment operations from the MemorySegment.
 *
 * <p>This class subsumes the functionality of the {@link org.apache.flink.core.memory.HeapMemorySegment},
 * but is a bit less efficient for operations on individual bytes.
 *
 * <p>Note that memory segments should usually not be allocated manually, but rather through the
 * {@link MemorySegmentFactory}.
 */
@Internal
public final class HybridMemorySegment extends MemorySegment {
	/**
	 * The direct byte buffer that wraps the off-heap memory. This memory segment holds a
	 * reference to that buffer, so as long as this memory segment lives, the memory will not be
	 * released.
	 */
	@Nullable
	private ByteBuffer offHeapBuffer;

	/**
	  * Creates a new memory segment that represents the memory backing the given direct byte buffer.
	  * Note that the given ByteBuffer must be direct {@link java.nio.ByteBuffer#allocateDirect(int)},
	  * otherwise this method with throw an IllegalArgumentException.
	  *
	  * <p>The memory segment references the given owner.
	  *
	  * @param buffer The byte buffer whose memory is represented by this memory segment.
	  * @param owner The owner references by this memory segment.
	  * @throws IllegalArgumentException Thrown, if the given ByteBuffer is not direct.
	  */
	HybridMemorySegment(@Nonnull ByteBuffer buffer, @Nullable Object owner) {
		super(getByteBufferAddress(buffer), buffer.capacity(), owner);
		this.offHeapBuffer = buffer;
	}

	/**
	 * Creates a new memory segment that represents the memory of the byte array.
	 *
	 * <p>The memory segment references the given owner.
	 *
	 * @param buffer The byte array whose memory is represented by this memory segment.
	 * @param owner The owner references by this memory segment.
	 */
	HybridMemorySegment(byte[] buffer, Object owner) {
		super(buffer, owner);
		this.offHeapBuffer = null;
	}

	// -------------------------------------------------------------------------
	//  MemorySegment operations
	// -------------------------------------------------------------------------

	@Override
	public void free() {
		super.free();
		offHeapBuffer = null; // to enable GC of unsafe memory
	}

	/**
	 * Gets the buffer that owns the memory of this memory segment.
	 *
	 * @return The byte buffer that owns the memory of this memory segment.
	 */
	public ByteBuffer getOffHeapBuffer() {
		if (offHeapBuffer != null) {
			return offHeapBuffer;
		} else if (isFreed()) {
			throw new IllegalStateException("segment has been freed");
		} else {
			throw new IllegalStateException("Memory segment does not represent off heap memory");
		}
	}

	@Override
	public ByteBuffer wrap(int offset, int length) {
		if (address <= addressLimit) {
			if (heapMemory != null) {
				return ByteBuffer.wrap(heapMemory, offset, length);
			}
			else {
				try {
					ByteBuffer wrapper = offHeapBuffer.duplicate();
					wrapper.limit(offset + length);
					wrapper.position(offset);
					return wrapper;
				}
				catch (IllegalArgumentException e) {
					throw new IndexOutOfBoundsException();
				}
			}
		}
		else {
			throw new IllegalStateException("segment has been freed");
		}
	}

	// ------------------------------------------------------------------------
	//  Random Access get() and put() methods
	// ------------------------------------------------------------------------

	@Override
	public final byte get(int index) {
		final long pos = address + index;
		if (index >= 0 && pos < addressLimit) {
			return UNSAFE.getByte(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public final void put(int index, byte b) {
		final long pos = address + index;
		if (index >= 0 && pos < addressLimit) {
			UNSAFE.putByte(heapMemory, pos, b);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
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
		// check the byte array offset and length and the status
		if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
			throw new IndexOutOfBoundsException();
		}

		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - length) {
			final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
			UNSAFE.copyMemory(heapMemory, pos, dst, arrayAddress, length);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			throw new IndexOutOfBoundsException(String.format("pos: %d, length: %d, index: %d, offset: %d", pos, length, index, offset));
		}
	}

	@Override
	public final void put(int index, byte[] src, int offset, int length) {
		// check the byte array offset and length
		if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
			throw new IndexOutOfBoundsException();
		}

		final long pos = address + index;

		if (index >= 0 && pos <= addressLimit - length) {
			final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
			UNSAFE.copyMemory(src, arrayAddress, heapMemory, pos, length);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public final boolean getBoolean(int index) {
		return get(index) != 0;
	}

	@Override
	public final void putBoolean(int index, boolean value) {
		put(index, (byte) (value ? 1 : 0));
	}

	// -------------------------------------------------------------------------
	//  Bulk Read and Write Methods
	// -------------------------------------------------------------------------

	@Override
	public final void get(DataOutput out, int offset, int length) throws IOException {
		if (address <= addressLimit) {
			if (heapMemory != null) {
				out.write(heapMemory, offset, length);
			}
			else {
				while (length >= 8) {
					out.writeLong(getLongBigEndian(offset));
					offset += 8;
					length -= 8;
				}

				while (length > 0) {
					out.writeByte(get(offset));
					offset++;
					length--;
				}
			}
		}
		else {
			throw new IllegalStateException("segment has been freed");
		}
	}

	@Override
	public final void put(DataInput in, int offset, int length) throws IOException {
		if (address <= addressLimit) {
			if (heapMemory != null) {
				in.readFully(heapMemory, offset, length);
			}
			else {
				while (length >= 8) {
					putLongBigEndian(offset, in.readLong());
					offset += 8;
					length -= 8;
				}
				while (length > 0) {
					put(offset, in.readByte());
					offset++;
					length--;
				}
			}
		}
		else {
			throw new IllegalStateException("segment has been freed");
		}
	}

	@Override
	public final void get(int offset, ByteBuffer target, int numBytes) {
		// check the byte array offset and length
		if ((offset | numBytes | (offset + numBytes)) < 0) {
			throw new IndexOutOfBoundsException();
		}
		if (target.isReadOnly()) {
			throw new ReadOnlyBufferException();
		}

		final int targetOffset = target.position();
		final int remaining = target.remaining();

		if (remaining < numBytes) {
			throw new BufferOverflowException();
		}

		if (target.isDirect()) {
			// copy to the target memory directly
			final long targetPointer = getByteBufferAddress(target) + targetOffset;
			final long sourcePointer = address + offset;

			if (sourcePointer <= addressLimit - numBytes) {
				UNSAFE.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
				target.position(targetOffset + numBytes);
			}
			else if (address > addressLimit) {
				throw new IllegalStateException("segment has been freed");
			}
			else {
				throw new IndexOutOfBoundsException();
			}
		}
		else if (target.hasArray()) {
			// move directly into the byte array
			get(offset, target.array(), targetOffset + target.arrayOffset(), numBytes);

			// this must be after the get() call to ensue that the byte buffer is not
			// modified in case the call fails
			target.position(targetOffset + numBytes);
		}
		else {
			// other types of byte buffers
			throw new IllegalArgumentException("The target buffer is not direct, and has no array.");
		}
	}

	@Override
	public final void put(int offset, ByteBuffer source, int numBytes) {
		// check the byte array offset and length
		if ((offset | numBytes | (offset + numBytes)) < 0) {
			throw new IndexOutOfBoundsException();
		}

		final int sourceOffset = source.position();
		final int remaining = source.remaining();

		if (remaining < numBytes) {
			throw new BufferUnderflowException();
		}

		if (source.isDirect()) {
			// copy to the target memory directly
			final long sourcePointer = getByteBufferAddress(source) + sourceOffset;
			final long targetPointer = address + offset;

			if (targetPointer <= addressLimit - numBytes) {
				UNSAFE.copyMemory(null, sourcePointer, heapMemory, targetPointer, numBytes);
				source.position(sourceOffset + numBytes);
			}
			else if (address > addressLimit) {
				throw new IllegalStateException("segment has been freed");
			}
			else {
				throw new IndexOutOfBoundsException();
			}
		}
		else if (source.hasArray()) {
			// move directly into the byte array
			put(offset, source.array(), sourceOffset + source.arrayOffset(), numBytes);

			// this must be after the get() call to ensue that the byte buffer is not
			// modified in case the call fails
			source.position(sourceOffset + numBytes);
		}
		else {
			// other types of byte buffers
			for (int i = 0; i < numBytes; i++) {
				put(offset++, source.get());
			}
		}
	}
}
