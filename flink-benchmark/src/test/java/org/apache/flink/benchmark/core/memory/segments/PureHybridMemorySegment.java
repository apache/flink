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

package org.apache.flink.benchmark.core.memory.segments;

import org.apache.flink.core.memory.MemoryUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PureHybridMemorySegment {

	/** Constant that flags the byte order. Because this is a boolean constant,
	 * the JIT compiler can use this well to aggressively eliminate the non-applicable code paths */
	private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
	
	/** The direct byte buffer that allocated the off-heap memory. This memory segment holds a reference
	 * to that buffer, so as long as this memory segment lives, the memory will not be released. */
	private final ByteBuffer offHeapMemory;
	
	/** The heap byte array object relative to which we access the memory. Is non-null if the
	 *  memory is on the heap, is null, if the memory if off the heap. If we have this buffer, we
	 *  must never void this reference, or the memory segment will point to undefined addresses 
	 *  outside the heap and may in out-of-order execution cases cause segmentation faults. */
	private final byte[] heapMemory;

	/** The address to the data, relative to the heap memory byte array. If the heap memory byte array
	 * is null, this becomes an absolute memory address outside the heap. */
	private long address;

	/** The address one byte after the last addressable byte.
	 *  This is address + size while the segment is not disposed */
	private final long addressLimit;

	/** The size in bytes of the memory segment */
	private final int size;

	// -------------------------------------------------------------------------
	//                             Constructors
	// -------------------------------------------------------------------------

	/**
	 * Creates a new memory segment that represents the memory backing the given direct byte buffer.
	 * Note that the given ByteBuffer must be direct {@link java.nio.ByteBuffer#allocateDirect(int)},
	 * otherwise this method with throw an IllegalArgumentException.
	 *
	 * @param buffer The byte buffer whose memory is represented by this memory segment.
	 * @throws IllegalArgumentException Thrown, if the given ByteBuffer is not direct.
	 */
	public PureHybridMemorySegment(ByteBuffer buffer) {
		if (buffer == null || !buffer.isDirect()) {
			throw new IllegalArgumentException("Can't initialize from non-direct ByteBuffer.");
		}

		this.offHeapMemory = buffer;
		this.heapMemory = null;
		this.size = buffer.capacity();
		this.address = getAddress(buffer);
		this.addressLimit = this.address + size;

		if (address >= Long.MAX_VALUE - Integer.MAX_VALUE) {
			throw new RuntimeException("Segment initialized with too large address: " + address
					+ " ; Max allowed address is " + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));
		}
	}

	/**
	 * Creates a new memory segment that represents the memory of the byte array.
	 *
	 * @param buffer The byte array whose memory is represented by this memory segment.
	 */
	public PureHybridMemorySegment(byte[] buffer) {
		if (buffer == null) {
			throw new NullPointerException("buffer");
		}
		
		this.offHeapMemory = null;
		this.heapMemory = buffer;
		this.address = BYTE_ARRAY_BASE_OFFSET;
		this.addressLimit = BYTE_ARRAY_BASE_OFFSET + buffer.length;
		this.size = buffer.length;
	}
	
	// -------------------------------------------------------------------------
	//                      Memory Segment Specifics
	// -------------------------------------------------------------------------

	/**
	 * Gets the size of the memory segment, in bytes.
	 * @return The size of the memory segment.
	 */
	public final int size() {
		return size;
	}

	/**
	 * Checks whether the memory segment was freed.
	 * @return True, if the memory segment has been freed, false otherwise.
	 */
	public final boolean isFreed() {
		return this.address > this.addressLimit;
	}

	/**
	 * Frees this memory segment. After this operation has been called, no further operations are
	 * possible on the memory segment and will fail. The actual memory (heap or off-heap) will only
	 * be released after this memory segment object has become garbage collected. 
	 */
	public final void free() {
		// this ensures we can place no more data and trigger
		// the checks for the freed segment
		address = addressLimit + 1;
	}
	
	/**
	 * Checks whether this memory segment is backed by off-heap memory.
	 * @return True, if the memory segment is backed by off-heap memory, false if it is backed
	 *         by heap memory.
	 */
	public final boolean isOffHeap() {
		return heapMemory == null;
	}

	public byte[] getArray() {
		if (heapMemory != null) {
			return heapMemory;
		} else {
			throw new IllegalStateException("Memory segment does not represent heap memory");
		}
	}
	
	/**
	 * Gets the buffer that owns the memory of this memory segment.
	 *
	 * @return The byte buffer that owns the memory of this memory segment.
	 */
	public ByteBuffer getOffHeapBuffer() {
		if (offHeapMemory != null) {
			return offHeapMemory;
		} else {
			throw new IllegalStateException("Memory segment does not represent off heap memory");
		}
	}
	
	public ByteBuffer wrap(int offset, int length) {
		if (offset < 0 || offset > this.size || offset > this.size - length) {
			throw new IndexOutOfBoundsException();
		}

		if (heapMemory != null) {
			return ByteBuffer.wrap(heapMemory, offset, length);
		}
		else {
			ByteBuffer wrapper = offHeapMemory.duplicate();
			wrapper.limit(offset + length);
			wrapper.position(offset);
			return wrapper;
		}
	}

	/**
	 * Gets this memory segment as a pure heap memory segment.
	 * 
	 * @return A heap memory segment variant of this memory segment.
	 * @throws UnsupportedOperationException Thrown, if this memory segment is not
	 *                                       backed by heap memory.
	 */
	public final PureHeapMemorySegment asHeapSegment() {
		if (heapMemory != null) {
			return new PureHeapMemorySegment(heapMemory);
		} else {
			throw new UnsupportedOperationException("Memory segment is not backed by heap memory");
		}
	}

	/**
	 * Gets this memory segment as a pure off-heap memory segment.
	 *
	 * @return An off-heap memory segment variant of this memory segment.
	 * @throws UnsupportedOperationException Thrown, if this memory segment is not
	 *                                       backed by off-heap memory.
	 */
	public final PureOffHeapMemorySegment asOffHeapSegment() {
		if (offHeapMemory != null) {
			return new PureOffHeapMemorySegment(offHeapMemory);
		} else {
			throw new UnsupportedOperationException("Memory segment is not backed by off-heap memory");
		}
	}

	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("restriction")
	public final byte get(int index) {
		final long pos = address + index;
		if (index >= 0 && pos < addressLimit) {
			return UNSAFE.getByte(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}
	
	@SuppressWarnings("restriction")
	public final void put(int index, byte b) {
		final long pos = address + index;
		if (index >= 0 && pos < addressLimit) {
			UNSAFE.putByte(heapMemory, pos, b);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}
	
	public final void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}
	
	@SuppressWarnings("restriction")
	public final void get(int index, byte[] dst, int offset, int length) {
		// check the byte array offset and length
		if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
			throw new IndexOutOfBoundsException();
		}

		long pos = address + index;

		if (index >= 0 && pos <= addressLimit - length) {
			long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;

			// the copy must proceed in batches not too large, because the JVM may
			// poll for points that are safe for GC (moving the array and changing its address)
			while (length > 0) {
				long toCopy = Math.min(length, COPY_PER_BATCH);
				UNSAFE.copyMemory(heapMemory, pos, dst, arrayAddress, toCopy);
				length -= toCopy;
				pos += toCopy;
				arrayAddress += toCopy;
			}
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}
	
	@SuppressWarnings("restriction")
	public final void put(int index, byte[] src, int offset, int length) {
		// check the byte array offset and length
		if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
			throw new IndexOutOfBoundsException();
		}

		long pos = address + index;

		if (index >= 0 && pos <= addressLimit - length) {
			long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
			while (length > 0) {
				long toCopy = Math.min(length, COPY_PER_BATCH);
				UNSAFE.copyMemory(src, arrayAddress, heapMemory, pos, toCopy);
				length -= toCopy;
				pos += toCopy;
				arrayAddress += toCopy;
			}
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final boolean getBoolean(int index) {
		return get(index) != 0;
	}

	public final void putBoolean(int index, boolean value) {
		put(index, (byte) (value ? 1 : 0));
	}

	@SuppressWarnings("restriction")
	public final char getChar(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			return UNSAFE.getChar(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final char getCharLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getChar(index);
		} else {
			return Character.reverseBytes(getChar(index));
		}
	}

	public final char getCharBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Character.reverseBytes(getChar(index));
		} else {
			return getChar(index);
		}
	}

	@SuppressWarnings("restriction")
	public final void putChar(int index, char value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			UNSAFE.putChar(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final void putCharLittleEndian(int index, char value) {
		if (LITTLE_ENDIAN) {
			putChar(index, value);
		} else {
			putChar(index, Character.reverseBytes(value));
		}
	}

	public final void putCharBigEndian(int index, char value) {
		if (LITTLE_ENDIAN) {
			putChar(index, Character.reverseBytes(value));
		} else {
			putChar(index, value);
		}
	}

	@SuppressWarnings("restriction")
	public final short getShort(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			return UNSAFE.getShort(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final short getShortLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getShort(index);
		} else {
			return Short.reverseBytes(getShort(index));
		}
	}
	
	public final short getShortBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Short.reverseBytes(getShort(index));
		} else {
			return getShort(index);
		}
	}

	@SuppressWarnings("restriction")
	public final void putShort(int index, short value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			UNSAFE.putShort(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final void putShortLittleEndian(int index, short value) {
		if (LITTLE_ENDIAN) {
			putShort(index, value);
		} else {
			putShort(index, Short.reverseBytes(value));
		}
	}
	
	public final void putShortBigEndian(int index, short value) {
		if (LITTLE_ENDIAN) {
			putShort(index, Short.reverseBytes(value));
		} else {
			putShort(index, value);
		}
	}

	@SuppressWarnings("restriction")
	public final int getInt(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 4) {
			return UNSAFE.getInt(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final int getIntLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getInt(index);
		} else {
			return Integer.reverseBytes(getInt(index));
		}
	}
	
	public final int getIntBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Integer.reverseBytes(getInt(index));
		} else {
			return getInt(index);
		}
	}
	
	@SuppressWarnings("restriction")
	public final void putInt(int index, int value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 4) {
			UNSAFE.putInt(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final void putIntLittleEndian(int index, int value) {
		if (LITTLE_ENDIAN) {
			putInt(index, value);
		} else {
			putInt(index, Integer.reverseBytes(value));
		}
	}
	
	public final void putIntBigEndian(int index, int value) {
		if (LITTLE_ENDIAN) {
			putInt(index, Integer.reverseBytes(value));
		} else {
			putInt(index, value);
		}
	}

	@SuppressWarnings("restriction")
	public final long getLong(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 8) {
			return UNSAFE.getLong(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final long getLongLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getLong(index);
		} else {
			return Long.reverseBytes(getLong(index));
		}
	}
	
	public final long getLongBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Long.reverseBytes(getLong(index));
		} else {
			return getLong(index);
		}
	}

	@SuppressWarnings("restriction")
	public final void putLong(int index, long value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 8) {
			UNSAFE.putLong(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	public final void putLongLittleEndian(int index, long value) {
		if (LITTLE_ENDIAN) {
			putLong(index, value);
		} else {
			putLong(index, Long.reverseBytes(value));
		}
	}
	
	public final void putLongBigEndian(int index, long value) {
		if (LITTLE_ENDIAN) {
			putLong(index, Long.reverseBytes(value));
		} else {
			putLong(index, value);
		}
	}

	public final float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}
	
	public final float getFloatLittleEndian(int index) {
		return Float.intBitsToFloat(getIntLittleEndian(index));
	}
	
	public final float getFloatBigEndian(int index) {
		return Float.intBitsToFloat(getIntBigEndian(index));
	}
	
	public final void putFloat(int index, float value) {
		putInt(index, Float.floatToRawIntBits(value));
	}
	
	public final void putFloatLittleEndian(int index, float value) {
		putIntLittleEndian(index, Float.floatToRawIntBits(value));
	}
	
	public final void putFloatBigEndian(int index, float value) {
		putIntBigEndian(index, Float.floatToRawIntBits(value));
	}
	
	public final double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}
	
	public final double getDoubleLittleEndian(int index) {
		return Double.longBitsToDouble(getLongLittleEndian(index));
	}

	public final double getDoubleBigEndian(int index) {
		return Double.longBitsToDouble(getLongBigEndian(index));
	}
	
	public final void putDouble(int index, double value) {
		putLong(index, Double.doubleToRawLongBits(value));
	}
	
	public final void putDoubleLittleEndian(int index, double value) {
		putLongLittleEndian(index, Double.doubleToRawLongBits(value));
	}
	
	public final void putDoubleBigEndian(int index, double value) {
		putLongBigEndian(index, Double.doubleToRawLongBits(value));
	}

	// -------------------------------------------------------------------------
	//                     Bulk Read and Write Methods
	// -------------------------------------------------------------------------

	public final void get(DataOutput out, int offset, int length) throws IOException {
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

	public final void put(DataInput in, int offset, int length) throws IOException {
		if (heapMemory != null) {
			in.readFully(heapMemory, offset, length);
		}
		else {
			while (length >= 8) {
				putLongBigEndian(offset, in.readLong());
				offset += 8;
				length -= 8;
			}
			while(length > 0) {
				put(offset, in.readByte());
				offset++;
				length--;
			}
		}
	}

	@SuppressWarnings("restriction")
	public final void get(int offset, ByteBuffer target, int numBytes) {
		if (heapMemory != null) {
			// ByteBuffer performs the boundary checks
			target.put(heapMemory, offset, numBytes);
		}
		else {
			// check the byte array offset and length
			if ((offset | numBytes | (offset + numBytes) | (size - (offset + numBytes))) < 0) {
				throw new IndexOutOfBoundsException();
			}
	
			final int targetOffset = target.position();
			final int remaining = target.remaining();
	
			if (remaining < numBytes) {
				throw new BufferOverflowException();
			}
	
			if (target.isDirect()) {
				// copy to the target memory directly
				final long targetPointer = getAddress(target) + targetOffset;
				final long sourcePointer = address + offset;
	
				if (sourcePointer <= addressLimit - numBytes) {
					UNSAFE.copyMemory(sourcePointer, targetPointer, numBytes);
				}
				else if (address > addressLimit) {
					throw new IllegalStateException("This segment has been freed.");
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
				// neither heap buffer nor direct buffer
				while (target.hasRemaining()) {
					target.put(get(offset++));
				}
			}
		}
	}

	@SuppressWarnings("restriction")
	public final void put(int offset, ByteBuffer source, int numBytes) {
		if (heapMemory != null) {
			source.get(heapMemory, offset, numBytes);
		}
		else {
			// check the byte array offset and length
			if ((offset | numBytes | (offset + numBytes) | (size - (offset + numBytes))) < 0) {
				throw new IndexOutOfBoundsException();
			}
	
			final int sourceOffset = source.position();
			final int remaining = source.remaining();
	
			if (remaining < numBytes) {
				throw new BufferUnderflowException();
			}
	
			if (source.isDirect()) {
				// copy to the target memory directly
				final long sourcePointer = getAddress(source) + sourceOffset;
				final long targetPointer = address + offset;
	
				if (sourcePointer <= addressLimit - numBytes) {
					UNSAFE.copyMemory(sourcePointer, targetPointer, numBytes);
				}
				else if (address > addressLimit) {
					throw new IllegalStateException("This segment has been freed.");
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
				// neither heap buffer nor direct buffer
				while (source.hasRemaining()) {
					put(offset++, source.get());
				}
			}
		}
	}

	@SuppressWarnings("restriction")
	public final void copyTo(int offset, PureHybridMemorySegment target, int targetOffset, int numBytes) {
		final byte[] thisHeapRef = this.heapMemory;
		final byte[] otherHeapRef = target.heapMemory;
		final long thisPointer = this.address + offset;
		final long otherPointer = target.address + targetOffset;

		if (numBytes >= 0 & thisPointer <= this.addressLimit - numBytes & otherPointer <= target.addressLimit - numBytes) {
			UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
		}
		else if (address > addressLimit | target.address > target.addressLimit) {
			throw new IllegalStateException("segment has been freed.");
		}
		else {
			throw new IndexOutOfBoundsException();
		}
	}

	public int compare(PureHybridMemorySegment seg2, int offset1, int offset2, int len) {
		while (len >= 8) {
			long l1 = this.getLongBigEndian(offset1);
			long l2 = seg2.getLongBigEndian(offset2);

			if (l1 != l2) {
				return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
			}

			offset1 += 8;
			offset2 += 8;
			len -= 8;
		}
		while (len > 0) {
			int b1 = this.get(offset1) & 0xff;
			int b2 = seg2.get(offset2) & 0xff;
			int cmp = b1 - b2;
			if (cmp != 0) {
				return cmp;
			}
			offset1++;
			offset2++;
			len--;
		}
		return 0;
	}

	public void swapBytes(byte[] tempBuffer, PureHybridMemorySegment seg2, int offset1, int offset2, int len) {
		if (len < 32) {
			// fast path for short copies
			while (len >= 8) {
				long tmp = this.getLong(offset1);
				this.putLong(offset1, seg2.getLong(offset2));
				seg2.putLong(offset2, tmp);
				offset1 += 8;
				offset2 += 8;
				len -= 8;
			}
			while (len > 0) {
				byte tmp = this.get(offset1);
				this.put(offset1, seg2.get(offset2));
				seg2.put(offset2, tmp);
				offset1++;
				offset2++;
				len--;
			}
		}
		else if ( (offset1 | offset2 | len | (offset1 + len) | (offset2 + len) |
				(this.size - (offset1 + len)) | (seg2.size() - (offset2 + len))) < 0 || len > tempBuffer.length)
		{
			throw new IndexOutOfBoundsException();
		}
		else {
			final long thisPos = this.address + offset1;
			final long otherPos = seg2.address + offset2;

			if (thisPos <= this.addressLimit - len && otherPos <= seg2.addressLimit - len) {
				final long arrayAddress = BYTE_ARRAY_BASE_OFFSET;

				// this -> temp buffer
				UNSAFE.copyMemory(this.heapMemory, thisPos, tempBuffer, arrayAddress, len);

				// other -> this
				UNSAFE.copyMemory(seg2.heapMemory, otherPos, this.heapMemory, thisPos, len);

				// temp buffer -> other
				UNSAFE.copyMemory(tempBuffer, arrayAddress, seg2.heapMemory, otherPos, len);
			}
			else if (this.address <= 0 || seg2.address <= 0) {
				throw new IllegalStateException("Memory segment has been freed.");
			}
			else {
				// index is in fact invalid
				throw new IndexOutOfBoundsException();
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//                     Utilities for native memory accesses and checks
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

	@SuppressWarnings("restriction")
	private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

	private static final long COPY_PER_BATCH = 1024 * 1024;

	private static final Field ADDRESS_FIELD;

	static {
		try {
			ADDRESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");
			ADDRESS_FIELD.setAccessible(true);
		}
		catch (Throwable t) {
			throw new RuntimeException("Cannot initialize DirectMemorySegment - direct memory not supported by the JVM.");
		}
	}

	private static long getAddress(ByteBuffer buf) {
		try {
			return (Long) ADDRESS_FIELD.get(buf);
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not access direct byte buffer address.", t);
		}
	}
}
