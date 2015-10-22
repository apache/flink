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

package org.apache.flink.core.memory.benchmarks;

import org.apache.flink.core.memory.MemoryUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PureHeapMemorySegment {

	/** Constant that flags the byte order. Because this is a boolean constant,
	 * the JIT compiler can use this well to aggressively eliminate the non-applicable code paths */
	private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

	/** The array in which the data is stored. */
	private byte[] memory;

	/** Wrapper for I/O requests. */
	private ByteBuffer wrapper;

	/** The size, stored extra, because we may clear the reference to the byte array */
	private final int size;

	// -------------------------------------------------------------------------
	//                             Constructors
	// -------------------------------------------------------------------------

	/**
	 * Creates a new memory segment that represents the data in the given byte array.
	 *
	 * @param memory The byte array that holds the data.
	 */
	public PureHeapMemorySegment(byte[] memory) {
		this.memory = memory;
		this.size = memory.length;
	}

	// -------------------------------------------------------------------------
	//                      Direct Memory Segment Specifics
	// -------------------------------------------------------------------------

	/**
	 * Gets the byte array that backs this memory segment.
	 *
	 * @return The byte array that backs this memory segment.
	 */
	public byte[] getArray() {
		return this.memory;
	}

	// -------------------------------------------------------------------------
	//                        MemorySegment Accessors
	// -------------------------------------------------------------------------
	
	public final boolean isFreed() {
		return this.memory == null;
	}
	
	public final void free() {
		this.wrapper = null;
		this.memory = null;
	}
	
	public final int size() {
		return this.size;
	}
	
	public final ByteBuffer wrap(int offset, int length) {
		if (offset > this.memory.length || offset > this.memory.length - length) {
			throw new IndexOutOfBoundsException();
		}

		if (this.wrapper == null) {
			this.wrapper = ByteBuffer.wrap(this.memory, offset, length);
		}
		else {
			this.wrapper.limit(offset + length);
			this.wrapper.position(offset);
		}

		return this.wrapper;
	}
	
	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------
	
	public final byte get(int index) {
		return this.memory[index];
	}
	
	public final void put(int index, byte b) {
		this.memory[index] = b;
	}
	
	public final void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}
	
	public final void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}
	
	public final void get(int index, byte[] dst, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, index, dst, offset, length);
	}
	
	public final void put(int index, byte[] src, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(src, offset, this.memory, index, length);
	}
	
	public final boolean getBoolean(int index) {
		return this.memory[index] != 0;
	}
	
	public final void putBoolean(int index, boolean value) {
		this.memory[index] = (byte) (value ? 1 : 0);
	}
	
	@SuppressWarnings("restriction")
	public final char getChar(int index) {
		if (index >= 0 && index <= this.memory.length - 2) {
			return UNSAFE.getChar(this.memory, BASE_OFFSET + index);
		} else {
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
		if (index >= 0 && index <= this.memory.length - 2) {
			UNSAFE.putChar(this.memory, BASE_OFFSET + index, value);
		} else {
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
		if (index >= 0 && index <= this.memory.length - 2) {
			return UNSAFE.getShort(this.memory, BASE_OFFSET + index);
		} else {
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
		if (index >= 0 && index <= this.memory.length - 2) {
			UNSAFE.putShort(this.memory, BASE_OFFSET + index, value);
		} else {
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
		if (index >= 0 && index <= this.memory.length - 4) {
			return UNSAFE.getInt(this.memory, BASE_OFFSET + index);
		} else {
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
		if (index >= 0 && index <= this.memory.length - 4) {
			UNSAFE.putInt(this.memory, BASE_OFFSET + index, value);
		} else {
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
		if (index >= 0 && index <= this.memory.length - 8) {
			return UNSAFE.getLong(this.memory, BASE_OFFSET + index);
		} else {
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
		if (index >= 0 && index <= this.memory.length - 8) {
			UNSAFE.putLong(this.memory, BASE_OFFSET + index, value);
		} else {
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
		out.write(this.memory, offset, length);
	}
	
	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}
	
	public final void get(int offset, ByteBuffer target, int numBytes) {
		// ByteBuffer performs the boundary checks
		target.put(this.memory, offset, numBytes);
	}
	
	public final void put(int offset, ByteBuffer source, int numBytes) {
		// ByteBuffer performs the boundary checks
		source.get(this.memory, offset, numBytes);
	}
	
	public final void copyTo(int offset, PureHeapMemorySegment target, int targetOffset, int numBytes) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, offset, target.memory, targetOffset, numBytes);
	}

	// -------------------------------------------------------------------------
	//                      Comparisons & Swapping
	// -------------------------------------------------------------------------
	
	public final int compare(PureHeapMemorySegment seg2, int offset1, int offset2, int len) {
		final byte[] b2 = seg2.memory;
		final byte[] b1 = this.memory;

		int val = 0;
		for (int pos = 0; pos < len && (val = (b1[offset1 + pos] & 0xff) - (b2[offset2 + pos] & 0xff)) == 0; pos++);
		return val;
	}

	public final void swapBytes(PureHeapMemorySegment seg2, int offset1, int offset2, int len) {
		// swap by bytes (chunks of 8 first, then single bytes)
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
	
	public final void swapBytes(byte[] auxBuffer, PureHeapMemorySegment seg2, int offset1, int offset2, int len) {
		byte[] otherMem = seg2.memory;
		System.arraycopy(this.memory, offset1, auxBuffer, 0, len);
		System.arraycopy(otherMem, offset2, this.memory, offset1, len);
		System.arraycopy(auxBuffer, 0, otherMem, offset2, len);
	}

	// --------------------------------------------------------------------------------------------
	//                     Utilities for native memory accesses and checks
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

	@SuppressWarnings("restriction")
	private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
}
