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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class represents a piece of memory allocated from the memory manager. The segment may be backed by 
 * a byte array or by off-heap memory.
 */
public abstract class MemorySegment {
	
	private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
	
	/**
	 * Checks whether this memory segment has already been freed. In that case, the
	 * segment must not be used any more.
	 * 
	 * @return True, if the segment has been freed, false otherwise.
	 */
	public abstract boolean isFreed();
	
	/**
	 * Gets the size of the memory segment, in bytes. Because segments
	 * are backed by arrays, they cannot be larger than two GiBytes.
	 * 
	 * @return The size in bytes.
	 */
	public abstract int size();

	/**
	 * Wraps the chunk of the underlying memory located between <tt>offset<tt> and 
	 * <tt>length</tt> in a NIO ByteBuffer.
	 * 
	 * @param offset The offset in the memory segment.
	 * @param length The number of bytes to be wrapped as a buffer.
	 * @return A <tt>ByteBuffer</tt> backed by the specified portion of the memory segment.
	 * @throws IndexOutOfBoundsException Thrown, if offset is negative or larger than the memory segment size,
	 *                                   or if the offset plus the length is larger than the segment size.
	 */
	public abstract ByteBuffer wrap(int offset, int length);

	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------

	/**
	 * Reads the byte at the given position.
	 * 
	 * @param index The position from which the byte will be read
	 * @return The byte at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public abstract byte get(int index);

	/**
	 * Writes the given byte into this buffer at the given position.
	 * 
	 * @param index The index at which the byte will be written.
	 * @param b The byte value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public abstract void put(int index, byte b);

	/**
	 * Bulk get method. Copies dst.length memory from the specified position to
	 * the destination memory.
	 * 
	 * @param index The position at which the first byte will be read.
	 * @param dst The memory into which the memory will be copied.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the data between the 
	 *                                   index and the memory segment end is not enough to fill the destination array.
	 */
	public abstract void get(int index, byte[] dst);

	/**
	 * Bulk put method. Copies src.length memory from the source memory into the
	 * memory segment beginning at the specified position.
	 * 
	 * @param index The index in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   size exceed the amount of memory between the index and the memory
	 *                                   segment's end. 
	 */
	public abstract void put(int index, byte[] src);

	/**
	 * Bulk get method. Copies length memory from the specified position to the
	 * destination memory, beginning at the given offset
	 * 
	 * @param index The position at which the first byte will be read.
	 * @param dst The memory into which the memory will be copied.
	 * @param offset The copying offset in the destination memory.
	 * @param length The number of bytes to be copied.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the requested number of 
	 *                                   bytes exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	public abstract void get(int index, byte[] dst, int offset, int length);

	/**
	 * Bulk put method. Copies length memory starting at position offset from
	 * the source memory into the memory segment starting at the specified
	 * index.
	 * 
	 * @param index The position in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * @param offset The offset in the source array where the copying is started.
	 * @param length The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   portion to copy exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	public abstract void put(int index, byte[] src, int offset, int length);

	/**
	 * Reads one byte at the given position and returns its boolean
	 * representation.
	 * 
	 * @param index The position from which the memory will be read.
	 * @return The boolean value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	public abstract boolean getBoolean(int index);

	/**
	 * Writes one byte containing the byte value into this buffer at the given
	 * position.
	 * 
	 * @param index The position at which the memory will be written.
	 * @param value The char value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	public abstract void putBoolean(int index, boolean value);

	/**
	 * Reads two memory at the given position, composing them into a char value
	 * according to the current byte order.
	 * 
	 * @param index The position from which the memory will be read.
	 * @return The char value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public abstract char getChar(int index);

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
	
	/**
	 * Writes two memory containing the given char value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param index The position at which the memory will be written.
	 * @param value The char value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public abstract void putChar(int index, char value);

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
	
	/**
	 * Reads two memory at the given position, composing them into a short value
	 * according to the current byte order.
	 * 
	 * @param index The position from which the memory will be read.
	 * @return The short value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public abstract short getShort(int index);
	
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
	
	/**
	 * Writes the given short value into this buffer at the given position, using
	 * the native byte order of the system.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The short value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public abstract void putShort(int index, short value);
	
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
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in the system's native byte order.
	 * This method offers the best speed for integer reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public abstract int getInt(int index);
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in little endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getInt(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getInt(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final int getIntLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getInt(index);
		} else {
			return Integer.reverseBytes(getInt(index));
		}
	}
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in big endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getInt(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getInt(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final int getIntBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Integer.reverseBytes(getInt(index));
		} else {
			return getInt(index);
		}
	}

	/**
	 * Writes the given int value (32bit, 4 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for integer writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The int value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public abstract void putInt(int index, int value);
	
	/**
	 * Writes the given int value (32bit, 4 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putInt(int, int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putInt(int, int)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The int value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final void putIntLittleEndian(int index, int value) {
		if (LITTLE_ENDIAN) {
			putInt(index, value);
		} else {
			putInt(index, Integer.reverseBytes(value));
		}
	}
	
	/**
	 * Writes the given int value (32bit, 4 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putInt(int, int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putInt(int, int)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The int value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final void putIntBigEndian(int index, int value) {
		if (LITTLE_ENDIAN) {
			putInt(index, Integer.reverseBytes(value));
		} else {
			putInt(index, value);
		}
	}
	
	/**
	 * Reads a long value (64bit, 8 bytes) from the given position, in the system's native byte order.
	 * This method offers the best speed for long integer reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public abstract long getLong(int index);
	
	/**
	 * Reads a long integer value (64bit, 8 bytes) from the given position, in little endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getLong(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getLong(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final long getLongLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getLong(index);
		} else {
			return Long.reverseBytes(getLong(index));
		}
	}
	
	/**
	 * Reads a long integer value (64bit, 8 bytes) from the given position, in big endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getLong(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getLong(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final long getLongBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Long.reverseBytes(getLong(index));
		} else {
			return getLong(index);
		}
	}

	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for long integer writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public abstract void putLong(int index, long value);
	
	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putLong(int, long)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putLong(int, long)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putLongLittleEndian(int index, long value) {
		if (LITTLE_ENDIAN) {
			putLong(index, value);
		} else {
			putLong(index, Long.reverseBytes(value));
		}
	}
	
	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putLong(int, long)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putLong(int, long)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putLongBigEndian(int index, long value) {
		if (LITTLE_ENDIAN) {
			putLong(index, Long.reverseBytes(value));
		} else {
			putLong(index, value);
		}
	}
	
	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in the system's
	 * native byte order. This method offers the best speed for float reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The float value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}
	
	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getFloat(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getFloat(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final float getFloatLittleEndian(int index) {
		return Float.intBitsToFloat(getIntLittleEndian(index));
	}
	
	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getFloat(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getFloat(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final float getFloatBigEndian(int index) {
		return Float.intBitsToFloat(getIntBigEndian(index));
	}

	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for float writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The float value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final void putFloat(int index, float value) {
		putInt(index, Float.floatToRawIntBits(value));
	}
	
	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putFloat(int, float)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putFloat(int, float)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putFloatLittleEndian(int index, float value) {
		putIntLittleEndian(index, Float.floatToRawIntBits(value));
	}
	
	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putFloat(int, float)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putFloat(int, float)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putFloatBigEndian(int index, float value) {
		putIntBigEndian(index, Float.floatToRawIntBits(value));
	}
	
	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in the system's
	 * native byte order. This method offers the best speed for double reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The double value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}
	
	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getDouble(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getDouble(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final double getDoubleLittleEndian(int index) {
		return Double.longBitsToDouble(getLongLittleEndian(index));
	}
	
	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getDouble(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getDouble(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final double getDoubleBigEndian(int index) {
		return Double.longBitsToDouble(getLongBigEndian(index));
	}

	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in the
	 * system's native byte order. This method offers the best speed for double writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the memory will be written.
	 * @param value The double value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putDouble(int index, double value) {
		putLong(index, Double.doubleToRawLongBits(value));
	}
	
	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putDouble(int, double)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putDouble(int, double)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putDoubleLittleEndian(int index, double value) {
		putLongLittleEndian(index, Double.doubleToRawLongBits(value));
	}
	
	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putDouble(int, double)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putDouble(int, double)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putDoubleBigEndian(int index, double value) {
		putLongBigEndian(index, Double.doubleToRawLongBits(value));
	}
	
	// -------------------------------------------------------------------------
	//                     Bulk Read and Write Methods
	// -------------------------------------------------------------------------

	public abstract void get(DataOutput out, int offset, int length) throws IOException;
	
	/**
	 * Bulk put method. Copies length memory from the given DataInput to the
	 * memory starting at position offset.
	 * 
	 * @param in The DataInput to get the data from.
	 * @param offset The position in the memory segment to copy the chunk to.
	 * @param length The number of bytes to get. 
	 * 
	 * @throws IOException Thrown, if the DataInput encountered a problem upon reading,
	 *                     such as an End-Of-File.
	 */
	public abstract void put(DataInput in, int offset, int length) throws IOException;
	
	/**
	 * Bulk get method. Copies {@code numBytes} bytes from this memory segment, starting at position
	 * {@code offset} to the target {@code ByteBuffer}. The bytes will be put into the target buffer
	 * starting at the buffer's current position. If this method attempts to write more bytes than
	 * the target byte buffer has remaining (with respect to {@link ByteBuffer#remaining()}),
	 * this method will cause a {@link java.nio.BufferOverflowException}.
	 * 
	 * @param offset The position where the bytes are started to be read from in this memory segment.
	 * @param target The ByteBuffer to copy the bytes to.
	 * @param numBytes The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException If the offset is invalid, or this segment does not
	 *           contain the given number of bytes (starting from offset), or the target byte buffer does
	 *           not have enough space for the bytes.
	 */
	public abstract void get(int offset, ByteBuffer target, int numBytes);
	
	/**
	 * Bulk put method. Copies {@code numBytes} bytes from the given {@code ByteBuffer}, into
	 * this memory segment. The bytes will be read from the target buffer
	 * starting at the buffer's current position, and will be written to this memory segment starting
	 * at {@code offset}.
	 * If this method attempts to read more bytes than
	 * the target byte buffer has remaining (with respect to {@link ByteBuffer#remaining()}),
	 * this method will cause a {@link java.nio.BufferUnderflowException}.
	 * 
	 * @param offset The position where the bytes are started to be written to in this memory segment.
	 * @param source The ByteBuffer to copy the bytes from.
	 * @param numBytes The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException If the offset is invalid, or the source buffer does not
	 *           contain the given number of bytes, or this segment does
	 *           not have enough space for the bytes (counting from offset).
	 */
	public abstract void put(int offset, ByteBuffer source, int numBytes);
	
	/**
	 * Bulk copy method. Copies {@code numBytes} bytes from this memory segment, starting at position
	 * {@code offset} to the target memory segment. The bytes will be put into the target segment
	 * starting at position {@code targetOffset}.
	 * 
	 * @param offset The position where the bytes are started to be read from in this memory segment.
	 * @param target The memory segment to copy the bytes to.
	 * @param targetOffset The position in the target memory segment to copy the chunk to.
	 * @param numBytes The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException If either of the offsets is invalid, or the source segment does not
	 *           contain the given number of bytes (starting from offset), or the target segment does
	 *           not have enough space for the bytes (counting from targetOffset).
	 */
	public abstract void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes);
	
	// -------------------------------------------------------------------------
	//                      Comparisons & Swapping
	// -------------------------------------------------------------------------
	
//	public abstract int compare(MemorySegment seg1, MemorySegment seg2, int offset1, int offset2, int len);
//	
//	public abstract void swapBytes(MemorySegment seg1, MemorySegment seg2, byte[] tempBuffer, int offset1, int offset2, int len);
}
