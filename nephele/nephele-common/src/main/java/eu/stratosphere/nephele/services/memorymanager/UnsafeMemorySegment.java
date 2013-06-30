/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.memorymanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class represents a piece of memory allocated from the memory manager. The segment is backed
 * by a byte array and features random put and get methods for the basic types that are stored in a byte-wise
 * fashion in the memory.
 */
public class UnsafeMemorySegment {
	
	// flag to enable / disable boundary checks. Note that the compiler eliminates the check code
	// paths (as dead code) when this constant is set to false.
	private static final boolean CHECKED = true;
	
	/**
	 * The array in which the data is stored.
	 */
	protected byte[] memory;
	
	/**
	 * Wrapper for I/O requests.
	 */
	protected ByteBuffer wrapper;
	
	// -------------------------------------------------------------------------
	//                             Constructors
	// -------------------------------------------------------------------------

	/**
	 * Creates a new memory segment of given size with the provided views.
	 * 
	 * @param size The size of the memory segment.
	 * @param inputView The input view to use.
	 * @param outputView The output view to use.
	 */
	public UnsafeMemorySegment(byte[] memory) {
		this.memory = memory;
	}

	// -------------------------------------------------------------------------
	//                        MemorySegment Accessors
	// -------------------------------------------------------------------------
	
	/**
	 * Checks whether this memory segment has already been freed. In that case, the
	 * segment must not be used any more.
	 * 
	 * @return True, if the segment has been freed, false otherwise.
	 */
	public final boolean isFreed() {
		return this.memory == null;
	}
	
	/**
	 * Gets the size of the memory segment, in bytes. Because segments
	 * are backed by arrays, they cannot be larger than two GiBytes.
	 * 
	 * @return The size in bytes.
	 */
	public final int size() {
		return this.memory.length;
	}
	
	/**
	 * Gets the byte array that backs the memory segment and this random access view.
	 * Since different regions of the backing array are used by different segments, the logical
	 * positions in this view do not correspond to the indexes in the backing array and need
	 * to be translated via the {@link #translateOffset(int)} method.
	 * 
	 * @return The backing byte array.
	 */
	@Deprecated
	public final byte[] getBackingArray() {
		return this.memory;
	}

	/**
	 * Translates the given offset for this view into the offset for the backing array.
	 * 
	 * @param offset The offset to be translated.
	 * @return The corresponding position in the backing array.
	 */
	@Deprecated
	public final int translateOffset(int offset) {
		return offset;
	}
	
	// -------------------------------------------------------------------------
	//                       Helper methods
	// -------------------------------------------------------------------------
	

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
	public ByteBuffer wrap(int offset, int length) {
		if (offset > this.memory.length || offset > this.memory.length - length) {
			throw new IndexOutOfBoundsException();
		}
		
		if (this.wrapper == null) {
			this.wrapper = ByteBuffer.wrap(this.memory, offset, length);
		}
		else {
			this.wrapper.position(offset);
			this.wrapper.limit(offset + length);
		}
		
		return this.wrapper;
	}


	// --------------------------------------------------------------------
	//                            Random Access
	// --------------------------------------------------------------------

	// ------------------------------------------------------------------------------------------------------
	// WARNING: Any code for range checking must take care to avoid integer overflows. The position
	// integer may go up to <code>Integer.MAX_VALUE</tt>. Range checks that work after the principle
	// <code>position + 3 &lt; end</code> may fail because <code>position + 3</code> becomes negative.
	// A safe solution is to subtract the delta from the limit, for example
	// <code>position &lt; end - 3</code>. Since all indices are always positive, and the integer domain
	// has one more negative value than positive values, this can never cause an underflow.
	// ------------------------------------------------------------------------------------------------------


	/**
	 * Reads the byte at the given position.
	 * 
	 * @param position The position from which the byte will be read
	 * @return The byte at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public final byte get(int index) {
		return this.memory[index];
	}

	/**
	 * Writes the given byte into this buffer at the given position.
	 * 
	 * @param position The position at which the byte will be written.
	 * @param b The byte value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public final void put(int index, byte b) {
		this.memory[index] = b;
	}

	/**
	 * Bulk get method. Copies dst.length memory from the specified position to
	 * the destination memory.
	 * 
	 * @param position The position at which the first byte will be read.
	 * @param dst The memory into which the memory will be copied.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the data between the 
	 *                                   index and the memory segment end is not enough to fill the destination array.
	 */
	public final void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}

	/**
	 * Bulk put method. Copies src.length memory from the source memory into the
	 * memory segment beginning at the specified position.
	 * 
	 * @param index The position in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * @return This random access view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   size exceed the amount of memory between the index and the memory
	 *                                   segment's end. 
	 */
	public final void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}

	/**
	 * Bulk get method. Copies length memory from the specified position to the
	 * destination memory, beginning at the given offset
	 * 
	 * @param position
	 *        The position at which the first byte will be read.
	 * @param dst
	 *        The memory into which the memory will be copied.
	 * @param offset
	 *        The copying offset in the destination memory.
	 * @param length
	 *        The number of bytes to be copied.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the requested number of 
	 *                                   bytes exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	public final void get(int index, byte[] dst, int offset, int length) {
		System.arraycopy(this.memory, index, dst, offset, length);
	}

	/**
	 * Bulk put method. Copies length memory starting at position offset from
	 * the source memory into the memory segment starting at the specified
	 * index.
	 * 
	 * @param index The position in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * @param offset The offset in the source array where the copying is started.
	 * @param length The number of bytes to copy.
	 * @return This random access view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   portion to copy exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	public final void put(int index, byte[] src, int offset, int length) {
		System.arraycopy(src, offset, this.memory, index, length);
	}

	/**
	 * Bulk get method. Copies length memory from the specified offset to the
	 * provided <tt>DataOutput</tt>.
	 * 
	 * @param out The data output object to copy the data to.
	 * @param offset The first byte to by copied.
	 * @param length The number of bytes to copy.
	 * @return This view itself.
	 * 
	 * @throws IOException Thrown, if the DataOutput encountered a problem upon writing.
	 */
	public final void get(DataOutput out, int offset, int length) throws IOException {
		out.write(this.memory, offset, length);
	}

	/**
	 * Bulk put method. Copies length memory from the given DataInput to the
	 * memory starting at position offset.
	 * 
	 * @param in The DataInput to get the data from.
	 * @param offset The position in the memory segment to copy the chunk to.
	 * @param length The number of bytes to get. 
	 * @return This random access view itself.
	 * 
	 * @throws IOException Thrown, if the DataInput encountered a problem upon reading,
	 *                     such as an End-Of-File.
	 */
	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}

	/**
	 * Reads one byte at the given position and returns its boolean
	 * representation.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The char value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	public final boolean getBoolean(int index) {
		return this.memory[index] != 0;
	}

	/**
	 * Writes one byte containing the byte value into this buffer at the given
	 * position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The char value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	public final void putBoolean(int index, boolean value) {
		this.memory[index] = (byte) (value ? 1 : 0);
	}

	/**
	 * Reads two memory at the given position, composing them into a char value
	 * according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The char value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final char getChar(int index) {
		return (char) ( ((this.memory[index    ] & 0xff) << 8) | 
		                 (this.memory[index + 1] & 0xff) );
	}

	/**
	 * Writes two memory containing the given char value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The char value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final void putChar(int index, char value) {
		this.memory[index    ] = (byte) (value >> 8);
		this.memory[index + 1] = (byte) value;
	}

	/**
	 * Reads two memory at the given position, composing them into a short value
	 * according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The short value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final short getShort(int index) {
		return (short) (
				((this.memory[index    ] & 0xff) << 8) |
				((this.memory[index + 1] & 0xff)) );
	}

	/**
	 * Writes the given short value into this buffer at the given position, using
	 * the native byte order of the system.
	 * 
	 * @param position The position at which the value will be written.
	 * @param value The short value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final void putShort(int index, short value) {
		this.memory[index    ] = (byte) (value >> 8);
		this.memory[index + 1] = (byte) value;
	}
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in the system's native byte order.
	 * This method offers the best speed for integer reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param position The position from which the value will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	@SuppressWarnings("restriction")
	public final int getInt(int index) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 4) {
				return UNSAFE.getInt(this.memory, BASE_OFFSET + index);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			return UNSAFE.getInt(this.memory, BASE_OFFSET + index);
		}
	}
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in little endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getInt(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getInt(int)} is the preferable choice.
	 * 
	 * @param position The position from which the value will be read.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position at which the value will be written.
	 * @param value The int value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	@SuppressWarnings("restriction")
	public final void putInt(int index, int value) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 4) {
				UNSAFE.putInt(this.memory, BASE_OFFSET + index, value);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			UNSAFE.putInt(this.memory, BASE_OFFSET + index, value);
		}
	}
	
	/**
	 * Writes the given int value (32bit, 4 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putInt(int, int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putInt(int, int)} is the preferable choice.
	 * 
	 * @param position The position at which the value will be written.
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
	 * @param position The position at which the value will be written.
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
	 * @param position The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	@SuppressWarnings("restriction")
	public final long getLong(int index) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 8) {
				return UNSAFE.getLong(this.memory, BASE_OFFSET + index);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			return UNSAFE.getLong(this.memory, BASE_OFFSET + index);
		}
	}
	
	/**
	 * Reads a long integer value (64bit, 8 bytes) from the given position, in little endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getLong(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getLong(int)} is the preferable choice.
	 * 
	 * @param position The position from which the value will be read.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	@SuppressWarnings("restriction")
	public final void putLong(int index, long value) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 8) {
				UNSAFE.putLong(this.memory, BASE_OFFSET + index, value);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			UNSAFE.putLong(this.memory, BASE_OFFSET + index, value);
		}
	}
	
	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putLong(int, long)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putLong(int, long)} is the preferable choice.
	 * 
	 * @param position The position at which the value will be written.
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
	 * @param position The position at which the value will be written.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position at which the value will be written.
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
	 * @param position The position at which the value will be written.
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
	 * @param position The position at which the value will be written.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position from which the value will be read.
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
	 * @param position The position at which the memory will be written.
	 * @param value The double value to be written.
	 * @return This view itself.
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
	 * @param position The position at which the value will be written.
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
	 * @param position The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putDoubleBigEndian(int index, double value) {
		putLongBigEndian(index, Double.doubleToRawLongBits(value));
	}
	
	// --------------------------------------------------------------------------------------------
	// Utilities for native memory accesses and checks
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	
	@SuppressWarnings("restriction")
	private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	
	private static final boolean LITTLE_ENDIAN = (MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN);
}