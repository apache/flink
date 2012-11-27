/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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


/**
 * This class represents a piece of memory allocated from the memory manager. The segment is backed
 * by a byte array and features random put and get methods for the basic types that are stored in a byte-wise
 * fashion in the memory.
 *
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public class MemorySegment
{
	public static final int POINTER_LEN = 5;
	
	/**
	 * The array in which the data is stored.
	 */
	protected byte[] memory;
	
	/**
	 * The offset in the memory array where this segment starts.
	 */
	protected final int offset;
	
	/**
	 * The size of the memory segment.
	 */
	protected final int size;
	
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
	public MemorySegment(byte[] memory, int offset, int size)
	{		
		this.memory = memory;
		this.offset = offset;
		this.size = size;
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
	public boolean isFreed() {
		return this.memory == null;
	}
	
	/**
	 * Gets the size of the memory segment, in bytes. Because segments
	 * are backed by arrays, they cannot be larger than two GiBytes.
	 * 
	 * @return The size in bytes.
	 */
	public final int size() {
		return size;
	}
	
	/**
	 * Gets the byte array that backs the memory segment and this random access view.
	 * Since different regions of the backing array are used by different segments, the logical
	 * positions in this view do not correspond to the indexes in the backing array and need
	 * to be translated via the {@link #translateOffset(int)} method.
	 * 
	 * @return The backing byte array.
	 */
	public final byte[] getBackingArray() {
		return this.memory;
	}

	/**
	 * Translates the given offset for this view into the offset for the backing array.
	 * 
	 * @param offset The offset to be translated.
	 * @return The corresponding position in the backing array.
	 */
	public final int translateOffset(int offset) {
		return this.offset + offset;
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
		if (offset > this.size || offset > this.size - length) {
			throw new IndexOutOfBoundsException();
		}
		
		if (this.wrapper == null) {
			this.wrapper = ByteBuffer.wrap(this.memory, this.offset + offset, length);
		}
		else {
			this.wrapper.position(this.offset + offset);
			this.wrapper.limit(this.offset + offset + length);
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
		if (index >= 0 && index < this.size) {
			return this.memory[this.offset + index];
		} else {
			throw new IndexOutOfBoundsException();
		}
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
	public final MemorySegment put(int index, byte b) {
		if (index >= 0 && index < this.size) {
			this.memory[this.offset + index] = b;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
	public final MemorySegment get(int index, byte[] dst) {
		return get(index, dst, 0, dst.length);
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
	public final MemorySegment put(int index, byte[] src) {
		return put(index, src, 0, src.length);
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
	public final MemorySegment get(int index, byte[] dst, int offset, int length) {
		if (index >= 0 && index < this.size && index <= this.size - length && offset <= dst.length - length) {
			System.arraycopy(this.memory, this.offset + index, dst, offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
	public final MemorySegment put(int index, byte[] src, int offset, int length) {
		if (index >= 0 && index < this.size && index <= this.size - length && offset <= src.length - length) {
			System.arraycopy(src, offset, this.memory, this.offset + index, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
	public final MemorySegment get(DataOutput out, int offset, int length) throws IOException {
		if (offset >= 0 && offset < this.size && length >= 0 && offset <= this.size - length) {
			out.write(this.memory, this.offset + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
	public final MemorySegment put(DataInput in, int offset, int length) throws IOException {
		if (offset >= 0 && offset < this.size && length >= 0 && offset <= this.size - length) {
			in.readFully(this.memory, this.offset + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
		if (index >= 0 && index < this.size) {
			return this.memory[this.offset + index] != 0;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
	public final MemorySegment putBoolean(int index, boolean value) {
		if (index >= 0 && index < this.size) {
			this.memory[this.offset + index] = (byte) (value ? 1 : 0);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
		if (index >= 0 && index < this.size - 1) {
			return (char) ( ((this.memory[this.offset + index + 0] & 0xff) << 8) | 
					         (this.memory[this.offset + index + 1] & 0xff) );
		} else {
			throw new IndexOutOfBoundsException();
		}
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
	public final MemorySegment putChar(int index, char value) {
		if (index >= 0 && index < this.size - 1) {
			this.memory[this.offset + index + 0] = (byte) (value >> 8);
			this.memory[this.offset + index + 1] = (byte) value;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
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
		if (index >= 0 && index < this.size - 1) {
			return (short) (
					((this.memory[this.offset + index + 0] & 0xff) << 8) |
					((this.memory[this.offset + index + 1] & 0xff)) );
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Writes two memory containing the given short value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The short value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final MemorySegment putShort(int index, short value) {
		if (index >= 0 && index < this.size - 1) {
			this.memory[this.offset + index + 0] = (byte) (value >> 8);
			this.memory[this.offset + index + 1] = (byte) value;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	/**
	 * Reads four memory at the given position, composing them into a int value
	 * according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final int getInt(int index) {
		if (index >= 0 && index < this.size - 3) {
			return ((this.memory[this.offset + index + 0] & 0xff) << 24)
				| ((this.memory[this.offset + index + 1] & 0xff) << 16)
				| ((this.memory[this.offset + index + 2] & 0xff) << 8)
				| ((this.memory[this.offset + index + 3] & 0xff) << 0);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Writes four memory containing the given int value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The int value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final MemorySegment putInt(int index, int value) {
		if (index >= 0 && index < this.size - 3) {
			this.memory[this.offset + index + 0] = (byte) (value >> 24);
			this.memory[this.offset + index + 1] = (byte) (value >> 16);
			this.memory[this.offset + index + 2] = (byte) (value >> 8);
			this.memory[this.offset + index + 3] = (byte) value;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	/**
	 * Reads eight memory at the given position, composing them into a long
	 * value according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final long getLong(int index) {
		if (index >= 0 && index < this.size - 7) {
			return (((long) this.memory[this.offset + index + 0] & 0xff) << 56)
				| (((long) this.memory[this.offset + index + 1] & 0xff) << 48)
				| (((long) this.memory[this.offset + index + 2] & 0xff) << 40)
				| (((long) this.memory[this.offset + index + 3] & 0xff) << 32)
				| (((long) this.memory[this.offset + index + 4] & 0xff) << 24)
				| (((long) this.memory[this.offset + index + 5] & 0xff) << 16)
				| (((long) this.memory[this.offset + index + 6] & 0xff) << 8)
				| (((long) this.memory[this.offset + index + 7] & 0xff) << 0);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Writes eight memory containing the given long value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The long value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final MemorySegment putLong(int index, long value) {
		if (index >= 0 && index < this.size - 7) {
			this.memory[this.offset + index + 0] = (byte) (value >> 56);
			this.memory[this.offset + index + 1] = (byte) (value >> 48);
			this.memory[this.offset + index + 2] = (byte) (value >> 40);
			this.memory[this.offset + index + 3] = (byte) (value >> 32);
			this.memory[this.offset + index + 4] = (byte) (value >> 24);
			this.memory[this.offset + index + 5] = (byte) (value >> 16);
			this.memory[this.offset + index + 6] = (byte) (value >> 8);
			this.memory[this.offset + index + 7] = (byte) value;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final long getPointer(int index) {
		if (index >= 0 && index < this.size - 4) {
			return (((long) this.memory[this.offset + index + 0] & 0xff) << 32)
				| (((long) this.memory[this.offset + index + 1] & 0xff) << 24)
				| (((long) this.memory[this.offset + index + 2] & 0xff) << 16)
				| (((long) this.memory[this.offset + index + 3] & 0xff) << 8)
				| (((long) this.memory[this.offset + index + 4] & 0xff) << 0);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final MemorySegment putPointer(int index, long pointer) {
		if (index >= 0 && index < this.size - 4) {
			this.memory[this.offset + index + 0] = (byte) (pointer >> 32);
			this.memory[this.offset + index + 1] = (byte) (pointer >> 24);
			this.memory[this.offset + index + 2] = (byte) (pointer >> 16);
			this.memory[this.offset + index + 3] = (byte) (pointer >> 8);
			this.memory[this.offset + index + 4] = (byte) pointer;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	/**
	 * Reads four memory at the given position, composing them into a float
	 * value according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The float value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}

	/**
	 * Writes four memory containing the given float value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The float value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final MemorySegment putFloat(int index, float value) {
		putLong(index, Float.floatToIntBits(value));
		return this;
	}
	
	/**
	 * Reads eight memory at the given position, composing them into a double
	 * value according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The double value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}

	/**
	 * Writes eight memory containing the given double value, in the current
	 * byte order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The double value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final MemorySegment putDouble(int index, double value) {
		putLong(index, Double.doubleToLongBits(value));
		return this;
	}
}
