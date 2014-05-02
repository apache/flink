/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
 */
public class DirectMemorySegment {
	
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
	public DirectMemorySegment(byte[] memory) {
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
		System.arraycopy(this.memory, index, dst, offset, length);
	}

	public final void put(int index, byte[] src, int offset, int length) {
		System.arraycopy(src, offset, this.memory, index, length);
	}

	public final void get(DataOutput out, int offset, int length) throws IOException {
		out.write(this.memory, offset, length);
	}

	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}

	public final boolean getBoolean(int index) {
		return this.memory[index] != 0;
	}

	public final void putBoolean(int index, boolean value) {
		this.memory[index] = (byte) (value ? 1 : 0);
	}

	public final char getChar(int index) {
		return (char) ( ((this.memory[index    ] & 0xff) << 8) | 
						(this.memory[index + 1] & 0xff) );
	}

	public final void putChar(int index, char value) {
		this.memory[index    ] = (byte) (value >> 8);
		this.memory[index + 1] = (byte) value;
	}

	public final short getShort(int index) {
		return (short) (
				((this.memory[index    ] & 0xff) << 8) |
				((this.memory[index + 1] & 0xff)) );
	}

	public final void putShort(int index, short value) {
		this.memory[index    ] = (byte) (value >> 8);
		this.memory[index + 1] = (byte) value;
	}
	
	public final int getInt(int index) {
		return ((this.memory[index    ] & 0xff) << 24)
			| ((this.memory[index + 1] & 0xff) << 16)
			| ((this.memory[index + 2] & 0xff) <<  8)
			| ((this.memory[index + 3] & 0xff)     );
	}

	public final int getIntLittleEndian(int index) {
		return ((this.memory[index    ] & 0xff)      )
			| ((this.memory[index + 1] & 0xff) <<  8)
			| ((this.memory[index + 2] & 0xff) << 16)
			| ((this.memory[index + 3] & 0xff) << 24);
	}
	
	public final int getIntBigEndian(int index) {
		return ((this.memory[index    ] & 0xff) << 24)
			| ((this.memory[index + 1] & 0xff) << 16)
			| ((this.memory[index + 2] & 0xff) <<  8)
			| ((this.memory[index + 3] & 0xff)      );
	}
	
	public final void putInt(int index, int value) {
		this.memory[index    ] = (byte) (value >> 24);
		this.memory[index + 1] = (byte) (value >> 16);
		this.memory[index + 2] = (byte) (value >> 8);
		this.memory[index + 3] = (byte) value;
	}
	
	public final void putIntLittleEndian(int index, int value) {
		this.memory[index    ] = (byte) value;
		this.memory[index + 1] = (byte) (value >>  8);
		this.memory[index + 2] = (byte) (value >> 16);
		this.memory[index + 3] = (byte) (value >> 24);
	}
	
	public final void putIntBigEndian(int index, int value) {
		this.memory[index    ] = (byte) (value >> 24);
		this.memory[index + 1] = (byte) (value >> 16);
		this.memory[index + 2] = (byte) (value >> 8);
		this.memory[index + 3] = (byte) value;
	}
	
	public final long getLong(int index) {
		return (((long) this.memory[index    ] & 0xff) << 56)
			| (((long) this.memory[index + 1] & 0xff) << 48)
			| (((long) this.memory[index + 2] & 0xff) << 40)
			| (((long) this.memory[index + 3] & 0xff) << 32)
			| (((long) this.memory[index + 4] & 0xff) << 24)
			| (((long) this.memory[index + 5] & 0xff) << 16)
			| (((long) this.memory[index + 6] & 0xff) <<  8)
			| (((long) this.memory[index + 7] & 0xff)      );
	}
	public final long getLongLittleEndian(int index) {
		return (((long) this.memory[index    ] & 0xff)      )
			| (((long) this.memory[index + 1] & 0xff) <<  8)
			| (((long) this.memory[index + 2] & 0xff) << 16)
			| (((long) this.memory[index + 3] & 0xff) << 24)
			| (((long) this.memory[index + 4] & 0xff) << 32)
			| (((long) this.memory[index + 5] & 0xff) << 40)
			| (((long) this.memory[index + 6] & 0xff) << 48)
			| (((long) this.memory[index + 7] & 0xff) << 56);
	}
	
	public final long getLongBigEndian(int index) {
		return (((long) this.memory[index    ] & 0xff) << 56)
			| (((long) this.memory[index + 1] & 0xff) << 48)
			| (((long) this.memory[index + 2] & 0xff) << 40)
			| (((long) this.memory[index + 3] & 0xff) << 32)
			| (((long) this.memory[index + 4] & 0xff) << 24)
			| (((long) this.memory[index + 5] & 0xff) << 16)
			| (((long) this.memory[index + 6] & 0xff) <<  8)
			| (((long) this.memory[index + 7] & 0xff)      );
	}

	public final void putLong(int index, long value) {
		this.memory[index    ] = (byte) (value >> 56);
		this.memory[index + 1] = (byte) (value >> 48);
		this.memory[index + 2] = (byte) (value >> 40);
		this.memory[index + 3] = (byte) (value >> 32);
		this.memory[index + 4] = (byte) (value >> 24);
		this.memory[index + 5] = (byte) (value >> 16);
		this.memory[index + 6] = (byte) (value >>  8);
		this.memory[index + 7] = (byte)  value;
	}
	
	public final void putLongLittleEndian(int index, long value) {
		this.memory[index    ] = (byte)  value;
		this.memory[index + 1] = (byte) (value >>  8);
		this.memory[index + 2] = (byte) (value >> 16);
		this.memory[index + 3] = (byte) (value >> 24);
		this.memory[index + 4] = (byte) (value >> 32);
		this.memory[index + 5] = (byte) (value >> 40);
		this.memory[index + 6] = (byte) (value >> 48);
		this.memory[index + 7] = (byte) (value >> 56);
	}
	
	public final void putLongBigEndian(int index, long value) {
		this.memory[index    ] = (byte) (value >> 56);
		this.memory[index + 1] = (byte) (value >> 48);
		this.memory[index + 2] = (byte) (value >> 40);
		this.memory[index + 3] = (byte) (value >> 32);
		this.memory[index + 4] = (byte) (value >> 24);
		this.memory[index + 5] = (byte) (value >> 16);
		this.memory[index + 6] = (byte) (value >>  8);
		this.memory[index + 7] = (byte)  value;
	}
	
	public final float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}

	public final void putFloat(int index, float value) {
		putLong(index, Float.floatToIntBits(value));
	}
	
	public final double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}

	public final void putDouble(int index, double value) {
		putLong(index, Double.doubleToLongBits(value));
	}
}
