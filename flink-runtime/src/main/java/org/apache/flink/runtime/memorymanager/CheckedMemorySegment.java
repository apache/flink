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


package org.apache.flink.runtime.memorymanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class represents a piece of memory allocated from the memory manager. The segment is backed
 * by a byte array and features random put and get methods for the basic types that are stored in a byte-wise
 * fashion in the memory.
 */
public class CheckedMemorySegment {
	
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

	public CheckedMemorySegment(byte[] memory) {
		this.memory = memory;
		this.offset = 0;
		this.size = memory.length;
	}

	// -------------------------------------------------------------------------
	//                        MemorySegment Accessors
	// -------------------------------------------------------------------------

	public boolean isFreed() {
		return this.memory == null;
	}
	
	public final int size() {
		return size;
	}

	public final byte[] getBackingArray() {
		return this.memory;
	}

	public final int translateOffset(int offset) {
		return this.offset + offset;
	}
	
	// -------------------------------------------------------------------------
	//                       Helper methods
	// -------------------------------------------------------------------------

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

	public final byte get(int index) {
		if (index >= 0 && index < this.size) {
			return this.memory[this.offset + index];
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment put(int index, byte b) {
		if (index >= 0 && index < this.size) {
			this.memory[this.offset + index] = b;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment get(int index, byte[] dst) {
		return get(index, dst, 0, dst.length);
	}

	public final CheckedMemorySegment put(int index, byte[] src) {
		return put(index, src, 0, src.length);
	}

	public final CheckedMemorySegment get(int index, byte[] dst, int offset, int length) {
		if (index >= 0 && index < this.size && index <= this.size - length && offset <= dst.length - length) {
			System.arraycopy(this.memory, this.offset + index, dst, offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment put(int index, byte[] src, int offset, int length) {
		if (index >= 0 && index < this.size && index <= this.size - length && offset <= src.length - length) {
			System.arraycopy(src, offset, this.memory, this.offset + index, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment get(DataOutput out, int offset, int length) throws IOException {
		if (offset >= 0 && offset < this.size && length >= 0 && offset <= this.size - length) {
			out.write(this.memory, this.offset + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment put(DataInput in, int offset, int length) throws IOException {
		if (offset >= 0 && offset < this.size && length >= 0 && offset <= this.size - length) {
			in.readFully(this.memory, this.offset + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final boolean getBoolean(int index) {
		if (index >= 0 && index < this.size) {
			return this.memory[this.offset + index] != 0;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment putBoolean(int index, boolean value) {
		if (index >= 0 && index < this.size) {
			this.memory[this.offset + index] = (byte) (value ? 1 : 0);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final char getChar(int index) {
		if (index >= 0 && index < this.size - 1) {
			return (char) ( ((this.memory[this.offset + index + 0] & 0xff) << 8) | 
							(this.memory[this.offset + index + 1] & 0xff) );
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment putChar(int index, char value) {
		if (index >= 0 && index < this.size - 1) {
			this.memory[this.offset + index + 0] = (byte) (value >> 8);
			this.memory[this.offset + index + 1] = (byte) value;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final short getShort(int index) {
		if (index >= 0 && index < this.size - 1) {
			return (short) (
					((this.memory[this.offset + index + 0] & 0xff) << 8) |
					((this.memory[this.offset + index + 1] & 0xff)) );
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final CheckedMemorySegment putShort(int index, short value) {
		if (index >= 0 && index < this.size - 1) {
			this.memory[this.offset + index + 0] = (byte) (value >> 8);
			this.memory[this.offset + index + 1] = (byte) value;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final int getInt(int index) {
		if (index >= 0 && index < this.size - 3) {
			return ((this.memory[index    ] & 0xff) << 24)
				| ((this.memory[index + 1] & 0xff) << 16)
				| ((this.memory[index + 2] & 0xff) <<  8)
				| ((this.memory[index + 3] & 0xff)     );
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final int getIntLittleEndian(int index) {
		if (index >= 0 && index < this.size - 3) {
			return ((this.memory[index    ] & 0xff)      )
				| ((this.memory[index + 1] & 0xff) <<  8)
				| ((this.memory[index + 2] & 0xff) << 16)
				| ((this.memory[index + 3] & 0xff) << 24);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final int getIntBigEndian(int index) {
		if (index >= 0 && index < this.size - 3) {
			return ((this.memory[index    ] & 0xff) << 24)
				| ((this.memory[index + 1] & 0xff) << 16)
				| ((this.memory[index + 2] & 0xff) <<  8)
				| ((this.memory[index + 3] & 0xff)      );
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final void putInt(int index, int value) {
		if (index >= 0 && index < this.size - 3) {
			this.memory[index    ] = (byte) (value >> 24);
			this.memory[index + 1] = (byte) (value >> 16);
			this.memory[index + 2] = (byte) (value >> 8);
			this.memory[index + 3] = (byte) value;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final void putIntLittleEndian(int index, int value) {
		if (index >= 0 && index < this.size - 3) {
			this.memory[index    ] = (byte) value;
			this.memory[index + 1] = (byte) (value >>  8);
			this.memory[index + 2] = (byte) (value >> 16);
			this.memory[index + 3] = (byte) (value >> 24);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final void putIntBigEndian(int index, int value) {
		if (index >= 0 && index < this.size - 3) {
			this.memory[index    ] = (byte) (value >> 24);
			this.memory[index + 1] = (byte) (value >> 16);
			this.memory[index + 2] = (byte) (value >> 8);
			this.memory[index + 3] = (byte) value;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final long getLong(int index) {
		if (index >= 0 && index < this.size - 7) {
			return (((long) this.memory[index    ] & 0xff) << 56)
				| (((long) this.memory[index + 1] & 0xff) << 48)
				| (((long) this.memory[index + 2] & 0xff) << 40)
				| (((long) this.memory[index + 3] & 0xff) << 32)
				| (((long) this.memory[index + 4] & 0xff) << 24)
				| (((long) this.memory[index + 5] & 0xff) << 16)
				| (((long) this.memory[index + 6] & 0xff) <<  8)
				| (((long) this.memory[index + 7] & 0xff)      );
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	public final long getLongLittleEndian(int index) {
		if (index >= 0 && index < this.size - 7) {
			return (((long) this.memory[index    ] & 0xff)      )
				| (((long) this.memory[index + 1] & 0xff) <<  8)
				| (((long) this.memory[index + 2] & 0xff) << 16)
				| (((long) this.memory[index + 3] & 0xff) << 24)
				| (((long) this.memory[index + 4] & 0xff) << 32)
				| (((long) this.memory[index + 5] & 0xff) << 40)
				| (((long) this.memory[index + 6] & 0xff) << 48)
				| (((long) this.memory[index + 7] & 0xff) << 56);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final long getLongBigEndian(int index) {
		if (index >= 0 && index < this.size - 7) {
			return (((long) this.memory[index    ] & 0xff) << 56)
				| (((long) this.memory[index + 1] & 0xff) << 48)
				| (((long) this.memory[index + 2] & 0xff) << 40)
				| (((long) this.memory[index + 3] & 0xff) << 32)
				| (((long) this.memory[index + 4] & 0xff) << 24)
				| (((long) this.memory[index + 5] & 0xff) << 16)
				| (((long) this.memory[index + 6] & 0xff) <<  8)
				| (((long) this.memory[index + 7] & 0xff)      );
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final void putLong(int index, long value) {
		if (index >= 0 && index < this.size - 7) {
			this.memory[index    ] = (byte) (value >> 56);
			this.memory[index + 1] = (byte) (value >> 48);
			this.memory[index + 2] = (byte) (value >> 40);
			this.memory[index + 3] = (byte) (value >> 32);
			this.memory[index + 4] = (byte) (value >> 24);
			this.memory[index + 5] = (byte) (value >> 16);
			this.memory[index + 6] = (byte) (value >>  8);
			this.memory[index + 7] = (byte)  value;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final void putLongLittleEndian(int index, long value) {
		if (index >= 0 && index < this.size - 7) {
			this.memory[index    ] = (byte)  value;
			this.memory[index + 1] = (byte) (value >>  8);
			this.memory[index + 2] = (byte) (value >> 16);
			this.memory[index + 3] = (byte) (value >> 24);
			this.memory[index + 4] = (byte) (value >> 32);
			this.memory[index + 5] = (byte) (value >> 40);
			this.memory[index + 6] = (byte) (value >> 48);
			this.memory[index + 7] = (byte) (value >> 56);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final void putLongBigEndian(int index, long value) {
		if (index >= 0 && index < this.size - 7) {
			this.memory[index    ] = (byte) (value >> 56);
			this.memory[index + 1] = (byte) (value >> 48);
			this.memory[index + 2] = (byte) (value >> 40);
			this.memory[index + 3] = (byte) (value >> 32);
			this.memory[index + 4] = (byte) (value >> 24);
			this.memory[index + 5] = (byte) (value >> 16);
			this.memory[index + 6] = (byte) (value >>  8);
			this.memory[index + 7] = (byte)  value;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	public final float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}

	public final CheckedMemorySegment putFloat(int index, float value) {
		putLong(index, Float.floatToIntBits(value));
		return this;
	}

	public final double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}

	public final CheckedMemorySegment putDouble(int index, double value) {
		putLong(index, Double.doubleToLongBits(value));
		return this;
	}
}
