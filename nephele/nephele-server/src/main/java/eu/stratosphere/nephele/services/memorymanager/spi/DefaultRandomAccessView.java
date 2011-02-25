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

package eu.stratosphere.nephele.services.memorymanager.spi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.RandomAccessView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.MemorySegmentDescriptor;

public final class DefaultRandomAccessView extends DefaultMemorySegmentView implements RandomAccessView {
	// --------------------------------------------------------------------
	// Constructors
	// --------------------------------------------------------------------

	public DefaultRandomAccessView(MemorySegmentDescriptor descriptor) {
		super(descriptor);
	}

	// --------------------------------------------------------------------
	// RandomAccessView
	// --------------------------------------------------------------------

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public byte get(int index) {
		if (index >= 0 && index < this.size) {
			return this.memory[this.offset + index];
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView put(int index, byte b) {
		if (index >= 0 && index < this.size) {
			this.memory[this.offset + index] = b;
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView get(int index, byte[] dst) {
		return get(index, dst, 0, dst.length);
	}

	@Override
	public RandomAccessView put(int index, byte[] src) {
		return put(index, src, 0, src.length);
	}

	@Override
	public RandomAccessView get(int index, byte[] dst, int offset, int length) {
		if (index >= 0 && index < this.size && index + length <= this.size && offset + length <= dst.length) {
			System.arraycopy(this.memory, this.offset + index, dst, offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView put(int index, byte[] src, int offset, int length) {
		if (index >= 0 && index < this.size && index + length <= this.size && offset + length <= src.length) {
			System.arraycopy(src, offset, this.memory, this.offset + index, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView get(DataOutput out, int offset, int length) throws IOException {
		if (offset >= 0 && offset < this.size && length >= 0 && length < this.size) {
			out.write(this.memory, this.offset + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView put(DataInput in, int offset, int length) throws IOException {
		if (offset >= 0 && offset < this.size && length >= 0 && length < this.size) {
			in.readFully(this.memory, this.offset + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public boolean getBoolean(int index) {
		if (index >= 0 && index < this.size) {
			return this.memory[this.offset + index] != 0;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putBoolean(int index, boolean value) {
		if (index >= 0 && index < this.size) {
			this.memory[this.offset + index] = (byte) (value ? 1 : 0);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public char getChar(int index) {
		if (index >= 0 && index + 1 < this.size) {
			return (char) (((this.memory[this.offset + index + 0] & 0xff) << 8) | ((this.memory[this.offset
				+ index + 1] & 0xff) << 0));
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putChar(int index, char value) {
		if (index >= 0 && index + 1 < this.size) {
			this.memory[this.offset + index + 0] = (byte) ((value >> 8) & 0xff);
			this.memory[this.offset + index + 1] = (byte) ((value >> 0) & 0xff);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}

	@Override
	public RandomAccessView putDouble(int index, double value) {
		putLong(index, Double.doubleToLongBits(value));
		return this;
	}

	@Override
	public float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}

	@Override
	public RandomAccessView putFloat(int index, float value) {
		putLong(index, Float.floatToIntBits(value));
		return this;
	}

	@Override
	public long getLong(int index) {
		if (index >= 0 && index + 7 < this.size) {
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

	@Override
	public RandomAccessView putLong(int index, long value) {
		if (index >= 0 && index + 7 < this.size) {
			this.memory[this.offset + index + 0] = (byte) ((value >> 56) & 0xff);
			this.memory[this.offset + index + 1] = (byte) ((value >> 48) & 0xff);
			this.memory[this.offset + index + 2] = (byte) ((value >> 40) & 0xff);
			this.memory[this.offset + index + 3] = (byte) ((value >> 32) & 0xff);
			this.memory[this.offset + index + 4] = (byte) ((value >> 24) & 0xff);
			this.memory[this.offset + index + 5] = (byte) ((value >> 16) & 0xff);
			this.memory[this.offset + index + 6] = (byte) ((value >> 8) & 0xff);
			this.memory[this.offset + index + 7] = (byte) (value & 0xff);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public int getInt(int index) {
		if (index >= 0 && index + 3 < this.size) {
			return (((int) this.memory[this.offset + index + 0] & 0xff) << 24)
				| (((int) this.memory[this.offset + index + 1] & 0xff) << 16)
				| (((int) this.memory[this.offset + index + 2] & 0xff) << 8)
				| (((int) this.memory[this.offset + index + 3] & 0xff) << 0);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putInt(int index, int value) {
		if (index >= 0 && index + 3 < this.size) {
			this.memory[this.offset + index + 0] = (byte) ((value >> 24) & 0xff);
			this.memory[this.offset + index + 1] = (byte) ((value >> 16) & 0xff);
			this.memory[this.offset + index + 2] = (byte) ((value >> 8) & 0xff);
			this.memory[this.offset + index + 3] = (byte) ((value >> 0) & 0xff);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public short getShort(int index) {
		if (index >= 0 && index + 1 < this.size) {
			return (short) (((this.memory[this.offset + index + 0] & 0xff) << 8) | ((this.memory[this.offset
				+ index + 1] & 0xff) << 0));
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putShort(int index, short value) {
		if (index >= 0 && index + 1 < this.size) {
			this.memory[this.offset + index + 0] = (byte) ((value >> 8) & 0xff);
			this.memory[this.offset + index + 1] = (byte) ((value >> 0) & 0xff);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.api.RandomAccessView#getBackingArray()
	 */
	@Override
	public byte[] getBackingArray() {
		return this.memory;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.api.RandomAccessView#translateOffset(int)
	 */
	@Override
	public int translateOffset(int offset) {
		return this.offset + offset;
	}
}
