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

public class DefaultRandomAccessView extends DefaultMemorySegmentView implements RandomAccessView {
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
		return descriptorReference.get().size;
	}

	@Override
	public byte get(int index) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index < descriptor.size) {
			return descriptor.memory[descriptor.start + index];
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView put(int index, byte b) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index < descriptor.size) {
			descriptor.memory[descriptor.start + index] = b;
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
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index < descriptor.size && index + length <= descriptor.size && offset + length <= dst.length) {
			System.arraycopy(descriptor.memory, descriptor.start + index, dst, offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView put(int index, byte[] src, int offset, int length) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index < descriptor.size && index + length <= descriptor.size && offset + length <= src.length) {
			System.arraycopy(src, offset, descriptor.memory, descriptor.start + index, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView get(DataOutput out, int offset, int length) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (offset >= 0 && offset < descriptor.size && length >= 0 && length < descriptor.size) {
			out.write(descriptor.memory, descriptor.start + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView put(DataInput in, int offset, int length) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (offset >= 0 && offset < descriptor.size && length >= 0 && length < descriptor.size) {
			in.readFully(descriptor.memory, descriptor.start + offset, length);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public boolean getBoolean(int index) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index < descriptor.size) {
			return descriptor.memory[descriptor.start + index] != 0;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putBoolean(int index, boolean value) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index < descriptor.size) {
			descriptor.memory[descriptor.start + index] = (byte) (value ? 1 : 0);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public char getChar(int index) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 1 < descriptor.size) {
			return (char) (((descriptor.memory[descriptor.start + index + 0] & 0xff) << 8) | ((descriptor.memory[descriptor.start
				+ index + 1] & 0xff) << 0));
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putChar(int index, char value) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 1 < descriptor.size) {
			descriptor.memory[descriptor.start + index + 0] = (byte) ((value >> 8) & 0xff);
			descriptor.memory[descriptor.start + index + 1] = (byte) ((value >> 0) & 0xff);
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
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 7 < descriptor.size) {
			return (((long) descriptor.memory[descriptor.start + index + 0] & 0xff) << 56)
				| (((long) descriptor.memory[descriptor.start + index + 1] & 0xff) << 48)
				| (((long) descriptor.memory[descriptor.start + index + 2] & 0xff) << 40)
				| (((long) descriptor.memory[descriptor.start + index + 3] & 0xff) << 32)
				| (((long) descriptor.memory[descriptor.start + index + 4] & 0xff) << 24)
				| (((long) descriptor.memory[descriptor.start + index + 5] & 0xff) << 16)
				| (((long) descriptor.memory[descriptor.start + index + 6] & 0xff) << 8)
				| (((long) descriptor.memory[descriptor.start + index + 7] & 0xff) << 0);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putLong(int index, long value) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 7 < descriptor.size) {
			descriptor.memory[descriptor.start + index + 0] = (byte) ((value >> 56) & 0xff);
			descriptor.memory[descriptor.start + index + 1] = (byte) ((value >> 48) & 0xff);
			descriptor.memory[descriptor.start + index + 2] = (byte) ((value >> 40) & 0xff);
			descriptor.memory[descriptor.start + index + 3] = (byte) ((value >> 32) & 0xff);
			descriptor.memory[descriptor.start + index + 4] = (byte) ((value >> 24) & 0xff);
			descriptor.memory[descriptor.start + index + 5] = (byte) ((value >> 16) & 0xff);
			descriptor.memory[descriptor.start + index + 6] = (byte) ((value >> 8) & 0xff);
			descriptor.memory[descriptor.start + index + 7] = (byte) (value & 0xff);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public int getInt(int index) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 3 < descriptor.size) {
			return (((int) descriptor.memory[descriptor.start + index + 0] & 0xff) << 24)
				| (((int) descriptor.memory[descriptor.start + index + 1] & 0xff) << 16)
				| (((int) descriptor.memory[descriptor.start + index + 2] & 0xff) << 8)
				| (((int) descriptor.memory[descriptor.start + index + 3] & 0xff) << 0);
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putInt(int index, int value) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 3 < descriptor.size) {
			descriptor.memory[descriptor.start + index + 0] = (byte) ((value >> 24) & 0xff);
			descriptor.memory[descriptor.start + index + 1] = (byte) ((value >> 16) & 0xff);
			descriptor.memory[descriptor.start + index + 2] = (byte) ((value >> 8) & 0xff);
			descriptor.memory[descriptor.start + index + 3] = (byte) ((value >> 0) & 0xff);
			return this;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public short getShort(int index) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 1 < descriptor.size) {
			return (short) (((descriptor.memory[descriptor.start + index + 0] & 0xff) << 8) | ((descriptor.memory[descriptor.start
				+ index + 1] & 0xff) << 0));
		} else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public RandomAccessView putShort(int index, short value) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (index >= 0 && index + 1 < descriptor.size) {
			descriptor.memory[descriptor.start + index + 0] = (byte) ((value >> 8) & 0xff);
			descriptor.memory[descriptor.start + index + 1] = (byte) ((value >> 0) & 0xff);
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
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (descriptor != null) {
			return descriptor.memory;
		} else {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.api.RandomAccessView#translateOffset(int)
	 */
	@Override
	public int translateOffset(int offset) {
		MemorySegmentDescriptor descriptor = descriptorReference.get();
		return descriptor.start + offset;
	}
}
