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

package eu.stratosphere.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;

/**
 * Boxed serializable and comparable byte type, representing the primitive
 * type {@code byte} (signed 8 bit integer).
 * 
 * @see eu.stratosphere.types.Key
 */
public class ByteValue implements Key, NormalizableKey, ResettableValue<ByteValue>, CopyableValue<ByteValue> {
	private static final long serialVersionUID = 1L;
	
	private byte value;

	/**
	 * Initializes the encapsulated byte with 0.
	 */
	public ByteValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated byte with the provided value.
	 * 
	 * @param value Initial value of the encapsulated byte.
	 */
	public ByteValue(byte value) {
		this.value = value;
	}
	
	/**
	 * Returns the value of the encapsulated byte.
	 * 
	 * @return the value of the encapsulated byte.
	 */
	public byte getValue() {
		return this.value;
	}

	/**
	 * Sets the encapsulated byte to the specified value.
	 * 
	 * @param value
	 *        the new value of the encapsulated byte.
	 */
	public void setValue(byte value) {
		this.value = value;
	}

    @Override
    public void setValue(ByteValue value) {
        this.value = value.value;
    }

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInput in) throws IOException {
		this.value = in.readByte();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.valueOf(this.value);
	}
	
	@Override
	public int compareTo(Key o) {
		if (!(o instanceof ByteValue))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to ByteValue!");

		final byte other = ((ByteValue) o).value;
		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		return this.value;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof ByteValue) {
			return ((ByteValue) obj).value == this.value;
		}
		return false;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return 1;
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		if (len == 1) {
			// default case, full normalized key. need to explicitly convert to int to
			// avoid false results due to implicit type conversion to int when subtracting
			// the min byte value
			int highByte = this.value & 0xff;
			highByte -= Byte.MIN_VALUE;
			target.put(offset, (byte) highByte);
		}
		else if (len <= 0) {
		}
		else {
			int highByte = this.value & 0xff;
			highByte -= Byte.MIN_VALUE;
			target.put(offset, (byte) highByte);
			for (int i = 1; i < len; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return 1;
	}
	
	@Override
	public void copyTo(ByteValue target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 1);
	}
}
