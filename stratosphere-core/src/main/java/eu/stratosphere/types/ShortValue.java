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
 * Boxed serializable and comparable short integer type, representing the primitive
 * type {@code short}.
 * 
 * @see eu.stratosphere.types.Key
 */
public class ShortValue implements Key, NormalizableKey, CopyableValue<ShortValue> {
	private static final long serialVersionUID = 1L;
	
	private short value;

	/**
	 * Initializes the encapsulated short with 0.
	 */
	public ShortValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated short with the provided value.
	 * 
	 * @param value Initial value of the encapsulated short.
	 */
	public ShortValue(short value) {
		this.value = value;
	}
	
	/**
	 * Returns the value of the encapsulated short.
	 * 
	 * @return the value of the encapsulated short.
	 */
	public short getValue() {
		return this.value;
	}

	/**
	 * Sets the encapsulated short to the specified value.
	 * 
	 * @param value
	 *        the new value of the encapsulated short.
	 */
	public void setValue(short value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInput in) throws IOException {
		this.value = in.readShort();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof ShortValue))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to ShortValue!");

		final int other = ((ShortValue) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		return this.value;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof ShortValue) {
			return ((ShortValue) obj).value == this.value;
		}
		return false;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return 2;
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		if (len == 2) {
			// default case, full normalized key
			int highByte = ((value >>> 8) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target.put(offset, (byte) highByte);
			target.put(offset + 1, (byte) ((value) & 0xff));
		}
		else if (len <= 0) {
		}
		else if (len == 1) {
			int highByte = ((value >>> 8) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target.put(offset, (byte) highByte);
		}
		else {
			int highByte = ((value >>> 8) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target.put(offset, (byte) highByte);
			target.put(offset + 1, (byte) ((value) & 0xff));
			for (int i = 2; i < len; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return 2;
	}
	
	@Override
	public void copyTo(ShortValue target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 2);
	}
}
