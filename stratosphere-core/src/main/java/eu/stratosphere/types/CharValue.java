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
 * Boxed serializable and comparable character type, representing the primitive
 * type {@code char}.
 * 
 * @see eu.stratosphere.types.Key
 */
public class CharValue implements Key, NormalizableKey, CopyableValue<CharValue> {
	private static final long serialVersionUID = 1L;
	
	private char value;

	/**
	 * Initializes the encapsulated char with 0.
	 */
	public CharValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated char with the provided value.
	 * 
	 * @param value Initial value of the encapsulated char.
	 */
	public CharValue(char value) {
		this.value = value;
	}
	
	/**
	 * Returns the value of the encapsulated char.
	 * 
	 * @return the value of the encapsulated char.
	 */
	public char getValue() {
		return this.value;
	}

	/**
	 * Sets the encapsulated char to the specified value.
	 * 
	 * @param value
	 *        the new value of the encapsulated char.
	 */
	public void setValue(char value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInput in) throws IOException {
		this.value = in.readChar();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChar(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof CharValue))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to PactChar!");

		final int other = ((CharValue) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		return this.value;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof CharValue) {
			return ((CharValue) obj).value == this.value;
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
		// note that the char is an unsigned data type in java and consequently needs
		// no code that transforms the signed representation to an offsetted representation
		// that is equivalent to unsigned, when compared byte by byte
		if (len == 2) {
			// default case, full normalized key
			target.put(offset,     (byte) ((value >>> 8) & 0xff));
			target.put(offset + 1, (byte) ((value      ) & 0xff));
		}
		else if (len <= 0) {
		}
		else if (len == 1) {
			target.put(offset,     (byte) ((value >>> 8) & 0xff));
		}
		else {
			target.put(offset,     (byte) ((value >>> 8) & 0xff));
			target.put(offset + 1, (byte) ((value      ) & 0xff));
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
	public void copyTo(CharValue target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 2);
	}
}
