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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.CopyableValue;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NormalizableKey;

/**
 * Integer base type for PACT programs that implements the Key interface.
 * PactInteger encapsulates a Java primitive int.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 */
public class PactByte implements Key, NormalizableKey, CopyableValue<PactByte> {
	private static final long serialVersionUID = 1L;
	
	private byte value;

	/**
	 * Initializes the encapsulated byte with 0.
	 */
	public PactByte() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated byte with the provided value.
	 * 
	 * @param value Initial value of the encapsulated byte.
	 */
	public PactByte(byte value) {
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
		if (!(o instanceof PactByte))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to PactByte!");

		final byte other = ((PactByte) o).value;
		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		return this.value;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof PactByte) {
			return ((PactByte) obj).value == this.value;
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
	public void copyTo(PactByte target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 1);
	}
}
