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
 * Boxed serializable and comparable long integer type, representing the primitive
 * type {@code long}.
 * 
 * @see eu.stratosphere.types.Key
 */
public class LongValue implements NormalizableKey<LongValue>, ResettableValue<LongValue>, CopyableValue<LongValue> {
	private static final long serialVersionUID = 1L;

	private long value;

	/**
	 * Initializes the encapsulated long with 0.
	 */
	public LongValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated long with the specified value. 
	 * 
	 * @param value Initial value of the encapsulated long.
	 */
	public LongValue(final long value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated long.
	 * 
	 * @return The value of the encapsulated long.
	 */
	public long getValue() {
		return this.value;
	}

	/**
	 * Sets the value of the encapsulated long to the specified value.
	 * 
	 * @param value
	 *        The new value of the encapsulated long.
	 */
	public void setValue(final long value) {
		this.value = value;
	}

	@Override
	public void setValue(LongValue value) {
		this.value = value.value;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.value = in.readLong();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeLong(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int compareTo(LongValue o) {
		final long other = o.value;
		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		return 43 + (int) (this.value ^ this.value >>> 32);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj != null & obj instanceof LongValue) {
			return this.value == ((LongValue) obj).value;
		}
		return false;
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public int getMaxNormalizedKeyLen()
	{
		return 8;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.NormalizableKey#copyNormalizedKey(byte[], int, int)
	 */
	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		// see IntValue for an explanation of the logic
		if (len == 8) {
			// default case, full normalized key
			target.putLongBigEndian(offset, value - Long.MIN_VALUE);
		}
		else if (len <= 0) {
		}
		else if (len < 8) {
			long value = this.value - Long.MIN_VALUE;
			for (int i = 0; len > 0; len--, i++) {
				target.put(offset + i, (byte) (value >>> ((7-i)<<3)));
			}
		}
		else {
			target.putLongBigEndian(offset, value - Long.MIN_VALUE);
			for (int i = 8; i < len; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return 8;
	}
	
	@Override
	public void copyTo(LongValue target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 8);
	}
}
