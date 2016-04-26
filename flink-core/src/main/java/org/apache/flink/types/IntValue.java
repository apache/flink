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


package org.apache.flink.types;

import java.io.IOException;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

/**
 * Boxed serializable and comparable integer type, representing the primitive
 * type {@code int}.
 * 
 * @see org.apache.flink.types.Key
 */
@Public
public class IntValue implements NormalizableKey<IntValue>, ResettableValue<IntValue>, CopyableValue<IntValue> {
	private static final long serialVersionUID = 1L;
	
	private int value;

	/**
	 * Initializes the encapsulated int with 0.
	 */
	public IntValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated int with the provided value.
	 * 
	 * @param value Initial value of the encapsulated int.
	 */
	public IntValue(int value) {
		this.value = value;
	}
	
	/**
	 * Returns the value of the encapsulated int.
	 * 
	 * @return the value of the encapsulated int.
	 */
	public int getValue() {
		return this.value;
	}

	/**
	 * Sets the encapsulated int to the specified value.
	 * 
	 * @param value
	 *        the new value of the encapsulated int.
	 */
	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public void setValue(IntValue value) {
		this.value = value.value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		this.value = in.readInt();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int compareTo(IntValue o) {
		final int other = o.value;
		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		return this.value;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof IntValue) {
			return ((IntValue) obj).value == this.value;
		}
		return false;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return 4;
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		// take out value and add the integer min value. This gets an offsetted
		// representation when interpreted as an unsigned integer (as is the case
		// with normalized keys). write this value as big endian to ensure the
		// most significant byte comes first.
		if (len == 4) {
			target.putIntBigEndian(offset, value - Integer.MIN_VALUE);
		}
		else if (len <= 0) {
		}
		else if (len < 4) {
			int value = this.value - Integer.MIN_VALUE;
			for (int i = 0; len > 0; len--, i++) {
				target.put(offset + i, (byte) ((value >>> ((3-i)<<3)) & 0xff));
			}
		}
		else {
			target.putIntBigEndian(offset, value - Integer.MIN_VALUE);
			for (int i = 4; i < len; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return 4;
	}

	@Override
	public void copyTo(IntValue target) {
		target.value = this.value;
	}

	@Override
	public IntValue copy() {
		return new IntValue(this.value);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 4);
	}
}
