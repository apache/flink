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

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

/**
 * Boxed serializable and comparable short integer type, representing the primitive
 * type {@code short} (signed 16-bit integer).
 */
@Public
public class ShortValue implements NormalizableKey<ShortValue>, ResettableValue<ShortValue>, CopyableValue<ShortValue> {
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

	// --------------------------------------------------------------------------------------------

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
	// ResettableValue
	// --------------------------------------------------------------------------------------------

	@Override
	public void setValue(ShortValue value) {
		this.value = value.value;
	}

	// --------------------------------------------------------------------------------------------
	// IOReadableWritable
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.value = in.readShort();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeShort(this.value);
	}

	// --------------------------------------------------------------------------------------------
	// Comparable
	// --------------------------------------------------------------------------------------------

	@Override
	public int compareTo(ShortValue o) {
		final int other = o.value;
		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	// --------------------------------------------------------------------------------------------
	// Key
	// --------------------------------------------------------------------------------------------

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
	// NormalizableKey
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return 2;
	}

	@Override
	public void copyNormalizedKey(MemorySegment memory, int offset, int len) {
		// see IntValue for an explanation of the logic
		short normalizedValue = (short) (value - Short.MIN_VALUE);

		if (len > 1) {
			memory.putShortBigEndian(offset, normalizedValue);

			for (int i = 2; i < len; i++) {
				memory.put(offset + i, (byte) 0);
			}
		} else if (len > 0) {
			memory.put(offset, (byte) ((normalizedValue >>> 8) & 0xff));
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
	public ShortValue copy() {
		return new ShortValue(this.value);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 2);
	}
}
