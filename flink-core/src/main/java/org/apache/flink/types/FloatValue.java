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
 * Boxed serializable and comparable single precision floating point type, representing the primitive
 * type {@code float}.
 *
 * Comparable and Key are super-interfaces of NormalizableKey but are required for API compatibility.
 */
@Public
public class FloatValue implements NormalizableKey<FloatValue>, ResettableValue<FloatValue>, CopyableValue<FloatValue>, Comparable<FloatValue>, Key<FloatValue> {

	private static final long serialVersionUID = 1L;

	private float value;

	/**
	 * Initializes the encapsulated float with 0.0.
	 */
	public FloatValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated float with the provided value.
	 * 
	 * @param value
	 *        Initial value of the encapsulated float.
	 */
	public FloatValue(float value) {
		this.value = value;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the value of the encapsulated primitive float.
	 * 
	 * @return the value of the encapsulated primitive float.
	 */
	public float getValue() {
		return this.value;
	}

	/**
	 * Sets the value of the encapsulated primitive float.
	 * 
	 * @param value
	 *        the new value of the encapsulated primitive float.
	 */
	public void setValue(float value) {
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
	public void setValue(FloatValue value) {
		this.value = value.value;
	}

	// --------------------------------------------------------------------------------------------
	// IOReadableWritable
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.value = in.readFloat();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeFloat(this.value);
	}

	// --------------------------------------------------------------------------------------------
	// Comparable
	// --------------------------------------------------------------------------------------------

	@Override
	public int compareTo(FloatValue o) {
		final double other = o.value;
		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	// --------------------------------------------------------------------------------------------
	// Key
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return Float.floatToIntBits(this.value);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof FloatValue) {
			final FloatValue other = (FloatValue) obj;
			return Float.floatToIntBits(this.value) == Float.floatToIntBits(other.value);
		}
		return false;
	}

	// --------------------------------------------------------------------------------------------
	// NormalizableKey
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return 8;
	}

	@Override
	public void copyNormalizedKey(MemorySegment memory, int offset, int len) {
		// float representation is the same for positive and negative values
		// except for the leading sign bit; representations for positive values
		// are normalized and representations for negative values need inversion
		int bits = Float.floatToIntBits(value);
		bits = (value < 0) ? ~bits : bits - Integer.MIN_VALUE;

		// see IntValue for an explanation of the logic
		if (len > 3) {
			memory.putIntBigEndian(offset, bits);

			for (int i = 4; i < len; i++) {
				memory.put(offset + i, (byte) 0);
			}
		} else if (len > 0) {
			for (int i = 0; len > 0; len--, i++) {
				memory.put(offset + i, (byte) (bits >>> ((3-i)<<3)));
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	// CopyableValue
	// --------------------------------------------------------------------------------------------

	@Override
	public int getBinaryLength() {
		return 4;
	}

	@Override
	public void copyTo(FloatValue target) {
		target.value = this.value;
	}

	@Override
	public FloatValue copy() {
		return new FloatValue(this.value);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 4);
	}
}
