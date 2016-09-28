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
 * Boxed serializable and comparable boolean type, representing the primitive
 * type {@code boolean}.
 */
@Public
public class BooleanValue implements NormalizableKey<BooleanValue>, ResettableValue<BooleanValue>, CopyableValue<BooleanValue> {

	private static final long serialVersionUID = 1L;

	public static final BooleanValue TRUE = new BooleanValue(true);

	public static final BooleanValue FALSE = new BooleanValue(false);

	private boolean value;

	/**
	 * Initializes the encapsulated boolean with {@code false}.
	 */
	public BooleanValue() {
		this.value = false;
	}

	/**
	 * Initializes the encapsulated boolean with the provided value.
	 *
	 * @param value Initial value of the encapsulated boolean.
	 */
	public BooleanValue(boolean value) {
		this.value = value;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the value of the encapsulated boolean.
	 *
	 * @return the value of the encapsulated boolean.
	 * @deprecated use {@link #getValue()}
	 */
	@Deprecated
	public boolean get() {
		return value;
	}

	/**
	 * Returns the value of the encapsulated boolean.
	 *
	 * @return the value of the encapsulated boolean.
	 */
	public boolean getValue() {
		return value;
	}

	/**
	 * Sets the encapsulated boolean to the specified value.
	 *
	 * @param value
	 *        the new value of the encapsulated boolean.
	 * @deprecated use {@link #setValue(boolean)}
	 */
	@Deprecated
	public void set(boolean value) {
		this.value = value;
	}

	/**
	 * Sets the encapsulated boolean to the specified value.
	 *
	 * @param value
	 *        the new value of the encapsulated boolean.
	 */
	public void setValue(boolean value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return this.value ? "true" : "false";
	}

	// --------------------------------------------------------------------------------------------
	// ResettableValue
	// --------------------------------------------------------------------------------------------

	@Override
	public void setValue(BooleanValue value) {
		this.value = value.value;
	}

	// --------------------------------------------------------------------------------------------
	// IOReadableWritable
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.value = in.readBoolean();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(this.value);
	}

	// --------------------------------------------------------------------------------------------
	// Comparable
	// --------------------------------------------------------------------------------------------

	@Override
	public int compareTo(BooleanValue o) {
		final int ov = o.value ? 1 : 0;
		final int tv = this.value ? 1 : 0;
		return tv - ov;
	}


	// --------------------------------------------------------------------------------------------
	// Key
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return this.value ? 1 : 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BooleanValue) {
			return ((BooleanValue) obj).value == this.value;
		}
		return false;
	}

	// --------------------------------------------------------------------------------------------
	// NormalizableKey
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return 1;
	}

	@Override
	public void copyNormalizedKey(MemorySegment memory, int offset, int len) {
		if (len > 0) {
			memory.putBoolean(offset, this.value);

			for (offset = offset + 1; len > 1; len--) {
				memory.put(offset++, (byte) 0);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	// CopyableValue
	// --------------------------------------------------------------------------------------------

	@Override
	public int getBinaryLength() {
		return 1;
	}

	@Override
	public void copyTo(BooleanValue target) {
		target.value = this.value;
	}

	@Override
	public BooleanValue copy() {
		return new BooleanValue(this.value);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 1);
	}
}
