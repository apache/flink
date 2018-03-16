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
 * Boxed serializable and comparable character type, representing the primitive
 * type {@code char}.
 */
@Public
public class CharValue implements NormalizableKey<CharValue>, ResettableValue<CharValue>, CopyableValue<CharValue> {
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
	public void setValue(CharValue value) {
		this.value = value.value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		this.value = in.readChar();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeChar(this.value);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int compareTo(CharValue o) {
		final int other = o.value;
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
		// no code that transforms the signed representation to an offset representation
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
	public CharValue copy() {
		return new CharValue(this.value);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 2);
	}
}
