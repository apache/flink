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

package eu.stratosphere.example.record.sort.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.Value;

/**
 * An integer number according to the rules of Jim Gray's sorting benchmark has a length of 100 bytes. The first 10
 * bytes represent the key for the sort process, the rest is the value. The final byte of a 100 byte integer is always
 * the newline character.
 * <p>
 * This class is a wrapper for the value part of the integer number.
 */
public final class TeraValue implements Value {
	private static final long serialVersionUID = 1L;

	/**
	 * The size of the value in bytes.
	 */
	public static final int VALUE_SIZE = 89;

	/**
	 * The buffer to store the value.
	 */
	private byte[] value;
	
	/**
	 * The offset to the value byte sequence.
	 */
	private int offset;

	/**
	 * Constructs a new value object. The value points to the subsequence in the given array, i.e. it 
	 * is sharing the byte array.
	 * 
	 * @param srcBuf The source buffer to read the value from.
	 * @param offset The offset in the byte array where the value subsequence starts.
	 */
	public TeraValue(final byte[] srcBuf, int offset) {
		this.value = srcBuf;
		this.offset = offset;
	}

	/**
	 * Default constructor required for serialization/deserialization.
	 */
	public TeraValue() {
		this.value = new byte[VALUE_SIZE];
	}
	
	/**
	 * Sets the value of this value object. This value will point to the subsequence in the given array, i.e. it 
	 * is sharing the byte array.
	 * 
	 * @param data The source buffer to read the value from.
	 * @param offset The offset in the byte array where the value subsequence starts.
	 */
	public void setValue(final byte[] data, int offset) {
		this.value = data;
		this.offset = offset;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(this.value, this.offset, VALUE_SIZE);
	}

	@Override
	public void read(DataInput in) throws IOException {
		in.readFully(this.value, 0, VALUE_SIZE);
		this.offset = 0;
	}
	
	@Override
	public String toString() {
		return new String(this.value, this.offset, VALUE_SIZE);
	}
	
	/**
	 * Copies the value to the given byte buffer.
	 * 
	 * @param buf
	 *        the buffer to copy the value to
	 */
	public void copyToBuffer(final byte[] buf) {
		System.arraycopy(this.value, this.offset, buf, 0, VALUE_SIZE);
	}
}

