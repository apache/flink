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

package eu.stratosphere.example.java.record.sort.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.Key;

/**
 * An integer number according to the rules of Jim Gray's sorting benchmark has a length of 100 bytes. The first 10
 * bytes represent the key for the sort process, the rest is the value. The final byte of a 100 byte integer is always
 * the newline character.
 * <p>
 * This class is a wrapper for the key part of the integer number.
 * 
 * @author warneke
 */
public final class TeraKey implements Key {
	private static final long serialVersionUID = 1L;

	/**
	 * The size of the key in bytes.
	 */
	public static final int KEY_SIZE = 10;

	/**
	 * The buffer to store the key.
	 */
	private byte[] key;
	
	/**
	 * The offset to the key byte sequence.
	 */
	private int offset;

	/**
	 * Constructs a new key object. The key points to the subsequence in the given array, i.e. it 
	 * is sharing the byte array.
	 * 
	 * @param srcBuf The source buffer to read the key from.
	 * @param offset The offset in the byte array where the key subsequence starts.
	 */
	public TeraKey(final byte[] srcBuf, int offset) {
		this.key = srcBuf;
		this.offset = offset;
	}

	/**
	 * Default constructor required for serialization/deserialization.
	 */
	public TeraKey() {
		this.key = new byte[KEY_SIZE];
	}
	
	/**
	 * Sets the value of this key object. This key will point to the subsequence in the given array, i.e. it 
	 * is sharing the byte array.
	 * 
	 * @param data The source buffer to read the key from.
	 * @param offset The offset in the byte array where the key subsequence starts.
	 */
	public void setValue(final byte[] data, int offset) {
		this.key = data;
		this.offset = offset;
	}


	@Override
	public void write(DataOutput out) throws IOException
	{
		out.write(this.key, this.offset, KEY_SIZE);
	}


	@Override
	public void read(DataInput in) throws IOException {
		in.readFully(this.key, 0, KEY_SIZE);
		this.offset = 0;
	}


	@Override
	public int compareTo(Key arg0) {

		if (!(arg0 instanceof TeraKey)) {
			return Integer.MAX_VALUE;
		}

		final TeraKey tsk = (TeraKey) arg0;

		int diff = 0;
		for (int i = 0; i < KEY_SIZE; ++i) {

			diff = (this.key[i + this.offset] - tsk.key[i + tsk.offset]);
			if (diff != 0) {
				break;
			}
		}

		return diff;
	}


	@Override
	public int hashCode() {
		int result = 1;
		for (int i = 0; i < KEY_SIZE; i++) {
			result = 31 * result + this.key[i + this.offset];
		}
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (getClass() != obj.getClass())
			return false;
		
		final TeraKey other = (TeraKey) obj;
		for (int i = 0, tx = this.offset, ox = other.offset; i < KEY_SIZE; i++, tx++, ox++) {
			if (this.key[tx] != other.key[ox]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Copies the key to the given byte buffer.
	 * 
	 * @param buf
	 *        the buffer to copy the key to
	 */
	public void copyToBuffer(final byte[] buf) {

		System.arraycopy(this.key, this.offset, buf, 0, KEY_SIZE);
	}


	@Override
	public String toString() {

		return new String(this.key, this.offset, KEY_SIZE);
	}
}
