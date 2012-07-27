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

package eu.stratosphere.pact.example.sort.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

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

	/**
	 * The size of the key in bytes.
	 */
	public static final int KEY_SIZE = 10;

	/**
	 * The buffer to store the key.
	 */
	private final byte[] key = new byte[KEY_SIZE];

	/**
	 * Constructs a new key object.
	 * 
	 * @param srcBuf
	 *        the source buffer to read the key from
	 */
	public TeraKey(final byte[] srcBuf, int offset) {
		System.arraycopy(srcBuf, offset, this.key, 0, KEY_SIZE);
	}

	/**
	 * Default constructor required for serialization/deserialization.
	 */
	public TeraKey() {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.write(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		in.readFully(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(Key arg0) {

		if (!(arg0 instanceof TeraKey)) {
			return Integer.MAX_VALUE;
		}

		final TeraKey tsk = (TeraKey) arg0;

		int diff = 0;
		for (int i = 0; i < KEY_SIZE; ++i) {

			diff = (this.key[i] - tsk.key[i]);
			if (diff != 0) {
				break;
			}
		}

		return diff;
	}

	/**
	 * Copies the key to the given byte buffer.
	 * 
	 * @param buf
	 *        the buffer to copy the key to
	 */
	public void copyToBuffer(final byte[] buf) {

		System.arraycopy(this.key, 0, buf, 0, KEY_SIZE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return new String(this.key);
	}
}
