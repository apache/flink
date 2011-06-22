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

package eu.stratosphere.pact.example.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

/**
 * An integer number according to the rules of Jim Gray's sorting benchmark has a length of 100 bytes. The first 10
 * bytes represent the key for the sort process, the rest is the value. The final byte of a 100 byte integer is always
 * the newline character.
 * <p>
 * This class is a wrapper for the value part of the integer number.
 * 
 * @author warneke
 */
public final class TeraValue implements Value {

	/**
	 * The size of the value in bytes.
	 */
	public static final int VALUE_SIZE = 89;

	/**
	 * The buffer to store the value.
	 */
	private final byte[] value = new byte[VALUE_SIZE];

	/**
	 * Constructs a new value object.
	 * 
	 * @param srcBuf
	 *        the source buffer to read the value from
	 */
	public TeraValue(final byte[] srcBuf) {
		System.arraycopy(srcBuf, TeraKey.KEY_SIZE, this.value, 0, VALUE_SIZE);
	}

	/**
	 * Default constructor required for serialization/deserialization.
	 */
	public TeraValue() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.write(this.value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		in.readFully(this.value);
	}

	/**
	 * Copies the value to the given byte buffer.
	 * 
	 * @param buf
	 *        the buffer to copy the value to
	 */
	public void copyToBuffer(final byte[] buf) {

		System.arraycopy(this.value, 0, buf, TeraKey.KEY_SIZE, VALUE_SIZE);
	}
}
