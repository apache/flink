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

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.util.StringUtils;

/**
 * ID is an abstract base class to provide statistically unique and serializable identification numbers in Nephele.
 * Every component that requires these kinds of IDs provides its own concrete type.
 * 
 * @author warneke
 */
public abstract class ID implements IOReadableWritable {

	/**
	 * The size of the ID in byte.
	 */
	private static final int SIZE = 16;

	/**
	 * The buffer storing the actual ID.
	 */
	private final byte[] bytes = new byte[SIZE];

	/**
	 * Constructs a new ID with a specific bytes value.
	 */
	public ID(final byte[] bytes) {

		if (bytes.length == SIZE) {
			System.arraycopy(bytes, 0, this.bytes, 0, SIZE);
		}
	}

	/**
	 * Constructs a new random ID from a uniform distribution.
	 */
	public ID() {

		for (int i = 0; i < SIZE; i++) {
			this.bytes[i] = (byte) ((Math.random() * 256.0) + Byte.MIN_VALUE);
		}
	}

	/**
	 * Sets the bytes the ID consists of.
	 * 
	 * @param src
	 *        the bytes the ID consists of
	 */
	private void setBytes(final byte[] src) {

		if (src == null) {
			return;
		}

		if (src.length != SIZE) {
			return;
		}

		System.arraycopy(src, 0, this.bytes, 0, SIZE);
	}

	/**
	 * Returns the bytes the ID consists of.
	 * 
	 * @return the bytes the ID consists of
	 */
	private byte[] getBytes() {
		return this.bytes;
	}

	/**
	 * Sets an ID from another ID by copying its internal byte representation.
	 * 
	 * @param src
	 *        the source ID
	 */
	public void setID(final ID src) {
		setBytes(src.getBytes());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof ID)) {
			return false;
		}

		final ID src = (ID) obj;

		final byte[] srcBytes = src.getBytes();

		if (srcBytes == null) {
			return false;
		}

		if (this.bytes == null) {
			return false;
		}

		if (srcBytes.length != this.bytes.length) {
			return false;
		}

		for (int i = 0; i < this.bytes.length; i++) {
			if (srcBytes[i] != this.bytes[i]) {
				return false;
			}
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		int hashCode = 0;

		if (this.bytes == null) {
			return 0;
		}

		int tmp = 0;
		for (int i = 0; i < this.bytes.length; i++) {

			final int shift = (Integer.SIZE - 1 - (i % Integer.SIZE)) * 8;
			tmp += (this.bytes[i] & 0x000000FF) << shift;

			if ((i % Integer.SIZE) == 0) {
				hashCode = hashCode ^ tmp;
				tmp = 0;
			}
		}

		return hashCode;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		in.readFully(this.bytes);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		// Write the particular bytes
		out.write(this.bytes);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		if (this.bytes == null) {
			return "null (0)";
		}

		return StringUtils.byteToHexString(bytes);
	}
}
