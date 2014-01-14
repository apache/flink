/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.util.StringUtils;

/**
 * A statistically unique identification number.
 */
public class AbstractID implements IOReadableWritable {

	/** The size of a long in bytes */
	private static final int SIZE_OF_LONG = 8;

	/** The size of the ID in byte */
	public static final int SIZE = 2 * SIZE_OF_LONG;

	/** The upper part of the actual ID */
	private long upperPart;

	/** The lower part of the actual ID */
	private long lowerPart;

	/**
	 * Constructs a new ID with a specific bytes value.
	 */
	public AbstractID(byte[] bytes) {
		if (bytes.length != SIZE) {
			throw new IllegalArgumentException("Argument bytes must by an array of " + SIZE + " bytes");
		}

		this.lowerPart = byteArrayToLong(bytes, 0);
		this.upperPart = byteArrayToLong(bytes, SIZE_OF_LONG);
	}

	/**
	 * Constructs a new abstract ID.
	 *
	 * @param lowerPart the lower bytes of the ID
	 * @param upperPart the higher bytes of the ID
	 */
	public AbstractID(long lowerPart, long upperPart) {
		this.lowerPart = lowerPart;
		this.upperPart = upperPart;
	}

	/**
	 * Creates a new abstract ID from the given one.
	 * <p>
	 * The given and the newly created abstract ID will be identical, i.e. a comparison by <code>equals</code> will
	 * return <code>true</code> and both objects will have the same hash code.
	 *
	 * @param id the abstract ID to copy
	 */
	public AbstractID(AbstractID id) {
		this.lowerPart = id.lowerPart;
		this.upperPart = id.upperPart;
	}

	/**
	 * Constructs a new random ID from a uniform distribution.
	 */
	public AbstractID() {
		this.lowerPart = generateRandomLong();
		this.upperPart = generateRandomLong();
	}

	/**
	 * Generates a uniformly distributed random positive long.
	 *
	 * @return a uniformly distributed random positive long
	 */
	protected static long generateRandomLong() {
		return (long) (Math.random() * Long.MAX_VALUE);
	}

	/**
	 * Converts the given byte array to a long.
	 *
	 * @param ba the byte array to be converted
	 * @param offset the offset indicating at which byte inside the array the conversion shall begin
	 * @return the long variable
	 */
	private static long byteArrayToLong(byte[] ba, int offset) {
		long l = 0;

		for (int i = 0; i < SIZE_OF_LONG; ++i) {
			l |= (ba[offset + SIZE_OF_LONG - 1 - i] & 0xffL) << (i << 3);
		}

		return l;
	}

	/**
	 * Converts a long to a byte array.
	 *
	 * @param l the long variable to be converted
	 * @param ba the byte array to store the result the of the conversion
	 * @param offset offset indicating at what position inside the byte array the result of the conversion shall be stored
	 */
	private static void longToByteArray(final long l, final byte[] ba, final int offset) {
		for (int i = 0; i < SIZE_OF_LONG; ++i) {
			final int shift = i << 3; // i * 8
			ba[offset + SIZE_OF_LONG - 1 - i] = (byte) ((l & (0xffL << shift)) >>> shift);
		}
	}

	/**
	 * Sets an ID from another ID by copying its internal byte representation.
	 *
	 * @param src source ID
	 */
	public void setID(AbstractID src) {
		this.lowerPart = src.lowerPart;
		this.upperPart = src.upperPart;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof AbstractID) {
			AbstractID src = (AbstractID) obj;
			return src.lowerPart == this.lowerPart && src.upperPart == this.upperPart;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return ((int)  this.lowerPart) ^
				((int) (this.lowerPart >>> 32)) ^
				((int)  this.upperPart) ^
				((int) (this.upperPart >>> 32));
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.lowerPart = in.readLong();
		this.upperPart = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.lowerPart);
		out.writeLong(this.upperPart);
	}

	public void write(ByteBuffer buffer) {
		buffer.putLong(this.lowerPart);
		buffer.putLong(this.upperPart);
	}

	@Override
	public String toString() {
		final byte[] ba = new byte[SIZE];
		longToByteArray(this.lowerPart, ba, 0);
		longToByteArray(this.upperPart, ba, SIZE_OF_LONG);
		return StringUtils.byteToHexString(ba);
	}
}
