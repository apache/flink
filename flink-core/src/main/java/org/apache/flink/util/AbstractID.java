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

package org.apache.flink.util;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A statistically unique identification number.
 */
@PublicEvolving
public class AbstractID implements IOReadableWritable, Comparable<AbstractID>, java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	private static final Random RND = new Random();
	

	/** The size of a long in bytes */
	private static final int SIZE_OF_LONG = 8;

	/** The size of the ID in byte */
	public static final int SIZE = 2 * SIZE_OF_LONG;
	

	/** The upper part of the actual ID */
	protected long upperPart;

	/** The lower part of the actual ID */
	protected long lowerPart;

	/** The memoized value returned by toString() */
	private String toString;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs a new ID with a specific bytes value.
	 */
	public AbstractID(byte[] bytes) {
		if (bytes == null || bytes.length != SIZE) {
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
		if (id == null) {
			throw new IllegalArgumentException("Id must not be null.");
		}
		this.lowerPart = id.lowerPart;
		this.upperPart = id.upperPart;
	}

	/**
	 * Constructs a new random ID from a uniform distribution.
	 */
	public AbstractID() {
		this.lowerPart = RND.nextLong();
		this.upperPart = RND.nextLong();
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the lower 64 bits of the ID.
	 *
	 * @return The lower 64 bits of the ID.
	 */
	public long getLowerPart() {
		return lowerPart;
	}

	/**
	 * Gets the upper 64 bits of the ID.
	 *
	 * @return The upper 64 bits of the ID.
	 */
	public long getUpperPart() {
		return upperPart;
	}

	/**
	 * Gets the bytes underlying this ID.
	 *
	 * @return The bytes underlying this ID.
	 */
	public byte[] getBytes() {
		byte[] bytes = new byte[SIZE];
		longToByteArray(lowerPart, bytes, 0);
		longToByteArray(upperPart, bytes, SIZE_OF_LONG);
		return bytes;
	}

	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.lowerPart = in.readLong();
		this.upperPart = in.readLong();

		this.toString = null;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(this.lowerPart);
		out.writeLong(this.upperPart);
	}

	// --------------------------------------------------------------------------------------------
	//  Standard Utilities
	// --------------------------------------------------------------------------------------------
	
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
	public String toString() {
		if (this.toString == null) {
			final byte[] ba = new byte[SIZE];
			longToByteArray(this.lowerPart, ba, 0);
			longToByteArray(this.upperPart, ba, SIZE_OF_LONG);

			this.toString = StringUtils.byteToHexString(ba);
		}

		return this.toString;
	}
	
	@Override
	public int compareTo(AbstractID o) {
		int diff1 = (this.upperPart < o.upperPart) ? -1 : ((this.upperPart == o.upperPart) ? 0 : 1);
		int diff2 = (this.lowerPart < o.lowerPart) ? -1 : ((this.lowerPart == o.lowerPart) ? 0 : 1);
		return diff1 == 0 ? diff2 : diff1;
	}

	// --------------------------------------------------------------------------------------------
	//  Conversion Utilities
	// --------------------------------------------------------------------------------------------

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
	private static void longToByteArray(long l, byte[] ba, int offset) {
		for (int i = 0; i < SIZE_OF_LONG; ++i) {
			final int shift = i << 3; // i * 8
			ba[offset + SIZE_OF_LONG - 1 - i] = (byte) ((l & (0xffL << shift)) >>> shift);
		}
	}
}
