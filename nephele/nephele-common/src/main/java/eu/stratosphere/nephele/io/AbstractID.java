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

import eu.stratosphere.nephele.util.StringUtils;

/**
 * ID is an abstract base class for providing statistically unique identification numbers in Nephele.
 * Every component that requires these kinds of IDs provides its own concrete type.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public abstract class AbstractID {

	/**
	 * The size of a long in bytes.
	 */
	private static final int SIZE_OF_LONG = 8;

	/**
	 * The size of the ID in byte.
	 */
	protected static final int SIZE = 2 * SIZE_OF_LONG;

	/**
	 * The upper part of the actual ID.
	 */
	private final long upperPart;

	/**
	 * The lower part of the actual ID.
	 */
	private final long lowerPart;

	/**
	 * Constructs a new ID with a specific bytes value.
	 */
	public AbstractID(final byte[] bytes) {

		if (bytes.length != SIZE) {
			throw new IllegalArgumentException("Argument bytes must by an array of " + SIZE + " bytes");
		}

		this.lowerPart = byteArrayToLong(bytes, 0);
		this.upperPart = byteArrayToLong(bytes, SIZE_OF_LONG);
	}

	/**
	 * Constructs a new abstract ID.
	 * 
	 * @param lowerPart
	 *        the lower bytes of the ID
	 * @param upperPart
	 *        the higher bytes of the ID
	 */
	protected AbstractID(final long lowerPart, final long upperPart) {

		this.lowerPart = lowerPart;
		this.upperPart = upperPart;
	}

	/**
	 * Creates a new abstract ID from the given one. The given and the newly created abtract ID will be identical, i.e.
	 * a comparison by <code>equals</code> will return <code>true</code> and both objects will have the same hash code.
	 * 
	 * @param id
	 *        the abstract ID to copy
	 */
	protected AbstractID(final AbstractID id) {

		this.lowerPart = id.lowerPart;
		this.upperPart = id.upperPart;
	}

	/**
	 * Default constructor required by kryo.
	 */
	protected AbstractID() {

		this.lowerPart = 0L;
		this.upperPart = 0L;
	}

	/**
	 * Generates a uniformly distributed random positive long.
	 * 
	 * @return a uniformly distributed random positive long
	 */
	protected static long generateRandomBytes() {

		return (long) (Math.random() * Long.MAX_VALUE);
	}

	/**
	 * Converts a long to a byte array.
	 * 
	 * @param l
	 *        the long variable to be converted
	 * @param ba
	 *        the byte array to store the result the of the conversion
	 * @param offset
	 *        the offset indicating at what position inside the byte array the result of the conversion shall be stored
	 */
	private static void longToByteArray(final long l, final byte[] ba, final int offset) {

		for (int i = 0; i < SIZE_OF_LONG; ++i) {
			final int shift = i << 3; // i * 8
			ba[offset + SIZE_OF_LONG - 1 - i] = (byte) ((l & (0xffL << shift)) >>> shift);
		}
	}

	/**
	 * Converts the given byte array to a long.
	 * 
	 * @param ba
	 *        the byte array to be converted
	 * @param offset
	 *        the offset indicating at which byte inside the array the conversion shall begin
	 * @return the long variable
	 */
	private static long byteArrayToLong(final byte[] ba, final int offset) {

		long l = 0;

		for (int i = 0; i < SIZE_OF_LONG; ++i) {
			l |= (ba[offset + SIZE_OF_LONG - 1 - i] & 0xffL) << (i << 3);
		}

		return l;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof AbstractID)) {
			return false;
		}

		final AbstractID src = (AbstractID) obj;

		if (src.lowerPart != this.lowerPart) {
			return false;
		}

		if (src.upperPart != this.upperPart) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return (int) (this.lowerPart ^ (this.upperPart >>> 32));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final byte[] ba = new byte[SIZE];
		longToByteArray(this.lowerPart, ba, 0);
		longToByteArray(this.upperPart, ba, SIZE_OF_LONG);

		return StringUtils.byteToHexString(ba);
	}
}
