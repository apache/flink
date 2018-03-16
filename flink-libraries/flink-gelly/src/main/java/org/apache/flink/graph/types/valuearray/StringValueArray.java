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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * An array of {@link StringValue}.
 *
 * <p>Strings are serialized to a byte array. Concatenating arrays is as simple
 * and fast as extending and copying byte arrays. Strings are serialized when
 * individually added to {@code StringValueArray}.
 *
 * <p>For each string added to the array the length is first serialized using a
 * variable length integer. Then the string characters are serialized using a
 * variable length encoding where the lower 128 ASCII/UFT-8 characters are
 * encoded in a single byte. This ensures that common characters are serialized
 * in only two bytes.
 */
public class StringValueArray
implements ValueArray<StringValue> {

	protected static final int DEFAULT_CAPACITY_IN_BYTES = 4096;

	// see note in ArrayList, HashTable, ...
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

	protected static final int HIGH_BIT = 0x1 << 7;

	private boolean isBounded;

	// the initial length of a bounded array, which is allowed to expand to
	// store one additional element beyond this initial length
	private int boundedLength;

	private byte[] data;

	// number of StringValue elements currently stored
	private int length;

	// the number of bytes currently stored
	private int position;

	// state for the bookmark used by mark() and reset()
	private transient int markLength;

	private transient int markPosition;

	// hasher used to generate the normalized key
	private MurmurHash hash = new MurmurHash(0x19264330);

	// hash result stored as normalized key
	private IntValue hashValue = new IntValue();

	/**
	 * Initializes an expandable array with default capacity.
	 */
	public StringValueArray() {
		isBounded = false;
		initialize(DEFAULT_CAPACITY_IN_BYTES);
	}

	/**
	 * Initializes a fixed-size array with the provided number of bytes.
	 *
	 * @param bytes number of bytes of the encapsulated array
	 */
	public StringValueArray(int bytes) {
		isBounded = true;
		boundedLength = bytes;
		initialize(bytes);
	}

	/**
	 * Initializes the array with the provided number of bytes.
	 *
	 * @param bytes initial size of the encapsulated array in bytes
	 */
	private void initialize(int bytes) {
		Preconditions.checkArgument(bytes > 0, "Requested array with zero capacity");
		Preconditions.checkArgument(bytes <= MAX_ARRAY_SIZE, "Requested capacity exceeds limit of " + MAX_ARRAY_SIZE);

		data = new byte[bytes];
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * If the size of the array is insufficient to hold the given capacity then
	 * copy the array into a new, larger array.
	 *
	 * @param minCapacity minimum required number of elements
	 */
	private void ensureCapacity(int minCapacity) {
		long currentCapacity = data.length;

		if (minCapacity <= currentCapacity) {
			return;
		}

		// increase capacity by at least ~50%
		long expandedCapacity = Math.max(minCapacity, currentCapacity + (currentCapacity >> 1));
		int newCapacity = (int) Math.min(MAX_ARRAY_SIZE, expandedCapacity);

		if (newCapacity < minCapacity) {
			// throw exception as unbounded arrays are not expected to fill
			throw new RuntimeException("Requested array size " + minCapacity + " exceeds limit of " + MAX_ARRAY_SIZE);
		}

		data = Arrays.copyOf(data, newCapacity);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		String separator = "";

		for (StringValue sv : this) {
			sb
				.append(sv.getValue())
				.append(separator);
			separator = ",";
		}

		sb.append("]");

		return sb.toString();
	}

	// --------------------------------------------------------------------------------------------
	// Iterable
	// --------------------------------------------------------------------------------------------

	private final ReadIterator iterator = new ReadIterator();

	@Override
	public Iterator<StringValue> iterator() {
		iterator.reset();
		return iterator;
	}

	private class ReadIterator
	implements Iterator<StringValue> {
		private static final int DEFAULT_SIZE = 64;

		private StringValue value = new StringValue(CharBuffer.allocate(DEFAULT_SIZE));

		private int size = DEFAULT_SIZE;

		private int pos;

		@Override
		public boolean hasNext() {
			return pos < position;
		}

		@Override
		public StringValue next() {
			// read length
			int len = data[pos++] & 0xFF;

			if (len >= HIGH_BIT) {
				int shift = 7;
				int curr;
				len = len & 0x7F;
				while ((curr = data[pos++] & 0xFF) >= HIGH_BIT) {
					len |= (curr & 0x7F) << shift;
					shift += 7;
				}
				len |= curr << shift;
			}

			// ensure capacity
			if (len > size) {
				while (size < len) {
					size *= 2;
				}

				value = new StringValue(CharBuffer.allocate(size));
			}

			// read string characters
			final char[] valueData = value.getCharArray();

			for (int i = 0; i < len; i++) {
				int c = data[pos++] & 0xFF;
				if (c >= HIGH_BIT) {
					int shift = 7;
					int curr;
					c = c & 0x7F;
					while ((curr = data[pos++] & 0xFF) >= HIGH_BIT) {
						c |= (curr & 0x7F) << shift;
						shift += 7;
					}
					c |= curr << shift;
				}
				valueData[i] = (char) c;
			}

			// cannot prevent allocation of new StringValue!
			return value.substring(0, len);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove");
		}

		public void reset() {
			pos = 0;
		}
	}

	// --------------------------------------------------------------------------------------------
	// IOReadableWritable
	// --------------------------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(length);
		out.writeInt(position);

		out.write(data, 0, position);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		length = in.readInt();
		position = in.readInt();

		markLength = 0;
		markPosition = 0;

		ensureCapacity(position);

		in.read(data, 0, position);
	}

	// --------------------------------------------------------------------------------------------
	// NormalizableKey
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return hashValue.getMaxNormalizedKeyLen();
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		hash.reset();

		hash.hash(position);
		for (int i = 0; i < position; i++) {
			hash.hash(data[i]);
		}

		hashValue.setValue(hash.hash());
		hashValue.copyNormalizedKey(target, offset, len);
	}

	// --------------------------------------------------------------------------------------------
	// Comparable
	// --------------------------------------------------------------------------------------------

	@Override
	public int compareTo(ValueArray<StringValue> o) {
		StringValueArray other = (StringValueArray) o;

		// sorts first on number of data in the array, then comparison between
		// the first non-equal element in the arrays
		int cmp = Integer.compare(position, other.position);

		if (cmp != 0) {
			return cmp;
		}

		for (int i = 0; i < position; i++) {
			cmp = Byte.compare(data[i], other.data[i]);

			if (cmp != 0) {
				return cmp;
			}
		}

		return 0;
	}

	// --------------------------------------------------------------------------------------------
	// Key
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int hash = 0;

		for (int i = 0; i < position; i++) {
			hash = 31 * hash + data[i];
		}

		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StringValueArray) {
			StringValueArray other = (StringValueArray) obj;

			if (length != other.length) {
				return false;
			}

			if (position != other.position) {
				return false;
			}

			for (int i = 0; i < position; i++) {
				if (data[i] != other.data[i]) {
					return false;
				}
			}

			return true;
		}

		return false;
	}

	// --------------------------------------------------------------------------------------------
	// ResettableValue
	// --------------------------------------------------------------------------------------------

	@Override
	public void setValue(ValueArray<StringValue> value) {
		value.copyTo(this);
	}

	// --------------------------------------------------------------------------------------------
	// CopyableValue
	// --------------------------------------------------------------------------------------------

	@Override
	public int getBinaryLength() {
		return -1;
	}

	@Override
	public void copyTo(ValueArray<StringValue> target) {
		StringValueArray other = (StringValueArray) target;

		other.length = length;
		other.position = position;
		other.markLength = markLength;
		other.markPosition = markPosition;

		other.ensureCapacity(position);
		System.arraycopy(data, 0, other.data, 0, position);
	}

	@Override
	public ValueArray<StringValue> copy() {
		ValueArray<StringValue> copy = new StringValueArray();

		this.copyTo(copy);

		return copy;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		copyInternal(source, target);
	}

	protected static void copyInternal(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);

		int position = source.readInt();
		target.writeInt(position);

		target.write(source, position);
	}

	// --------------------------------------------------------------------------------------------
	// ValueArray
	// --------------------------------------------------------------------------------------------

	@Override
	public int size() {
		return length;
	}

	@Override
	public boolean isFull() {
		if (isBounded) {
			return position >= boundedLength;
		} else {
			return position == MAX_ARRAY_SIZE;
		}
	}

	@Override
	public boolean add(StringValue value) {
		if (isBounded && position >= boundedLength) {
			return false;
		}

		// up to five bytes storing length
		if (position + 5 > data.length) {
			ensureCapacity(position + 5);
		}

		// update local variable until serialization succeeds
		int newPosition = position;

		// write the length, variable-length encoded
		int len = value.length();

		while (len >= HIGH_BIT) {
			data[newPosition++] = (byte) (len | HIGH_BIT);
			len >>>= 7;
		}
		data[newPosition++] = (byte) len;

		// write the char data, variable-length encoded
		final char[] valueData = value.getCharArray();
		int remainingCapacity = data.length - newPosition;

		len = value.length();
		for (int i = 0; i < len; i++) {
			// up to three bytes storing length
			if (remainingCapacity < 3) {
				ensureCapacity(remainingCapacity + 3);
				remainingCapacity = data.length - newPosition;
			}

			int c = valueData[i];

			while (c >= HIGH_BIT) {
				data[newPosition++] = (byte) (c | HIGH_BIT);
				remainingCapacity--;
				c >>>= 7;
			}
			data[newPosition++] = (byte) c;
			remainingCapacity--;
		}

		length++;
		position = newPosition;

		return true;
	}

	@Override
	public boolean addAll(ValueArray<StringValue> other) {
		StringValueArray source = (StringValueArray) other;

		int sourceSize = source.position;
		int newPosition = position + sourceSize;

		if (newPosition > data.length) {
			if (isBounded) {
				return false;
			} else {
				ensureCapacity(newPosition);
			}
		}

		System.arraycopy(source.data, 0, data, position, sourceSize);
		length += source.length;
  	    position = newPosition;

		return true;
	}

	@Override
	public void clear() {
		length = 0;
		position = 0;
	}

	@Override
	public void mark() {
		markLength = length;
		markPosition = position;
	}

	@Override
	public void reset() {
		length = markLength;
		position = markPosition;
	}
}
