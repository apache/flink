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

package org.apache.flink.runtime.state.heap;

import javax.annotation.Nonnull;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Utilities to get/put data to {@link ByteBuffer}. All methods don't change
 * byte buffer's position.
 */
@SuppressWarnings({"WeakerAccess", "unused", "UnusedReturnValue"})
public class ByteBufferUtils {

	private static final boolean UNSAFE_AVAIL = UnsafeHelp.isAvailable();
	private static final boolean UNSAFE_UNALIGNED = UnsafeHelp.unaligned();
	private static final Field ACCESS_FIELD;

	static {
		try {
			ACCESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");
		} catch (NoSuchFieldException e) {
			throw new RuntimeException("Failed to get address method from java.nio.Buffer", e);
		}
	}

	/**
	 * Reads an int value at the given buffer's offset.
	 *
	 * @param buffer the given buffer
	 * @param offset the given buffer's offset
	 * @return int value at offset
	 */
	public static int toInt(ByteBuffer buffer, int offset) {
		if (UNSAFE_UNALIGNED) {
			return UnsafeHelp.toInt(buffer, offset);
		} else {
			return buffer.getInt(offset);
		}
	}

	/**
	 * Reads a long value at the given buffer's offset.
	 *
	 * @param buffer the given buffer
	 * @param offset the given buffer's offset
	 * @return long value at offset
	 */
	public static long toLong(ByteBuffer buffer, int offset) {
		if (UNSAFE_UNALIGNED) {
			return UnsafeHelp.toLong(buffer, offset);
		} else {
			return buffer.getLong(offset);
		}
	}

	/**
	 * Reads a short value at the given buffer's offset.
	 *
	 * @param buffer the given buffer
	 * @param offset the given buffer's offset
	 * @return short value at offset
	 */
	public static short toShort(ByteBuffer buffer, int offset) {
		if (UNSAFE_UNALIGNED) {
			return UnsafeHelp.toShort(buffer, offset);
		} else {
			return buffer.getShort(offset);
		}
	}

	public static byte toByte(ByteBuffer buffer, int offset) {
		if (UnsafeHelp.isAvailable()) {
			return UnsafeHelp.toByte(buffer, offset);
		} else {
			return buffer.get(offset);
		}
	}

	public static void putInt(ByteBuffer buffer, int index, int val) {
		if (UNSAFE_UNALIGNED) {
			UnsafeHelp.putInt(buffer, index, val);
		} else {
			buffer.putInt(index, val);
		}
	}

	public static void putLong(ByteBuffer buffer, int index, long val) {
		if (UNSAFE_UNALIGNED) {
			UnsafeHelp.putLong(buffer, index, val);
		} else {
			buffer.putLong(index, val);
		}
	}

	/**
	 * Copy from one buffer to another from given offset. This will be absolute positional copying and
	 * won't affect the position of any of the buffers.
	 *
	 * @param in                the given buffer to read
	 * @param out               the given buffer of destination
	 * @param sourceOffset      the given buffer's offset of src
	 * @param destinationOffset the given buffer's offset of destination
	 * @param length            indicate data length
	 */
	public static int copyFromBufferToBuffer(
			@Nonnull ByteBuffer in,
			ByteBuffer out,
			int sourceOffset,
			int destinationOffset,
			int length) {
		if (in.hasArray() && out.hasArray()) {
			System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out.array(),
				out.arrayOffset() + destinationOffset, length);
		} else if (UNSAFE_AVAIL) {
			UnsafeHelp.copy(in, sourceOffset, out, destinationOffset, length);
		} else {
			int outOldPos = out.position();
			out.position(destinationOffset);
			ByteBuffer inDup = in.duplicate();
			inDup.position(sourceOffset).limit(sourceOffset + length);
			out.put(inDup);
			out.position(outOldPos);
		}
		return destinationOffset + length;
	}

	/**
	 * Copies bytes from given array's offset to length part into the given buffer. Puts the bytes
	 * to buffer's given position.
	 *
	 * @param out      the given buffer of destination
	 * @param in       array to read
	 * @param inOffset the offset at which the byte has to be read
	 * @param length   indicate data length
	 */
	public static void copyFromArrayToBuffer(ByteBuffer out, int outOffset, byte[] in, int inOffset, int length) {
		if (out.hasArray()) {
			System.arraycopy(in, inOffset, out.array(), out.arrayOffset() + outOffset, length);
		} else if (UNSAFE_AVAIL) {
			UnsafeHelp.copy(in, inOffset, out, outOffset, length);
		} else {
			int oldPos = out.position();
			out.position(outOffset);
			out.put(in, inOffset, length);
			out.position(oldPos);
		}
	}

	/**
	 * Copies specified number of bytes from given offset of 'in' ByteBuffer to
	 * the array.
	 *
	 * @param in                the given buffer to read
	 * @param out               array of destination
	 * @param sourceOffset      the offset
	 * @param destinationOffset the offset
	 * @param length            indicate data length
	 */
	public static void copyFromBufferToArray(ByteBuffer in, byte[] out, int sourceOffset, int destinationOffset,
		int length) {
		if (in.hasArray()) {
			System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out, destinationOffset, length);
		} else if (UNSAFE_AVAIL) {
			UnsafeHelp.copy(in, sourceOffset, out, destinationOffset, length);
		} else {
			int oldPos = in.position();
			in.position(sourceOffset);
			in.get(out, destinationOffset, length);
			in.position(oldPos);
		}
	}

	public static int compareTo(ByteBuffer buf1, int o1, int l1, byte[] buf2, int o2, int l2) {
		if (UNSAFE_UNALIGNED) {
			long offset1Adj;
			Object refObj1 = null;
			if (buf1.isDirect()) {
				offset1Adj = o1 + getBufferAddress(buf1);
			} else {
				offset1Adj = o1 + buf1.arrayOffset() + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET;
				refObj1 = buf1.array();
			}
			return compareToUnsafe(refObj1, offset1Adj, l1, buf2, o2 + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET, l2);
		}
		int end1 = o1 + l1;
		int end2 = o2 + l2;
		for (int i = o1, j = o2; i < end1 && j < end2; i++, j++) {
			int a = buf1.get(i) & 0xFF;
			int b = buf2[j] & 0xFF;
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}

	public static int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
		if (UNSAFE_UNALIGNED) {
			long offset1Adj, offset2Adj;
			Object refObj1 = null, refObj2 = null;
			if (buf1.isDirect()) {
				offset1Adj = o1 + getBufferAddress(buf1);
			} else {
				offset1Adj = o1 + buf1.arrayOffset() + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET;
				refObj1 = buf1.array();
			}
			if (buf2.isDirect()) {
				offset2Adj = o2 + getBufferAddress(buf2);
			} else {
				offset2Adj = o2 + buf2.arrayOffset() + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET;
				refObj2 = buf2.array();
			}
			return compareToUnsafe(refObj1, offset1Adj, l1, refObj2, offset2Adj, l2);
		}
		int end1 = o1 + l1;
		int end2 = o2 + l2;
		for (int i = o1, j = o2; i < end1 && j < end2; i++, j++) {
			int a = buf1.get(i) & 0xFF;
			int b = buf2.get(j) & 0xFF;
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}

	static int compareToUnsafe(Object obj1, long o1, int l1, Object obj2, long o2, int l2) {
		final int minLength = Math.min(l1, l2);
		final int minWords = minLength / Long.BYTES;

		/*
		 * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
		 * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
		 * 64-bit.
		 */
		int j = minWords << 3; // Same as minWords * SIZEOF_LONG
		for (int i = 0; i < j; i += Long.BYTES) {
			long lw = UnsafeHelp.UNSAFE.getLong(obj1, o1 + i);
			long rw = UnsafeHelp.UNSAFE.getLong(obj2, o2 + i);
			long diff = lw ^ rw;
			if (diff != 0) {
				return lessThanUnsignedLong(lw, rw) ? -1 : 1;
			}
		}
		int offset = j;

		if (minLength - offset >= Integer.BYTES) {
			int il = UnsafeHelp.UNSAFE.getInt(obj1, o1 + offset);
			int ir = UnsafeHelp.UNSAFE.getInt(obj2, o2 + offset);
			if (il != ir) {
				return lessThanUnsignedInt(il, ir) ? -1 : 1;
			}
			offset += Integer.BYTES;
		}
		if (minLength - offset >= Short.BYTES) {
			short sl = UnsafeHelp.UNSAFE.getShort(obj1, o1 + offset);
			short sr = UnsafeHelp.UNSAFE.getShort(obj2, o2 + offset);
			if (sl != sr) {
				return lessThanUnsignedShort(sl, sr) ? -1 : 1;
			}
			offset += Short.BYTES;
		}
		if (minLength - offset == 1) {
			int a = (UnsafeHelp.UNSAFE.getByte(obj1, o1 + offset) & 0xff);
			int b = (UnsafeHelp.UNSAFE.getByte(obj2, o2 + offset) & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}

	private static boolean lessThanUnsignedLong(long x1, long x2) {
		if (UnsafeHelp.LITTLE_ENDIAN) {
			x1 = Long.reverseBytes(x1);
			x2 = Long.reverseBytes(x2);
		}
		return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
	}

	private static boolean lessThanUnsignedInt(int x1, int x2) {
		if (UnsafeHelp.LITTLE_ENDIAN) {
			x1 = Integer.reverseBytes(x1);
			x2 = Integer.reverseBytes(x2);
		}
		return (x1 & 0xffffffffL) < (x2 & 0xffffffffL);
	}

	private static boolean lessThanUnsignedShort(short x1, short x2) {
		if (UnsafeHelp.LITTLE_ENDIAN) {
			x1 = Short.reverseBytes(x1);
			x2 = Short.reverseBytes(x2);
		}
		return (x1 & 0xffff) < (x2 & 0xffff);
	}

	/**
	 * Get address of the given {@link ByteBuffer} through reflection.
	 *
	 * @param byteBuffer the ByteBuffer instance.
	 * @return the address of the buffer.
	 */
	private static long getBufferAddress(ByteBuffer byteBuffer) {
		long address;
		try {
			address = (long) ACCESS_FIELD.get(byteBuffer);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Failed to access address method", e);
		}
		return address;
	}
}
