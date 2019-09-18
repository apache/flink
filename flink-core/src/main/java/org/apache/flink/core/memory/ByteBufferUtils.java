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

package org.apache.flink.core.memory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Utilities to get/put data to {@link ByteBuffer}. All methods don't change
 * byte buffer's position.
 *
 * <p/> This class partially refers to org.apache.hadoop.hbase.util.ByteBufferUtils.
 *
 * @see <a href=https://github.com/apache/hbase/blob/master/hbase-common/src/main/java/org/apache/hadoop/hbase/util/ByteBufferUtils.java>github source</a>
 */
public class ByteBufferUtils {

	private static final boolean UNSAFE_AVAIL = UnsafeHelp.isAvailable();
	private static final boolean UNSAFE_UNALIGNED = UnsafeHelp.unaligned();
	private static final Field ACCESS_FIELD;

	static {
		try {
			ACCESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");
			ACCESS_FIELD.setAccessible(true);
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
	 * @param sourceOffset      the given buffer's offset of src
	 * @param out               the given buffer of destination
	 * @param destinationOffset the given buffer's offset of destination
	 * @param length            indicate data length
	 */
	public static void copyFromBufferToBuffer(
			@Nonnull ByteBuffer in,
			int sourceOffset,
			ByteBuffer out,
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
	}

	/**
	 * Copies bytes from given array's offset to length part into the given buffer. Puts the bytes
	 * to buffer's given position.
	 *
	 * @param in       array to read
	 * @param inOffset the offset at which the byte has to be read
	 * @param out      the given buffer of destination
	 * @param length   indicate data length
	 */
	public static void copyFromArrayToBuffer(byte[] in, int inOffset, ByteBuffer out, int outOffset, int length) {
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
	 * @param sourceOffset      the offset
	 * @param out               array of destination
	 * @param destinationOffset the offset
	 * @param length            indicate data length
	 */
	public static void copyFromBufferToArray(
			ByteBuffer in,
			int sourceOffset,
			byte[] out,
			int destinationOffset,
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

	/**
	 * Compare the content of a {@link ByteBuffer} with that of a byte array.
	 *
	 * @param srcBuffer  the source ByteBuffer
	 * @param srcOffset  the source offset to start from for comparison.
	 * @param srcLength  the source length to compare.
	 * @param dest       the dest byte array.
	 * @param destOffset the dest offset to start from for comparison.
	 * @param destLength the dest length to compare.
	 * @return 0 if equals, 1 if the source is bigger by bytes order, or -1 if the dest is bigger.
	 */
	public static int compareTo(
			ByteBuffer srcBuffer,
			int srcOffset,
			int srcLength,
			byte[] dest,
			int destOffset,
			int destLength) {
		if (UNSAFE_UNALIGNED) {
			long srcOffsetAddress;
			Object srcArray = null;
			if (srcBuffer.isDirect()) {
				srcOffsetAddress = srcOffset + getBufferAddress(srcBuffer);
			} else {
				srcOffsetAddress = srcOffset + srcBuffer.arrayOffset() + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET;
				srcArray = srcBuffer.array();
			}
			return compareToUnsafe(srcArray, srcOffsetAddress, srcLength,
				dest, destOffset + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET, destLength);
		}
		int srcEnd = srcOffset + srcLength;
		int destEnd = destOffset + destLength;
		for (int i = srcOffset, j = destOffset; i < srcEnd && j < destEnd; i++, j++) {
			int a = srcBuffer.get(i) & 0xFF;
			int b = dest[j] & 0xFF;
			if (a != b) {
				return a - b;
			}
		}
		return srcLength - destLength;
	}

	/**
	 * Compare two {@link ByteBuffer}.
	 *
	 * @param srcBuffer  the source ByteBuffer.
	 * @param srcOffset  the source offset to start from for comparison.
	 * @param srcLength  the source length to compare.
	 * @param destBuffer the dest ByteBuffer.
	 * @param destOffset the dest offset to start from for comparison.
	 * @param destLength the dest length to compare.
	 * @return 0 if equals, positive if the source is bigger by bytes order, or negative if the dest is bigger.
	 */
	public static int compareTo(
			ByteBuffer srcBuffer,
			int srcOffset,
			int srcLength,
			ByteBuffer destBuffer,
			int destOffset,
			int destLength) {
		if (UNSAFE_UNALIGNED) {
			long srcOffsetAddress, destOffsetAddress;
			Object srcArray = null, destArray = null;
			if (srcBuffer.isDirect()) {
				srcOffsetAddress = srcOffset + getBufferAddress(srcBuffer);
			} else {
				srcOffsetAddress = srcOffset + srcBuffer.arrayOffset() + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET;
				srcArray = srcBuffer.array();
			}
			if (destBuffer.isDirect()) {
				destOffsetAddress = destOffset + getBufferAddress(destBuffer);
			} else {
				destOffsetAddress = destOffset + destBuffer.arrayOffset() + UnsafeHelp.BYTE_ARRAY_BASE_OFFSET;
				destArray = destBuffer.array();
			}
			return compareToUnsafe(srcArray, srcOffsetAddress, srcLength, destArray, destOffsetAddress, destLength);
		}
		int end1 = srcOffset + srcLength;
		int end2 = destOffset + destLength;
		for (int i = srcOffset, j = destOffset; i < end1 && j < end2; i++, j++) {
			int a = srcBuffer.get(i) & 0xFF;
			int b = destBuffer.get(j) & 0xFF;
			if (a != b) {
				return a - b;
			}
		}
		return srcLength - destLength;
	}

	/**
	 * Compare with {@link sun.misc.Unsafe} methods.
	 *
	 * @param src the source object, will use the offset as an absolute address if null.
	 * @param srcOffset the source offset to start from for comparison.
	 * @param srcLength the source length to compare.
	 * @param dest the dest object, will use the offset as an absolute address if null.
	 * @param destOffset the dest offset to start from for comparison.
	 * @param destLength the dest length to compare.
	 * @return 0 if equals, 1 if the source is bigger by bytes order, or -1 if the dest is bigger.
	 */
	private static int compareToUnsafe(
			@Nullable Object src,
			long srcOffset,
			int srcLength,
			@Nullable Object dest,
			long destOffset,
			int destLength) {
		final int minLength = Math.min(srcLength, destLength);
		final int minWords = minLength / Long.BYTES;

		/*
		 * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
		 * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
		 * 64-bit.
		 */
		int j = minWords << 3; // Same as minWords * SIZEOF_LONG
		for (int i = 0; i < j; i += Long.BYTES) {
			long lw = UnsafeHelp.UNSAFE.getLong(src, srcOffset + i);
			long rw = UnsafeHelp.UNSAFE.getLong(dest, destOffset + i);
			long diff = lw ^ rw;
			if (diff != 0) {
				return lessThanUnsignedLong(lw, rw) ? -1 : 1;
			}
		}
		int offset = j;

		if (minLength - offset >= Integer.BYTES) {
			int il = UnsafeHelp.UNSAFE.getInt(src, srcOffset + offset);
			int ir = UnsafeHelp.UNSAFE.getInt(dest, destOffset + offset);
			if (il != ir) {
				return lessThanUnsignedInt(il, ir) ? -1 : 1;
			}
			offset += Integer.BYTES;
		}
		if (minLength - offset >= Short.BYTES) {
			short sl = UnsafeHelp.UNSAFE.getShort(src, srcOffset + offset);
			short sr = UnsafeHelp.UNSAFE.getShort(dest, destOffset + offset);
			if (sl != sr) {
				return lessThanUnsignedShort(sl, sr) ? -1 : 1;
			}
			offset += Short.BYTES;
		}
		if (minLength - offset == 1) {
			int a = (UnsafeHelp.UNSAFE.getByte(src, srcOffset + offset) & 0xff);
			int b = (UnsafeHelp.UNSAFE.getByte(dest, destOffset + offset) & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return srcLength - destLength;
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
	static long getBufferAddress(ByteBuffer byteBuffer) {
		long address;
		try {
			address = (long) ACCESS_FIELD.get(byteBuffer);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Failed to access address field", e);
		}
		return address;
	}
}
