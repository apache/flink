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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Unsafe use help.
 */
@SuppressWarnings({"WeakerAccess", "UnusedReturnValue"})
public class UnsafeHelp {
	private static final Logger LOG = LoggerFactory.getLogger(UnsafeHelp.class);

	static final Unsafe UNSAFE;
	private static boolean unaligned;

	/**
	 * The offset to the first element in a byte array.
	 */
	public static final long BYTE_ARRAY_BASE_OFFSET;

	static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

	// This number limits the number of bytes to copy per call to Unsafe's
	// copyMemory method. A limit is imposed to allow for savepoint polling
	// during a large copy
	static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

	static {
		UNSAFE = (Unsafe) AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
			try {
				Field f = Unsafe.class.getDeclaredField("theUnsafe");
				f.setAccessible(true);
				return f.get(null);
			} catch (Throwable e) {
				LOG.warn("sun.misc.Unsafe is not accessible", e);
			}
			return null;
		});

		if (UNSAFE != null) {
			BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
			try {
				// Using java.nio.Bits#unaligned() to check for unaligned-access capability
				Class<?> clazz = Class.forName("java.nio.Bits");
				Method m = clazz.getDeclaredMethod("unaligned");
				m.setAccessible(true);
				unaligned = (boolean) m.invoke(null);
			} catch (Exception e) {
				unaligned = false; // FindBugs: Causes REC_CATCH_EXCEPTION. Suppressed.
			}
		} else {
			BYTE_ARRAY_BASE_OFFSET = -1;
			unaligned = false;
		}
	}

	private UnsafeHelp() {
	}

	/**
	 * @return true when the running JVM is having sun's Unsafe package available in it.
	 */
	public static boolean isAvailable() {
		return UNSAFE != null;
	}

	/**
	 * @return true when running JVM is having sun's Unsafe package available in it and underlying
	 * system having unaligned-access capability.
	 */
	public static boolean unaligned() {
		return unaligned;
	}

	// APIs to read primitive data from a byte[] using Unsafe way

	/**
	 * Reads an int value at the given buffer's offset considering it was written in big-endian
	 * format.
	 *
	 * @param buf    the buffer to read from
	 * @param offset the offset to read from
	 * @return int value at offset
	 */
	public static int toInt(ByteBuffer buf, int offset) {
		if (LITTLE_ENDIAN) {
			return Integer.reverseBytes(getAsInt(buf, offset));
		}
		return getAsInt(buf, offset);
	}

	/**
	 * Reads bytes at the given offset as an int value.
	 *
	 * @param buf    the buffer to read from
	 * @param offset the offset to read from
	 * @return int value at offset
	 */
	static int getAsInt(ByteBuffer buf, int offset) {
		if (buf.isDirect()) {
			return UNSAFE.getInt(((DirectBuffer) buf).address() + offset);
		}
		return UNSAFE.getInt(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
	}

	/**
	 * Reads a long value at the given buffer's offset considering it was written in big-endian
	 * format.
	 *
	 * @param buf    the buffer to read from
	 * @param offset the offset to read from
	 * @return long value at offset
	 */
	public static long toLong(ByteBuffer buf, int offset) {
		if (LITTLE_ENDIAN) {
			return Long.reverseBytes(getAsLong(buf, offset));
		}
		return getAsLong(buf, offset);
	}

	/**
	 * Reads bytes at the given offset as a long value.
	 *
	 * @param buf    the buffer to read from
	 * @param offset the offset to read from
	 * @return long value at offset
	 */
	static long getAsLong(ByteBuffer buf, int offset) {
		if (buf.isDirect()) {
			return UNSAFE.getLong(((DirectBuffer) buf).address() + offset);
		}
		return UNSAFE.getLong(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
	}

	/**
	 * Reads a short value at the given buffer's offset considering it was written in big-endian
	 * format.
	 *
	 * @param buf    the buffer to read from
	 * @param offset the offset to read from
	 * @return short value at offset
	 */
	public static short toShort(ByteBuffer buf, int offset) {
		if (LITTLE_ENDIAN) {
			return Short.reverseBytes(getAsShort(buf, offset));
		}
		return getAsShort(buf, offset);
	}

	/**
	 * Reads bytes at the given offset as a short value.
	 *
	 * @param buf    the buffer to read from
	 * @param offset the offset to read from
	 * @return short value at offset
	 */
	static short getAsShort(ByteBuffer buf, int offset) {
		if (buf.isDirect()) {
			return UNSAFE.getShort(((DirectBuffer) buf).address() + offset);
		}
		return UNSAFE.getShort(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
	}

	/**
	 * Returns the byte at the given offset.
	 *
	 * @param buf    the buffer to read
	 * @param offset the offset at which the byte has to be read
	 * @return the byte at the given offset
	 */
	public static byte toByte(ByteBuffer buf, int offset) {
		if (buf.isDirect()) {
			return UNSAFE.getByte(((DirectBuffer) buf).address() + offset);
		} else {
			return UNSAFE.getByte(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
		}
	}

	/**
	 * Put an int value out to the specified ByteBuffer offset in big-endian format.
	 *
	 * @param buf    the ByteBuffer to write to
	 * @param offset offset in the ByteBuffer
	 * @param val    int to write out
	 * @return incremented offset
	 */
	public static int putInt(ByteBuffer buf, int offset, int val) {
		if (LITTLE_ENDIAN) {
			val = Integer.reverseBytes(val);
		}
		if (buf.isDirect()) {
			UNSAFE.putInt(((DirectBuffer) buf).address() + offset, val);
		} else {
			UNSAFE.putInt(buf.array(), offset + buf.arrayOffset() + BYTE_ARRAY_BASE_OFFSET, val);
		}
		return offset + Integer.BYTES;
	}

	/**
	 * Put a long value out to the specified BB position in big-endian format.
	 *
	 * @param buf    the byte buffer
	 * @param offset position in the buffer
	 * @param val    long to write out
	 * @return incremented offset
	 */
	public static int putLong(ByteBuffer buf, int offset, long val) {
		if (LITTLE_ENDIAN) {
			val = Long.reverseBytes(val);
		}
		if (buf.isDirect()) {
			UNSAFE.putLong(((DirectBuffer) buf).address() + offset, val);
		} else {
			UNSAFE.putLong(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
		}
		return offset + Long.BYTES;
	}

	/**
	 * Copies specified number of bytes from given offset of {@code src} buffer into the {@code dest}
	 * buffer.
	 *
	 * @param src        the buffer to copy from
	 * @param srcOffset  the offset of the source buffer to copy from
	 * @param dest       the buffer to copy to
	 * @param destOffset the start offset of the target buffer to copy to
	 * @param length     the length of the data to copy
	 */
	public static void copy(ByteBuffer src, int srcOffset, ByteBuffer dest, int destOffset, int length) {
		long srcAddress, destAddress;
		Object srcBase = null, destBase = null;
		if (src.isDirect()) {
			srcAddress = srcOffset + ((DirectBuffer) src).address();
		} else {
			srcAddress = srcOffset + src.arrayOffset() + BYTE_ARRAY_BASE_OFFSET;
			srcBase = src.array();
		}
		if (dest.isDirect()) {
			destAddress = destOffset + ((DirectBuffer) dest).address();
		} else {
			destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
			destBase = dest.array();
		}
		unsafeCopy(srcBase, srcAddress, destBase, destAddress, length);
	}

	/**
	 * Copies specified number of bytes from given offset of {@code src} ByteBuffer to the
	 * {@code dest} array.
	 *
	 * @param src        the buffer to copy from
	 * @param srcOffset  the offset of the source buffer to copy from
	 * @param dest       the buffer to copy to
	 * @param destOffset the start offset of the target buffer to copy to
	 * @param length     the length of the data to copy
	 */
	public static void copy(ByteBuffer src, int srcOffset, byte[] dest, int destOffset, int length) {
		long srcAddress = srcOffset;
		Object srcBase = null;
		if (src.isDirect()) {
			srcAddress = srcAddress + ((DirectBuffer) src).address();
		} else {
			srcAddress = srcAddress + BYTE_ARRAY_BASE_OFFSET + src.arrayOffset();
			srcBase = src.array();
		}
		long destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET;
		unsafeCopy(srcBase, srcAddress, dest, destAddress, length);
	}

	/**
	 * Copies the bytes from given array's offset to length part into the given buffer.
	 *
	 * @param src        the buffer to copy from
	 * @param srcOffset  the offset of the source buffer to copy from
	 * @param dest       the buffer to copy to
	 * @param destOffset the start offset of the target buffer to copy to
	 * @param length     the length of the data to copy
	 */
	public static void copy(byte[] src, int srcOffset, ByteBuffer dest, int destOffset, int length) {
		long destAddress = destOffset;
		Object destBase = null;
		if (dest.isDirect()) {
			destAddress = destAddress + ((DirectBuffer) dest).address();
		} else {
			destAddress = destAddress + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
			destBase = dest.array();
		}
		long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
		unsafeCopy(src, srcAddress, destBase, destAddress, length);
	}

	private static void unsafeCopy(Object src, long srcAddress, Object dst, long destAddress, long len) {
		while (len > 0) {
			long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
			UNSAFE.copyMemory(src, srcAddress, dst, destAddress, len);
			len -= size;
			srcAddress += size;
			destAddress += size;
		}
	}
}
