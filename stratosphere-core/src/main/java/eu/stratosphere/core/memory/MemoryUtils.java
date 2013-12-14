/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.core.memory;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

/**
 * Utility class for native (unsafe) memory accesses.
 */
public class MemoryUtils {
	
	/**
	 * The "unsafe", which can be used to perform native memory accesses.
	 */
	@SuppressWarnings("restriction")
	public static final sun.misc.Unsafe UNSAFE = getUnsafe();
	
	/**
	 * The native byte order of the platform on which the system currently runs.
	 */
	public static final ByteOrder NATIVE_BYTE_ORDER = getByteOrder();
	
	
	@SuppressWarnings("restriction")
	private static sun.misc.Unsafe getUnsafe() {
		try {
			Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			return (sun.misc.Unsafe) unsafeField.get(null);
		} catch (SecurityException e) {
			throw new RuntimeException("Could not access the unsafe handle.", e);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException("The static unsafe handle field was not be found.");
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Bug: Illegal argument reflection access for static field.");
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Access to the unsafe handle is forbidden by the runtime.", e);
		}
	}
	
	@SuppressWarnings("restriction")
	private static ByteOrder getByteOrder() {
		final byte[] bytes = new byte[8];
		final long value = 0x12345678900abdefL;
		UNSAFE.putLong(bytes, (long) UNSAFE.arrayBaseOffset(byte[].class), value);
		
		final int lower = bytes[0] & 0xff;
		final int higher = bytes[7] & 0xff;
		
		if (lower == 0x12 && higher == 0xef) {
			return ByteOrder.BIG_ENDIAN;
		} else if (lower == 0xef && higher == 0x12) {
			return ByteOrder.LITTLE_ENDIAN;
		} else {
			throw new RuntimeException("Unrecognized byte order.");
		}
	}
	
	
	private MemoryUtils() {}
}
