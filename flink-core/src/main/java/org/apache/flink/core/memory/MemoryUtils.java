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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for memory operations.
 */
@Internal
public class MemoryUtils {

	/** The "unsafe", which can be used to perform native memory accesses. */
	@SuppressWarnings("restriction")
	public static final sun.misc.Unsafe UNSAFE = getUnsafe();

	/** The native byte order of the platform on which the system currently runs. */
	public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

	private static final Constructor<?> DIRECT_BUFFER_CONSTRUCTOR = getDirectBufferPrivateConstructor();

	@SuppressWarnings("restriction")
	private static sun.misc.Unsafe getUnsafe() {
		try {
			Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			return (sun.misc.Unsafe) unsafeField.get(null);
		} catch (SecurityException e) {
			throw new RuntimeException("Could not access the sun.misc.Unsafe handle, permission denied by security manager.", e);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException("The static handle field in sun.misc.Unsafe was not found.");
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Bug: Illegal argument reflection access for static field.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
		} catch (Throwable t) {
			throw new RuntimeException("Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
		}
	}

	/** Should not be instantiated. */
	private MemoryUtils() {}

	private static Constructor<? extends ByteBuffer> getDirectBufferPrivateConstructor() {
		//noinspection OverlyBroadCatchBlock
		try {
			Constructor<? extends ByteBuffer> constructor =
				ByteBuffer.allocateDirect(1).getClass().getDeclaredConstructor(long.class, int.class);
			constructor.setAccessible(true);
			return constructor;
		} catch (NoSuchMethodException e) {
			ExceptionUtils.rethrow(
				e,
				"The private constructor java.nio.DirectByteBuffer.<init>(long, int) is not available.");
		} catch (SecurityException e) {
			ExceptionUtils.rethrow(
				e,
				"The private constructor java.nio.DirectByteBuffer.<init>(long, int) is not available, " +
					"permission denied by security manager");
		} catch (Throwable t) {
			ExceptionUtils.rethrow(
				t,
				"Unclassified error while trying to access private constructor " +
					"java.nio.DirectByteBuffer.<init>(long, int).");
		}
		throw new RuntimeException("unexpected to avoid returning null");
	}

	/**
	 * Allocates unsafe native memory.
	 *
	 * @param size size of the unsafe memory to allocate.
	 * @return address of the allocated unsafe memory
	 */
	static long allocateUnsafe(long size) {
		return UNSAFE.allocateMemory(Math.max(1L, size));
	}

	/**
	 * Creates a cleaner to release the unsafe memory by VM GC.
	 *
	 * <p>When memory owner becomes <a href="package-summary.html#reachability">phantom reachable</a>,
	 * GC will release the underlying unsafe memory if not released yet.
	 *
	 * @param owner memory owner which phantom reaching is to monitor by GC and release the unsafe memory
	 * @param address address of the unsafe memory to release
	 * @return action to run to release the unsafe memory manually
	 */
	@SuppressWarnings("UseOfSunClasses")
	static Runnable createMemoryGcCleaner(Object owner, long address) {
		// TODO: rollbacked to direct memory because of FLINK-13985
		// The release call is wrapped with the sun.misc.Cleaner
		// which will schedule it before GC is run for the owner object (not reachable in user code).
		// but only if sun.misc.Cleaner::clean has not been already called explicitly by user before.
		// If sun.misc.Cleaner::clean is called after GC it will not call the release.
		// This way we guarantee that there will always be a release at some point but only once.
		return null; // sun.misc.Cleaner.create(owner, () -> releaseUnsafe(address))::clean;
	}

	private static void releaseUnsafe(long address) {
		UNSAFE.freeMemory(address);
	}

	/**
	 * Wraps the unsafe native memory with a {@link ByteBuffer}.
	 *
	 * @param address address of the unsafe memory to wrap
	 * @param size size of the unsafe memory to wrap
	 * @return a {@link ByteBuffer} which is a view of the given unsafe memory
	 */
	static ByteBuffer wrapUnsafeMemoryWithByteBuffer(long address, int size) {
		//noinspection OverlyBroadCatchBlock
		try {
			return (ByteBuffer) DIRECT_BUFFER_CONSTRUCTOR.newInstance(address, size);
		} catch (Throwable t) {
			ExceptionUtils.rethrow(t, "Failed to wrap unsafe off-heap memory with ByteBuffer");
		}
		throw new RuntimeException("unexpected to avoid returning null");
	}
}
