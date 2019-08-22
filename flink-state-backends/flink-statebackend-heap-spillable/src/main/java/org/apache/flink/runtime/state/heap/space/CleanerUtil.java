/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap.space;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodType.methodType;

/**
 * Direct buffer cleaner tool, mainly copied from Hadoop project (see org.apache.hadoop.util.CleanerUtil).
 *
 * <p/>sun.misc.Cleaner has moved in OpenJDK 9 and
 * sun.misc.Unsafe#invokeCleaner(ByteBuffer) is the replacement.
 * This class is a hack to use sun.misc.Cleaner in Java 8 and
 * use the replacement in Java 9+.
 * This implementation is inspired by LUCENE-6989.
 */
final class CleanerUtil {

	// Prevent instantiation
	private CleanerUtil(){}

	private static final BufferCleaner CLEANER;

	/**
	 * Reference to a BufferCleaner that does un-mapping.
	 *
	 * @return {@code null} if not supported.
	 */
	static BufferCleaner getCleaner() {
		return CLEANER;
	}

	static {
		final Object hack = AccessController.doPrivileged(
			(PrivilegedAction<Object>) CleanerUtil::unmapHackImpl);
		if (hack instanceof BufferCleaner) {
			CLEANER = (BufferCleaner) hack;
		} else {
			CLEANER = null;
		}
	}

	private static Object unmapHackImpl() {
		final MethodHandles.Lookup lookup = MethodHandles.lookup();
		try {
			try {
				// *** sun.misc.Unsafe un-mapping (Java 9+) ***
				final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
				// first check if Unsafe has the right method, otherwise we can
				// give up without doing any security critical stuff:
				//noinspection JavaLangInvokeHandleSignature
				final MethodHandle unMapper = lookup.findVirtual(unsafeClass,
					"invokeCleaner", methodType(void.class, ByteBuffer.class));
				// fetch the unsafe instance and bind it to the virtual MH:
				final Field f = unsafeClass.getDeclaredField("theUnsafe");
				f.setAccessible(true);
				final Object theUnsafe = f.get(null);
				return newBufferCleaner(ByteBuffer.class, unMapper.bindTo(theUnsafe));
			} catch (SecurityException se) {
				// rethrow to report errors correctly (we need to catch it here,
				// as we also catch RuntimeException below!):
				throw se;
			} catch (ReflectiveOperationException | RuntimeException e) {
				// *** sun.misc.Cleaner un-mapping (Java 8) ***
				final Class<?> directBufferClass =
					Class.forName("java.nio.DirectByteBuffer");

				final Method m = directBufferClass.getMethod("cleaner");
				m.setAccessible(true);
				final MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
				final Class<?> cleanerClass =
					directBufferCleanerMethod.type().returnType();

				/*
				 * "Compile" a MethodHandle that basically is equivalent
				 * to the following code:
				 *
				 * void unMapper(ByteBuffer byteBuffer) {
				 *   sun.misc.Cleaner cleaner =
				 *       ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
				 *   if (Objects.nonNull(cleaner)) {
				 *     cleaner.clean();
				 *   } else {
				 *     // the noop is needed because MethodHandles#guardWithTest
				 *     // always needs ELSE
				 *     noop(cleaner);
				 *   }
				 * }
				 */
				final MethodHandle cleanMethod = lookup.findVirtual(
					cleanerClass, "clean", methodType(void.class));
				final MethodHandle nonNullTest = lookup.findStatic(Objects.class,
					"nonNull", methodType(boolean.class, Object.class))
					.asType(methodType(boolean.class, cleanerClass));
				final MethodHandle noop = dropArguments(
					constant(Void.class, null).asType(methodType(void.class)),
					0, cleanerClass);
				final MethodHandle unMapper = filterReturnValue(
					directBufferCleanerMethod,
					guardWithTest(nonNullTest, cleanMethod, noop))
					.asType(methodType(void.class, ByteBuffer.class));
				return newBufferCleaner(directBufferClass, unMapper);
			}
		} catch (SecurityException se) {
			return "Un-mapping is not supported, because not all required " +
				"permissions are given to the Flink JAR file: " + se +
				" [Please grant at least the following permissions: " +
				"RuntimePermission(\"accessClassInPackage.sun.misc\") " +
				" and ReflectPermission(\"suppressAccessChecks\")]";
		} catch (ReflectiveOperationException | RuntimeException e) {
			return "Un-mapping is not supported on this platform, " +
				"because internal Java APIs are not compatible with " +
				"this Flink version: " + e;
		}
	}

	private static BufferCleaner newBufferCleaner(
		final Class<?> unMappableBufferClass, final MethodHandle unMapper) {
		assert Objects.equals(
			methodType(void.class, ByteBuffer.class), unMapper.type());
		return buffer -> {
			if (!buffer.isDirect()) {
				throw new IllegalArgumentException(
					"un-mapping only works with direct buffers");
			}
			if (!unMappableBufferClass.isInstance(buffer)) {
				throw new IllegalArgumentException("buffer is not an instance of " +
					unMappableBufferClass.getName());
			}
			final Throwable error = AccessController.doPrivileged(
				(PrivilegedAction<Throwable>) () -> {
					try {
						unMapper.invokeExact(buffer);
						return null;
					} catch (Throwable t) {
						return t;
					}
				});
			if (error != null) {
				throw new IOException("Unable to unmap the mapped buffer", error);
			}
		};
	}

	/**
	 * Pass in an implementation of this interface to cleanup ByteBuffers.
	 * CleanerUtil implements this to allow un-mapping of byte buffers
	 * with private Java APIs.
	 */
	@FunctionalInterface
	public interface BufferCleaner {
		void freeBuffer(ByteBuffer b) throws IOException;
	}
}
