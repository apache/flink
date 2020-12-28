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
import org.apache.flink.util.JavaGcCleanerWrapper;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Utility class for memory operations. */
@Internal
public class MemoryUtils {

    /** The "unsafe", which can be used to perform native memory accesses. */
    @SuppressWarnings({"restriction", "UseOfSunClasses"})
    public static final sun.misc.Unsafe UNSAFE = getUnsafe();

    /** The native byte order of the platform on which the system currently runs. */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    private static final long BUFFER_ADDRESS_FIELD_OFFSET =
            getClassFieldOffset(Buffer.class, "address");
    private static final long BUFFER_CAPACITY_FIELD_OFFSET =
            getClassFieldOffset(Buffer.class, "capacity");
    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS =
            getClassByName("java.nio.DirectByteBuffer");

    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        try {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (SecurityException e) {
            throw new Error(
                    "Could not access the sun.misc.Unsafe handle, permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error("The static handle field in sun.misc.Unsafe was not found.", e);
        } catch (IllegalArgumentException e) {
            throw new Error("Bug: Illegal argument reflection access for static field.", e);
        } catch (IllegalAccessException e) {
            throw new Error("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
        } catch (Throwable t) {
            throw new Error(
                    "Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
        }
    }

    private static long getClassFieldOffset(
            @SuppressWarnings("SameParameterValue") Class<?> cl, String fieldName) {
        try {
            return UNSAFE.objectFieldOffset(cl.getDeclaredField(fieldName));
        } catch (SecurityException e) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName)
                            + ", permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error(getClassFieldOffsetErrorMessage(cl, fieldName), e);
        } catch (Throwable t) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName) + ", unclassified error", t);
        }
    }

    private static String getClassFieldOffsetErrorMessage(Class<?> cl, String fieldName) {
        return "Could not get field '"
                + fieldName
                + "' offset in class '"
                + cl
                + "' for unsafe operations";
    }

    private static Class<?> getClassByName(
            @SuppressWarnings("SameParameterValue") String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new Error("Could not find class '" + className + "' for unsafe operations.", e);
        }
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
     * <p>When memory owner becomes <a href="package-summary.html#reachability">phantom
     * reachable</a>, GC will release the underlying unsafe memory if not released yet.
     *
     * @param owner memory owner which phantom reaching is to monitor by GC and release the unsafe
     *     memory
     * @param address address of the unsafe memory to release
     * @return action to run to release the unsafe memory manually
     */
    static Runnable createMemoryGcCleaner(Object owner, long address, Runnable customCleanup) {
        return JavaGcCleanerWrapper.createCleaner(
                owner,
                () -> {
                    releaseUnsafe(address);
                    customCleanup.run();
                });
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
            ByteBuffer buffer = (ByteBuffer) UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
            UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
            buffer.clear();
            return buffer;
        } catch (Throwable t) {
            throw new Error("Failed to wrap unsafe off-heap memory with ByteBuffer", t);
        }
    }

    /**
     * Get native memory address wrapped by the given {@link ByteBuffer}.
     *
     * @param buffer {@link ByteBuffer} which wraps the native memory address to get
     * @return native memory address wrapped by the given {@link ByteBuffer}
     */
    static long getByteBufferAddress(ByteBuffer buffer) {
        Preconditions.checkNotNull(buffer, "buffer is null");
        Preconditions.checkArgument(
                buffer.isDirect(), "Can't get address of a non-direct ByteBuffer.");
        try {
            return UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
        } catch (Throwable t) {
            throw new Error("Could not access direct byte buffer address field.", t);
        }
    }

    /** Should not be instantiated. */
    private MemoryUtils() {}
}
