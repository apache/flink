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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemoryUtils;

import sun.misc.Unsafe;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An object that can be reference counted, the internal resource would be released when the
 * reference count reaches zero. This class is designed to be high-performance, lock-free and
 * thread-safe.
 */
@Internal
@ThreadSafe
public abstract class ReferenceCounted<ReleaseHelper> {

    /** The "unsafe", which can be used to perform native memory accesses. */
    @SuppressWarnings({"restriction", "UseOfSunClasses"})
    private static final Unsafe unsafe = MemoryUtils.UNSAFE;

    private static final long referenceOffset;

    static {
        try {
            referenceOffset =
                    unsafe.objectFieldOffset(
                            ReferenceCounted.class.getDeclaredField("referenceCount"));
        } catch (SecurityException e) {
            throw new Error(
                    "Could not get field 'referenceCount' offset in class 'ReferenceCounted'"
                            + " for unsafe operations, permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error(
                    "Could not get field 'referenceCount' offset in class 'ReferenceCounted'"
                            + " for unsafe operations",
                    e);
        } catch (Throwable t) {
            throw new Error(
                    "Could not get field 'referenceCount' offset in class 'ReferenceCounted'"
                            + " for unsafe operations, unclassified error",
                    t);
        }
    }

    private volatile int referenceCount;

    public ReferenceCounted(int initReference) {
        this.referenceCount = initReference;
    }

    public int retain() {
        return unsafe.getAndAddInt(this, referenceOffset, 1) + 1;
    }

    /**
     * Try to retain this object. Fail if reference count is already zero.
     *
     * @return zero if failed, otherwise current reference count.
     */
    public int tryRetain() {
        int v;
        do {
            v = unsafe.getIntVolatile(this, referenceOffset);
        } while (v != 0 && !unsafe.compareAndSwapInt(this, referenceOffset, v, v + 1));
        return v == 0 ? 0 : v + 1;
    }

    public int release() {
        return release(null);
    }

    public int release(@Nullable ReleaseHelper releaseHelper) {
        int r = unsafe.getAndAddInt(this, referenceOffset, -1) - 1;
        if (r == 0) {
            referenceCountReachedZero(releaseHelper);
        }
        return r;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    /** A method called when the reference count reaches zero. */
    protected abstract void referenceCountReachedZero(@Nullable ReleaseHelper releaseHelper);
}
