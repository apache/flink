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

package org.apache.flink.runtime.memory;

import javax.annotation.Nonnegative;

import java.util.concurrent.atomic.AtomicLong;

/** Tracker of memory reservation and release within a custom limit. */
class UnsafeMemoryBudget {

    private final long totalMemorySize;

    private final AtomicLong availableMemorySize;

    UnsafeMemoryBudget(long totalMemorySize) {
        this.totalMemorySize = totalMemorySize;
        this.availableMemorySize = new AtomicLong(totalMemorySize);
    }

    long getTotalMemorySize() {
        return totalMemorySize;
    }

    long getAvailableMemorySize() {
        return availableMemorySize.get();
    }

    boolean verifyEmpty() {
        try {
            reserveMemory(totalMemorySize);
        } catch (MemoryReservationException e) {
            return false;
        }
        releaseMemory(totalMemorySize);
        return availableMemorySize.get() == totalMemorySize;
    }

    /**
     * Reserve memory of certain size if it is available.
     *
     * <p>Adjusted version of {@link java.nio.Bits#reserveMemory(long, int)} taken from Java 11.
     */
    @SuppressWarnings({"OverlyComplexMethod", "JavadocReference", "NestedTryStatement"})
    void reserveMemory(long size) throws MemoryReservationException {
        long availableOrReserved = tryReserveMemory(size);
        // optimist!
        if (availableOrReserved >= size) {
            return;
        }
        // no luck
        throw new MemoryReservationException(
                String.format(
                        "Could not allocate %d bytes, only %d bytes are remaining. This usually indicates "
                                + "that you are requesting more memory than you have reserved. "
                                + "However, when running an old JVM version it can also be caused by slow garbage collection. "
                                + "Try to upgrade to Java 8u72 or higher if running on an old Java version.",
                        size, availableOrReserved));
    }

    private long tryReserveMemory(long size) {
        long currentAvailableMemorySize;
        while (size <= (currentAvailableMemorySize = availableMemorySize.get())) {
            if (availableMemorySize.compareAndSet(
                    currentAvailableMemorySize, currentAvailableMemorySize - size)) {
                return size;
            }
        }
        return currentAvailableMemorySize;
    }

    void releaseMemory(@Nonnegative long size) {
        if (size == 0) {
            return;
        }
        boolean released = false;
        long currentAvailableMemorySize = 0L;
        while (!released
                && totalMemorySize
                        >= (currentAvailableMemorySize = availableMemorySize.get()) + size) {
            released =
                    availableMemorySize.compareAndSet(
                            currentAvailableMemorySize, currentAvailableMemorySize + size);
        }
        if (!released) {
            throw new IllegalStateException(
                    String.format(
                            "Trying to release more managed memory (%d bytes) than has been allocated (%d bytes), the total size is %d bytes",
                            size, currentAvailableMemorySize, totalMemorySize));
        }
    }
}
