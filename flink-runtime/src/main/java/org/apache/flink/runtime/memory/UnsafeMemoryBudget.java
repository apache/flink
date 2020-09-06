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

import org.apache.flink.util.JavaGcCleanerWrapper;

import javax.annotation.Nonnegative;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracker of memory allocation and release within a custom limit.
 *
 * <p>This memory management in Flink uses the same approach as Java uses to allocate direct memory
 * and release it upon GC but memory here can be also released directly with {@link #releaseMemory(long)}.
 * The memory reservation {@link #reserveMemory(long)} tries firstly to run all phantom cleaners, created with
 * {@link org.apache.flink.core.memory.MemoryUtils#createMemoryGcCleaner(Object, long, Runnable)},
 * for objects which are ready to be garbage collected. If memory is still not available, it triggers GC and
 * continues to process any ready cleaners making {@link #MAX_SLEEPS} attempts before throwing {@link OutOfMemoryError}.
 */
class UnsafeMemoryBudget {
	private static final int MAX_SLEEPS = 11; // 2^11 - 1 = (2 x 1024) - 1 ms ~ 2 s total sleep duration
	static final int MAX_SLEEPS_VERIFY_EMPTY = 17; // 2^17 - 1 = (128 x 1024) - 1 ms ~ 2 min total sleep duration
	private static final int RETRIGGER_GC_AFTER_SLEEPS = 9; // ~ 0.5 sec

	private final long totalMemorySize;

	private final AtomicLong availableMemorySize;

	private final int verifyEmptyWaitGcMaxSleeps;

	UnsafeMemoryBudget(long totalMemorySize, int verifyEmptyWaitGcMaxSleeps) {
		this.totalMemorySize = totalMemorySize;
		this.availableMemorySize = new AtomicLong(totalMemorySize);
		this.verifyEmptyWaitGcMaxSleeps = verifyEmptyWaitGcMaxSleeps;
	}

	long getTotalMemorySize() {
		return totalMemorySize;
	}

	long getAvailableMemorySize() {
		return availableMemorySize.get();
	}

	boolean verifyEmpty() {
		try {
			// we wait longer than during the normal reserveMemory as we have to GC all memory,
			// allocated by task, to perform the verification
			reserveMemory(totalMemorySize, verifyEmptyWaitGcMaxSleeps);
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
	void reserveMemory(long size) throws MemoryReservationException {
		reserveMemory(size, MAX_SLEEPS);
	}

	/**
	 * Reserve memory of certain size if it is available.
	 *
	 * <p>If the method cannot reserve immediately, it tries to process the phantom GC cleaners queue by
	 * calling {@link JavaGcCleanerWrapper#tryRunPendingCleaners()}. If it does not help,
	 * the method calls {@link System#gc} and tries again to reserve. If it still cannot reserve,
	 * it tries to process the phantom GC cleaners queue. If there are no cleaners to process,
	 * the method sleeps the {@code maxSleeps} number of times, starting 1 ms and each time doubling
	 * the sleeping duration: 1 (0), 2 (1), 4 (2), 8 (3), 16 (4), 32 (5), 64 (6), 128 (7), 256 (8), 512 (9), ...
	 * After the {@code RETRIGGER_GC_AFTER_SLEEPS} sleeps, the method also calls {@link System#gc} before sleeping.
	 * After the {@code maxSleeps} being unable to reserve, the {@link MemoryReservationException} is thrown.
	 *
	 * <p>Adjusted version of {@link java.nio.Bits#reserveMemory(long, int)} taken from Java 11.
	 */
	@SuppressWarnings({"OverlyComplexMethod", "JavadocReference", "NestedTryStatement"})
	void reserveMemory(long size, int maxSleeps) throws MemoryReservationException {
		long availableOrReserved = tryReserveMemory(size);
		// optimist!
		if (availableOrReserved >= size) {
			return;
		}

		boolean interrupted = false;
		try {

			// Retry allocation until success or there are no more
			// references (including Cleaners that might free direct
			// buffer memory) to process and allocation still fails.
			boolean refprocActive;
			do {
				try {
					refprocActive = JavaGcCleanerWrapper.tryRunPendingCleaners();
				} catch (InterruptedException e) {
					// Defer interrupts and keep trying.
					interrupted = true;
					refprocActive = true;
				}
				availableOrReserved = tryReserveMemory(size);
				if (availableOrReserved >= size) {
					return;
				}
			} while (refprocActive);

			// trigger VM's Reference processing
			System.gc();

			// A retry loop with exponential back-off delays.
			// Sometimes it would suffice to give up once reference
			// processing is complete.  But if there are many threads
			// competing for memory, this gives more opportunities for
			// any given thread to make progress.  In particular, this
			// seems to be enough for a stress test like
			// DirectBufferAllocTest to (usually) succeed, while
			// without it that test likely fails.  Since failure here
			// ends in MemoryReservationException, there's no need to hurry.
			long sleepTime = 1;
			int sleeps = 0;
			while (true) {
				availableOrReserved = tryReserveMemory(size);
				if (availableOrReserved >= size) {
					return;
				}
				if (sleeps >= maxSleeps) {
					break;
				}
				try {
					if (!JavaGcCleanerWrapper.tryRunPendingCleaners()) {
						if (sleeps >= RETRIGGER_GC_AFTER_SLEEPS) {
							// trigger again VM's Reference processing if we have to wait longer
							System.gc();
						}
						Thread.sleep(sleepTime);
						sleepTime <<= 1;
						sleeps++;
					}
				} catch (InterruptedException e) {
					interrupted = true;
				}
			}

			// no luck
			throw new MemoryReservationException(String.format(
				"Could not allocate %d bytes, only %d bytes are remaining. This usually indicates " +
					"that you are requesting more memory than you have reserved. " +
					"However, when running an old JVM version it can also be caused by slow garbage collection. " +
					"Try to upgrade to Java 8u72 or higher if running on an old Java version.",
				size,
				availableOrReserved));

		} finally {
			if (interrupted) {
				// don't swallow interrupts
				Thread.currentThread().interrupt();
			}
		}
	}

	private long tryReserveMemory(long size) {
		long currentAvailableMemorySize;
		while (size <= (currentAvailableMemorySize = availableMemorySize.get())) {
			if (availableMemorySize.compareAndSet(currentAvailableMemorySize, currentAvailableMemorySize - size)) {
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
		while (!released && totalMemorySize >= (currentAvailableMemorySize = availableMemorySize.get()) + size) {
			released = availableMemorySize
				.compareAndSet(currentAvailableMemorySize, currentAvailableMemorySize + size);
		}
		if (!released) {
			throw new IllegalStateException(String.format(
				"Trying to release more managed memory (%d bytes) than has been allocated (%d bytes), the total size is %d bytes",
				size,
				currentAvailableMemorySize,
				totalMemorySize));
		}
	}
}
