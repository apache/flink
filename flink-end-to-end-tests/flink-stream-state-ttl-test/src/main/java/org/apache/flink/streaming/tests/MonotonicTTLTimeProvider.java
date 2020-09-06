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

package org.apache.flink.streaming.tests;

import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A stub implementation of a {@link TtlTimeProvider} which guarantees that
 * processing time increases monotonically.
 */
@NotThreadSafe
final class MonotonicTTLTimeProvider implements TtlTimeProvider, Serializable {

	private static final long serialVersionUID = 1L;

	/*
	 * The following variables are static because the whole TTLTimeProvider will go
	 * through serialization and, eventually, the state backend and the task executing
	 * the TtlVerifyUpdateFunction will have different instances of it.
	 *
	 * If these were not static, then the TtlVerifyUpdateFunction would e.g. freeze
	 * the time, but the backend would not be notified about it, resulting in inconsistent
	 * state.
	 *
	 * We have to add synchronization because the time provider is also accessed concurrently
	 * from RocksDB compaction filter threads.
	 */

	private static boolean timeIsFrozen = false;

	private static long lastReturnedProcessingTime = Long.MIN_VALUE;

	private static final Object lock = new Object();

	@GuardedBy("lock")
	static <T, E extends Throwable> T doWithFrozenTime(FunctionWithException<Long, T, E> action) throws E {
		synchronized (lock) {
			final long timestampBeforeUpdate = freeze();
			T result = action.apply(timestampBeforeUpdate);
			final long timestampAfterUpdate = unfreezeTime();

			checkState(timestampAfterUpdate == timestampBeforeUpdate,
				"Timestamps before and after the update do not match.");
			return result;
		}
	}

	private static long freeze() {
		if (!timeIsFrozen || lastReturnedProcessingTime == Long.MIN_VALUE) {
			timeIsFrozen = true;
			return getCurrentTimestamp();
		} else {
			return lastReturnedProcessingTime;
		}
	}

	@Override
	@GuardedBy("lock")
	public long currentTimestamp() {
		synchronized (lock) {
			if (timeIsFrozen && lastReturnedProcessingTime != Long.MIN_VALUE) {
				return lastReturnedProcessingTime;
			}
			return getCurrentTimestamp();
		}
	}

	@GuardedBy("lock")
	private static long getCurrentTimestamp() {
		final long currentProcessingTime = System.currentTimeMillis();
		if (currentProcessingTime < lastReturnedProcessingTime) {
			return lastReturnedProcessingTime;
		}

		lastReturnedProcessingTime = currentProcessingTime;
		return lastReturnedProcessingTime;
	}

	private static long unfreezeTime() {
		timeIsFrozen = false;
		return lastReturnedProcessingTime;
	}
}
