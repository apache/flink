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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.StateTtlConfiguration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nonnull;

/**
 * Base class for TTL logic wrappers.
 *
 * @param <T> Type of originally wrapped object
 */
abstract class AbstractTtlDecorator<T> {
	/** Wrapped original state handler. */
	final T original;

	final StateTtlConfiguration config;

	final TtlTimeProvider timeProvider;

	/** Whether to renew expiration timestamp on state read access. */
	final boolean updateTsOnRead;

	/** Whether to renew expiration timestamp on state read access. */
	final boolean returnExpired;

	/** State value time to live in milliseconds. */
	final long ttl;

	AbstractTtlDecorator(
		T original,
		StateTtlConfiguration config,
		TtlTimeProvider timeProvider) {
		Preconditions.checkNotNull(original);
		Preconditions.checkNotNull(config);
		Preconditions.checkNotNull(timeProvider);
		this.original = original;
		this.config = config;
		this.timeProvider = timeProvider;
		this.updateTsOnRead = config.getTtlUpdateType() == StateTtlConfiguration.TtlUpdateType.OnReadAndWrite;
		this.returnExpired = config.getStateVisibility() == StateTtlConfiguration.TtlStateVisibility.ReturnExpiredIfNotCleanedUp;
		this.ttl = config.getTtl().toMilliseconds();
	}

	<V> V getUnexpired(TtlValue<V> ttlValue) {
		return ttlValue == null || (expired(ttlValue) && !returnExpired) ? null : ttlValue.getUserValue();
	}

	<V> boolean expired(TtlValue<V> ttlValue) {
		return ttlValue != null && getExpirationTimestamp(ttlValue) <= timeProvider.currentTimestamp();
	}

	private long getExpirationTimestamp(@Nonnull TtlValue<?> ttlValue) {
		long ts = ttlValue.getLastAccessTimestamp();
		long ttlWithoutOverflow = ts > 0 ? Math.min(Long.MAX_VALUE - ts, ttl) : ttl;
		return ts + ttlWithoutOverflow;
	}

	<V> TtlValue<V> wrapWithTs(V value) {
		return wrapWithTs(value, timeProvider.currentTimestamp());
	}

	static <V> TtlValue<V> wrapWithTs(V value, long ts) {
		return value == null ? null : new TtlValue<>(value, ts);
	}

	<V> TtlValue<V> rewrapWithNewTs(TtlValue<V> ttlValue) {
		return wrapWithTs(ttlValue.getUserValue());
	}

	<SE extends Throwable, CE extends Throwable, CLE extends Throwable, V> V getWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<V>, SE> getter,
		ThrowingConsumer<TtlValue<V>, CE> updater,
		ThrowingRunnable<CLE> stateClear) throws SE, CE, CLE {
		TtlValue<V> ttlValue = getter.get();
		if (ttlValue == null) {
			return null;
		} else if (expired(ttlValue)) {
			stateClear.run();
			if (!returnExpired) {
				return null;
			}
		} else if (updateTsOnRead) {
			updater.accept(rewrapWithNewTs(ttlValue));
		}
		return ttlValue.getUserValue();
	}
}
