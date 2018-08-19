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

package org.apache.flink.runtime.state.ttl.cleanup;

import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.ttl.TtlValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;

/**
 * State snapshot transformer with incremental cleanup of local state with TTL.
 *
 * <p>This class wraps the original transformer with logic which passes the detected expired state
 * to incremental cleanup service.
 *
 * @param <K> type of state key
 * @param <N> type of state namespace
 * @param <T> type of state
 */
public class StateTransformerWithIncCleanup<K, N, T> implements StateSnapshotTransformer<K, N, T> {
	private final StateSnapshotTransformer<K, N, T> originalTransformer;
	private final TtlIncrementalCleanup<K, N> ttlIncrementalCleanup;

	private StateTransformerWithIncCleanup(
		@Nonnull StateSnapshotTransformer<K, N, T> originalTransformer,
		@Nonnull TtlIncrementalCleanup<K, N> ttlIncrementalCleanup) {
		this.originalTransformer = originalTransformer;
		this.ttlIncrementalCleanup = ttlIncrementalCleanup;
	}

	@Override
	@Nullable
	public T filterOrTransform(@Nonnull K key, @Nonnull N namespace, @Nullable T value) {
		if (value == null) {
			return null;
		}
		T filteredValue = originalTransformer.filterOrTransform(key, namespace, value);
		ttlIncrementalCleanup.addIfExpired(key, namespace, value, filteredValue);
		return filteredValue;
	}

	/** Wraps factory of snapshot transformer with incremental cleanup of local state with TTL. */
	public static class Factory<K, N, T> implements StateSnapshotTransformFactory<K, N, TtlValue<T>> {
		private final StateSnapshotTransformFactory<K, N, TtlValue<T>> originalFactory;
		private final TtlIncrementalCleanup<K, N> ttlIncrementalCleanup;

		private Factory(
			@Nonnull StateSnapshotTransformFactory<K, N, TtlValue<T>> originalFactory,
			@Nonnull TtlIncrementalCleanup<K, N> ttlIncrementalCleanup) {
			this.originalFactory = originalFactory;
			this.ttlIncrementalCleanup = ttlIncrementalCleanup;
		}

		public static <K, N, T> StateSnapshotTransformFactory<K, N, TtlValue<T>> wrapWithIncCleanupIfEnabled(
			@Nonnull StateSnapshotTransformFactory<K, N, TtlValue<T>> originalFactory,
			@Nullable TtlIncrementalCleanup<K, N> ttlIncrementalCleanup) {
			return ttlIncrementalCleanup != null ?
				new Factory<>(originalFactory, ttlIncrementalCleanup) : originalFactory;
		}

		@Override
		public Optional<StateSnapshotTransformer<K, N, TtlValue<T>>> createForDeserializedState() {
			return originalFactory.createForDeserializedState()
				.map(t -> new StateTransformerWithIncCleanup<>(t, ttlIncrementalCleanup));
		}

		@Override
		public Optional<StateSnapshotTransformer<K, N, byte[]>> createForSerializedState() {
			return originalFactory.createForSerializedState()
				.map(t -> new StateTransformerWithIncCleanup<>(t, ttlIncrementalCleanup));
		}
	}
}
