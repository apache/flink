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

package org.apache.flink.runtime.state;

import javax.annotation.Nonnull;

import java.util.Optional;

/**
 * Filter of snapshot state values.
 *
 * <p>This filter can be applied to state values
 * to decide which entries should be included into the snapshot (can be modified before),
 * others should be dropped (returns {@code Optional.empty()}).
 */
public interface StateSnapshotFilter<T> {
	StateSnapshotFilter<?> SNAPSHOT_ALL = createSnapshotAll();

	@SuppressWarnings("unchecked")
	@Nonnull
	static <T> StateSnapshotFilter<T> snapshotAll() {
		return (StateSnapshotFilter<T>) SNAPSHOT_ALL;
	}

	static <T> StateSnapshotFilter<T> createSnapshotAll() {
		return new StateSnapshotFilter<T>() {
			@Override
			@Nonnull
			public Optional<T> filter(@Nonnull T value) {
				return Optional.of(value);
			}

			@Override
			@Nonnull
			public Optional<byte[]> filterSerialized(@Nonnull byte[] value) {
				return Optional.of(value);
			}
		};
	}

	/**
	 * Filter by non-serialized form of value.
	 *
	 * @param value non-serialized form of value
	 * @return Optional.of(value to snapshot) or Optional.empty() to drop
	 */
	@Nonnull
	Optional<T> filter(@Nonnull T value);

	/**
	 * Filter by serialized form of value.
	 *
	 * @param value serialized form of value
	 * @return Optional.of(value to snapshot) or Optional.empty() to drop
	 */
	@Nonnull
	Optional<byte[]> filterSerialized(@Nonnull byte[] value);
}
