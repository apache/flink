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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class wraps list state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user entry value of state with TTL
 */
class TtlListState<K, N, T> extends
	AbstractTtlState<K, N, List<T>, List<TtlValue<T>>, InternalListState<K, N, TtlValue<T>>>
	implements InternalListState<K, N, T> {
	TtlListState(
		InternalListState<K, N, TtlValue<T>> originalState,
		StateTtlConfiguration config,
		TtlTimeProvider timeProvider,
		TypeSerializer<List<T>> valueSerializer) {
		super(originalState, config, timeProvider, valueSerializer);
	}

	@Override
	public void update(List<T> values) throws Exception {
		updateInternal(values);
	}

	@Override
	public void addAll(List<T> values) throws Exception {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");
		original.addAll(withTs(values));
	}

	@Override
	public Iterable<T> get() throws Exception {
		Iterable<TtlValue<T>> ttlValue = original.get();
		ttlValue = ttlValue == null ? Collections.emptyList() : ttlValue;
		if (updateTsOnRead) {
			List<TtlValue<T>> collected = collect(ttlValue);
			ttlValue = collected;
			updateTs(collected);
		}
		final Iterable<TtlValue<T>> finalResult = ttlValue;
		return () -> new IteratorWithCleanup(finalResult.iterator());
	}

	private void updateTs(List<TtlValue<T>> ttlValue) throws Exception {
		List<TtlValue<T>> unexpiredWithUpdatedTs = ttlValue.stream()
			.filter(v -> !expired(v))
			.map(this::rewrapWithNewTs)
			.collect(Collectors.toList());
		if (!unexpiredWithUpdatedTs.isEmpty()) {
			original.update(unexpiredWithUpdatedTs);
		}
	}

	@Override
	public void add(T value) throws Exception {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
		original.add(wrapWithTs(value));
	}

	@Override
	public void clear() {
		original.clear();
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		original.mergeNamespaces(target, sources);
	}

	@Override
	public List<T> getInternal() throws Exception {
		return collect(get());
	}

	private <E> List<E> collect(Iterable<E> iterable) {
		return iterable instanceof List ? (List<E>) iterable :
			StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
	}

	@Override
	public void updateInternal(List<T> valueToStore) throws Exception {
		Preconditions.checkNotNull(valueToStore, "List of values to update cannot be null.");
		original.updateInternal(withTs(valueToStore));
	}

	private List<TtlValue<T>> withTs(List<T> values) {
		return values.stream().map(this::wrapWithTs).collect(Collectors.toList());
	}

	private class IteratorWithCleanup implements Iterator<T> {
		private final Iterator<TtlValue<T>> originalIterator;
		private boolean anyUnexpired = false;
		private boolean uncleared = true;
		private T nextUnexpired = null;

		private IteratorWithCleanup(Iterator<TtlValue<T>> ttlIterator) {
			this.originalIterator = ttlIterator;
		}

		@Override
		public boolean hasNext() {
			findNextUnexpired();
			cleanupIfEmpty();
			return nextUnexpired != null;
		}

		private void cleanupIfEmpty() {
			boolean endOfIter = !originalIterator.hasNext() && nextUnexpired == null;
			if (uncleared && !anyUnexpired && endOfIter) {
				original.clear();
				uncleared = false;
			}
		}

		@Override
		public T next() {
			if (hasNext()) {
				T result = nextUnexpired;
				nextUnexpired = null;
				return result;
			}
			throw new NoSuchElementException();
		}

		private void findNextUnexpired() {
			while (nextUnexpired == null && originalIterator.hasNext()) {
				TtlValue<T> ttlValue = originalIterator.next();
				if (ttlValue == null) {
					break;
				}
				boolean unexpired = !expired(ttlValue);
				if (unexpired) {
					anyUnexpired = true;
				}
				if (unexpired || returnExpired) {
					nextUnexpired = ttlValue.getUserValue();
				}
			}
		}
	}
}
