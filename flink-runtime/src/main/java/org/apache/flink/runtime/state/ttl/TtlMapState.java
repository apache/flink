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
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * This class wraps map state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <UK> Type of the user entry key of state with TTL
 * @param <UV> Type of the user entry value of state with TTL
 */
class TtlMapState<K, N, UK, UV>
	extends AbstractTtlState<K, N, Map<UK, UV>, Map<UK, TtlValue<UV>>, InternalMapState<K, N, UK, TtlValue<UV>>>
	implements InternalMapState<K, N, UK, UV> {
	TtlMapState(
		InternalMapState<K, N, UK, TtlValue<UV>> original,
		StateTtlConfiguration config,
		TtlTimeProvider timeProvider,
		TypeSerializer<Map<UK, UV>> valueSerializer) {
		super(original, config, timeProvider, valueSerializer);
	}

	@Override
	public UV get(UK key) throws Exception {
		return getWithTtlCheckAndUpdate(() -> original.get(key), v -> original.put(key, v), () -> original.remove(key));
	}

	@Override
	public void put(UK key, UV value) throws Exception {
		original.put(key, wrapWithTs(value));
	}

	@Override
	public void putAll(Map<UK, UV> map) throws Exception {
		if (map == null) {
			return;
		}
		Map<UK, TtlValue<UV>> ttlMap = new HashMap<>(map.size());
		for (UK key : map.keySet()) {
			ttlMap.put(key, wrapWithTs(map.get(key)));
		}
		original.putAll(ttlMap);
	}

	@Override
	public void remove(UK key) throws Exception {
		original.remove(key);
	}

	@Override
	public boolean contains(UK key) throws Exception {
		return get(key) != null;
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
		return entries(e -> e);
	}

	private <R> Iterable<R> entries(
		Function<Map.Entry<UK, UV>, R> resultMapper) throws Exception {
		Iterable<Map.Entry<UK, TtlValue<UV>>> withTs = original.entries();
		return () -> new EntriesIterator<>(withTs == null ? Collections.emptyList() : withTs, resultMapper);
	}

	@Override
	public Iterable<UK> keys() throws Exception {
		return entries(Map.Entry::getKey);
	}

	@Override
	public Iterable<UV> values() throws Exception {
		return entries(Map.Entry::getValue);
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
		return entries().iterator();
	}

	@Override
	public void clear() {
		original.clear();
	}

	private class EntriesIterator<R> implements Iterator<R> {
		private final Iterator<Map.Entry<UK, TtlValue<UV>>> originalIterator;
		private final Function<Map.Entry<UK, UV>, R> resultMapper;
		private Map.Entry<UK, UV> nextUnexpired = null;
		private boolean rightAfterNextIsCalled = false;

		private EntriesIterator(
			@Nonnull Iterable<Map.Entry<UK, TtlValue<UV>>> withTs,
			@Nonnull Function<Map.Entry<UK, UV>, R> resultMapper) {
			this.originalIterator = withTs.iterator();
			this.resultMapper = resultMapper;
		}

		@Override
		public boolean hasNext() {
			rightAfterNextIsCalled = false;
			while (nextUnexpired == null && originalIterator.hasNext()) {
				nextUnexpired = getUnexpiredAndUpdateOrCleanup(originalIterator.next());
			}
			return nextUnexpired != null;
		}

		@Override
		public R next() {
			if (hasNext()) {
				rightAfterNextIsCalled = true;
				R result = resultMapper.apply(nextUnexpired);
				nextUnexpired = null;
				return result;
			}
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			if (rightAfterNextIsCalled) {
				originalIterator.remove();
			} else {
				throw new IllegalStateException("next() has not been called or hasNext() has been called afterwards," +
					" remove() is supported only right after calling next()");
			}
		}

		private Map.Entry<UK, UV> getUnexpiredAndUpdateOrCleanup(Map.Entry<UK, TtlValue<UV>> e) {
			UV unexpiredValue;
			try {
				unexpiredValue = getWithTtlCheckAndUpdate(
					e::getValue,
					v -> original.put(e.getKey(), v),
					originalIterator::remove);
			} catch (Exception ex) {
				throw new FlinkRuntimeException(ex);
			}
			return unexpiredValue == null ? null : new AbstractMap.SimpleEntry<>(e.getKey(), unexpiredValue);
		}
	}
}
