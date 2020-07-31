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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.util.IterableIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link MapView} which is implemented using state backends.
 *
 * @param <N> the type of namespace
 * @param <EK> the external type of the {@link MapView} key
 * @param <EV> the external type of the {@link MapView} value
 */
@Internal
public abstract class StateMapView<N, EK, EV> extends MapView<EK, EV> implements StateDataView<N> {

	@Override
	public Map<EK, EV> getMap() {
		final Map<EK, EV> map = new HashMap<>();
		try {
			entries().forEach(entry -> map.put(entry.getKey(), entry.getValue()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return map;
	}

	@Override
	public void setMap(Map<EK, EV> map) {
		clear();
		try {
			putAll(map);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@link StateMapViewWithKeysNotNull} is a {@link MapView} which implemented
	 * using state backend which map keys shouldn't be null. This is the default
	 * implementation for {@link StateMapView}.
	 *
	 * @param <N> the type of namespace
	 * @param <EK> the external type of the {@link MapView} key
	 * @param <EV> the external type of the {@link MapView} value
	 */
	private abstract static class StateMapViewWithKeysNotNull<N, EK, EV> extends StateMapView<N, EK, EV> {

		private final Map<EK, EV> emptyState = Collections.emptyMap();

		protected abstract MapState<EK, EV> getMapState();

		@Override
		public EV get(EK key) throws Exception {
			return getMapState().get(key);
		}

		@Override
		public void put(EK key, EV value) throws Exception {
			getMapState().put(key, value);
		}

		@Override
		public void putAll(Map<EK, EV> map) throws Exception {
			getMapState().putAll(map);
		}

		@Override
		public void remove(EK key) throws Exception {
			getMapState().remove(key);
		}

		@Override
		public boolean contains(EK key) throws Exception {
			return getMapState().contains(key);
		}

		@Override
		public Iterable<Map.Entry<EK, EV>> entries() throws Exception {
			Iterable<Map.Entry<EK, EV>> original = getMapState().entries();
			return original != null ? original : emptyState.entrySet();
		}

		@Override
		public Iterable<EK> keys() throws Exception {
			Iterable<EK> original = getMapState().keys();
			return original != null ? original : emptyState.keySet();
		}

		@Override
		public Iterable<EV> values() throws Exception {
			Iterable<EV> original = getMapState().values();
			return original != null ? original : emptyState.values();
		}

		@Override
		public Iterator<Map.Entry<EK, EV>> iterator() throws Exception {
			Iterator<Map.Entry<EK, EV>> original = getMapState().iterator();
			return original != null ? original : emptyState.entrySet().iterator();
		}

		@Override
		public boolean isEmpty() throws Exception {
			return getMapState().isEmpty();
		}

		@Override
		public void clear() {
			getMapState().clear();
		}
	}

	/**
	 * {@link StateMapViewWithKeysNullable} is a {@link MapView} which is implemented using a state backend
	 * and can handle nullable map keys. Currently this is only used internally when implementing
	 * distinct aggregates.
	 *
	 * @param <N> the type of namespace
	 * @param <EK> the external type of the {@link MapView} key
	 * @param <EV> the external type of the {@link MapView} value
	 */
	private abstract static class StateMapViewWithKeysNullable<N, EK, EV> extends StateMapView<N, EK, EV> {

		@Override
		public EV get(EK key) throws Exception {
			if (key == null) {
				return getNullState().value();
			} else {
				return getMapState().get(key);
			}
		}

		@Override
		public void put(EK key, EV value) throws Exception {
			if (key == null) {
				getNullState().update(value);
			} else {
				getMapState().put(key, value);
			}
		}

		@Override
		public void putAll(Map<EK, EV> map) throws Exception {
			for (Map.Entry<EK, EV> entry : map.entrySet()) {
				// entry key might be null, so we can't invoke mapState.putAll(map) directly here
				put(entry.getKey(), entry.getValue());
			}
		}

		@Override
		public void remove(EK key) throws Exception {
			if (key == null) {
				getNullState().clear();
			} else {
				getMapState().remove(key);
			}
		}

		@Override
		public boolean contains(EK key) throws Exception {
			if (key == null) {
				return getNullState().value() != null;
			} else {
				return getMapState().contains(key);
			}
		}

		@Override
		public Iterable<Map.Entry<EK, EV>> entries() throws Exception {
			final Iterator<Map.Entry<EK, EV>> iterator = iterator();
			return () -> iterator;
		}

		@Override
		public Iterable<EK> keys() throws Exception {
			return new KeysIterable(this.iterator());
		}

		@Override
		public Iterable<EV> values() throws Exception {
			return new ValuesIterable(this.iterator());
		}

		@Override
		public Iterator<Map.Entry<EK, EV>> iterator() throws Exception {
			return new NullAwareMapIterator<>(getMapState().iterator(), new NullMapEntryImpl());
		}

		@Override
		public boolean isEmpty() throws Exception {
			return getMapState().isEmpty() && getNullState().value() == null;
		}

		@Override
		public void clear() {
			getMapState().clear();
			getNullState().clear();
		}

		protected abstract MapState<EK, EV> getMapState();

		protected abstract ValueState<EV> getNullState();

		/**
		 * {@link Map.Entry} for the null key of this {@link MapView}.
		 */
		private class NullMapEntryImpl implements NullAwareMapIterator.NullMapEntry<EK, EV> {

			@Override
			public EV getValue() {
				try {
					return getNullState().value();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public EV setValue(EV value) {
				EV oldValue;
				try {
					oldValue = getNullState().value();
					getNullState().update(value);
					return oldValue;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				getNullState().clear();
			}
		}

		/**
		 * Iterable of the keys on the {@link MapView}.
		 */
		private class KeysIterable implements IterableIterator<EK> {

			private final Iterator<Map.Entry<EK, EV>> iterator;

			private KeysIterable(Iterator<Map.Entry<EK, EV>> iterator) {
				this.iterator = iterator;
			}

			@Override
			public Iterator<EK> iterator() {
				return this;
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public EK next() {
				return iterator.next().getKey();
			}
		}

		/**
		 * Iterable of the values on the {@link MapView}.
		 */
		private class ValuesIterable implements IterableIterator<EV> {

			private final Iterator<Map.Entry<EK, EV>> iterator;

			private ValuesIterable(Iterator<Map.Entry<EK, EV>> iterator) {
				this.iterator = iterator;
			}

			@Override
			public Iterator<EV> iterator() {
				return this;
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public EV next() {
				return iterator.next().getValue();
			}
		}
	}

	/**
	 * A state {@link MapView} which does not support nullable keys and namespace.
	 */
	public static final class KeyedStateMapViewWithKeysNotNull<N, EK, EV> extends StateMapViewWithKeysNotNull<N, EK, EV> {

		private final MapState<EK, EV> mapState;

		public KeyedStateMapViewWithKeysNotNull(MapState<EK, EV> mapState) {
			this.mapState = mapState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			return mapState;
		}
	}

	/**
	 * A state {@link MapView} which supports namespace but does not support nullable keys.
	 */
	public static final class NamespacedStateMapViewWithKeysNotNull<N, EK, EV> extends StateMapViewWithKeysNotNull<N, EK, EV> {

		private final InternalMapState<?, N, EK, EV> internalMapState;

		private N namespace;

		public NamespacedStateMapViewWithKeysNotNull(InternalMapState<?, N, EK, EV> internalMapState) {
			this.internalMapState = internalMapState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			this.namespace = namespace;
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			internalMapState.setCurrentNamespace(namespace);
			return internalMapState;
		}
	}

	/**
	 * A state {@link MapView} which supports nullable keys but does not support namespace.
	 */
	public static final class KeyedStateMapViewWithKeysNullable<N, EK, EV> extends StateMapViewWithKeysNullable<N, EK, EV> {

		private final MapState<EK, EV> mapState;

		private final ValueState<EV> nullState;

		public KeyedStateMapViewWithKeysNullable(MapState<EK, EV> mapState, ValueState<EV> nullState) {
			this.mapState = mapState;
			this.nullState = nullState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			return mapState;
		}

		@Override
		protected ValueState<EV> getNullState() {
			return nullState;
		}
	}

	/**
	 * A state {@link MapView} which supports nullable keys and namespace.
	 */
	public static final class NamespacedStateMapViewWithKeysNullable<N, EK, EV> extends StateMapViewWithKeysNullable<N, EK, EV> {

		private final InternalMapState<?, N, EK, EV> internalMapState;

		private final InternalValueState<?, N, EV> internalNullState;

		private N namespace;

		public NamespacedStateMapViewWithKeysNullable(InternalMapState<?, N, EK, EV> internalMapState, InternalValueState<?, N, EV> internalNullState) {
			this.internalMapState = internalMapState;
			this.internalNullState = internalNullState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			this.namespace = namespace;
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			internalMapState.setCurrentNamespace(namespace);
			return internalMapState;
		}

		@Override
		protected ValueState<EV> getNullState() {
			internalNullState.setCurrentNamespace(namespace);
			return internalNullState;
		}
	}
}
