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

package org.apache.flink.table.dataview;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.util.IterableIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * MapView which is implemented using state backends.
 *
 * @param <N> the type of namespace
 * @param <MK> the type of MapView key
 * @param <MV> the type of MapView value
 */
public abstract class StateMapView<N, MK, MV> extends MapView<MK, MV> implements StateDataView<N> {

	private static final long serialVersionUID = 1L;

	/**
	 * {@link StateMapViewWithKeysNotNull} is a {@link MapView} which implemented
	 * using state backend which map keys shouldn't be null. This is the default
	 * implementation for {@link StateMapView}.
	 *
	 * @param <N> the type of namespace
	 * @param <MK> the type of MapView key
	 * @param <MV> the type of MapView value
	 */
	private abstract static class StateMapViewWithKeysNotNull<N, MK, MV> extends StateMapView<N, MK, MV> {

		private static final long serialVersionUID = 2605280027745112384L;

		private final MapState<MK, MV> mapState;
		private final Map<MK, MV> emptyState = Collections.emptyMap();

		private StateMapViewWithKeysNotNull(MapState<MK, MV> mapState) {
			this.mapState = mapState;
		}

		@Override
		public MV get(MK key) throws Exception {
			return mapState.get(key);
		}

		@Override
		public void put(MK key, MV value) throws Exception {
			mapState.put(key, value);
		}

		@Override
		public void putAll(Map<MK, MV> map) throws Exception {
			mapState.putAll(map);
		}

		@Override
		public void remove(MK key) throws Exception {
			mapState.remove(key);
		}

		@Override
		public boolean contains(MK key) throws Exception {
			return mapState.contains(key);
		}

		@Override
		public Iterable<Map.Entry<MK, MV>> entries() throws Exception {
			Iterable<Map.Entry<MK, MV>> original = mapState.entries();
			return original != null ? original : emptyState.entrySet();
		}

		@Override
		public Iterable<MK> keys() throws Exception {
			Iterable<MK> original = mapState.keys();
			return original != null ? original : emptyState.keySet();
		}

		@Override
		public Iterable<MV> values() throws Exception {
			Iterable<MV> original = mapState.values();
			return original != null ? original : emptyState.values();
		}

		@Override
		public Iterator<Map.Entry<MK, MV>> iterator() throws Exception {
			Iterator<Map.Entry<MK, MV>> original = mapState.iterator();
			return original != null ? original : emptyState.entrySet().iterator();
		}

		@Override
		public void clear() {
			mapState.clear();
		}
	}

	/**
	 * {@link StateMapViewWithKeysNullable} is a {@link MapView} which implemented using state backend
	 * and can handle nullable map keys. Currently this is only used internally when implementing
	 * distinct aggregates.
	 *
	 * @param <N> the type of namespace
	 * @param <MK> the type of MapView key
	 * @param <MV> the type of MapView value
	 */
	private abstract static class StateMapViewWithKeysNullable<N, MK, MV> extends StateMapView<N, MK, MV> {

		private static final long serialVersionUID = 2605280027745112384L;

		private final MapState<MK, MV> mapState;
		private final ValueState<MV> nullState;

		private StateMapViewWithKeysNullable(
			MapState<MK, MV> mapState,
			ValueState<MV> nullState) {

			this.mapState = mapState;
			this.nullState = nullState;
		}

		@Override
		public MV get(MK key) throws Exception {
			if (key == null) {
				return nullState.value();
			} else {
				return mapState.get(key);
			}
		}

		@Override
		public void put(MK key, MV value) throws Exception {
			if (key == null) {
				nullState.update(value);
			} else {
				mapState.put(key, value);
			}
		}

		@Override
		public void putAll(Map<MK, MV> map) throws Exception {
			for (Map.Entry<MK, MV> entry : map.entrySet()) {
				// entry key might be null, so we can't invoke mapState.putAll(map) directly here
				put(entry.getKey(), entry.getValue());
			}
		}

		@Override
		public void remove(MK key) throws Exception {
			if (key == null) {
				nullState.clear();
			} else {
				mapState.remove(key);
			}
		}

		@Override
		public boolean contains(MK key) throws Exception {
			if (key == null) {
				return nullState.value() != null;
			} else {
				return mapState.contains(key);
			}
		}

		@Override
		public Iterable<Map.Entry<MK, MV>> entries() throws Exception {
			final Iterator<Map.Entry<MK, MV>> iterator = iterator();
			return () -> iterator;
		}

		@Override
		public Iterable<MK> keys() throws Exception {
			return new KeysIterable(this.iterator());
		}

		@Override
		public Iterable<MV> values() throws Exception {
			return new ValuesIterable(this.iterator());
		}

		@Override
		public Iterator<Map.Entry<MK, MV>> iterator() throws Exception {
			return new NullAwareMapIterator<>(mapState.iterator(), new NullMapEntryImpl());
		}

		@Override
		public void clear() {
			mapState.clear();
			nullState.clear();
		}

		/**
		 * MapEntry for the null key of this MapView.
		 */
		private class NullMapEntryImpl implements NullAwareMapIterator.NullMapEntry<MK, MV> {

			@Override
			public MV getValue() {
				try {
					return nullState.value();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public MV setValue(MV value) {
				MV oldValue;
				try {
					oldValue = nullState.value();
					nullState.update(value);
					return oldValue;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				nullState.clear();
			}
		}

		/**
		 * Iterable of the keys on the MapView.
		 */
		private class KeysIterable implements IterableIterator<MK> {

			private final Iterator<Map.Entry<MK, MV>> iterator;

			private KeysIterable(Iterator<Map.Entry<MK, MV>> iterator) {
				this.iterator = iterator;
			}

			@Override
			public Iterator<MK> iterator() {
				return this;
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public MK next() {
				return iterator.next().getKey();
			}
		}

		/**
		 * Iterable of the values on the MapView.
		 */
		private class ValuesIterable implements IterableIterator<MV> {

			private final Iterator<Map.Entry<MK, MV>> iterator;

			private ValuesIterable(Iterator<Map.Entry<MK, MV>> iterator) {
				this.iterator = iterator;
			}

			@Override
			public Iterator<MV> iterator() {
				return this;
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public MV next() {
				return iterator.next().getValue();
			}
		}
	}

	/**
	 * A state MapView which do not support nullable keys and namespace.
	 */
	public static final class KeyedStateMapViewWithKeysNotNull<N, MK, MV> extends StateMapViewWithKeysNotNull<N, MK, MV> {

		private static final long serialVersionUID = 6650061094951931356L;

		public KeyedStateMapViewWithKeysNotNull(MapState<MK, MV> mapState) {
			super(mapState);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * A state MapView which support namespace but not support nullable keys.
	 */
	public static final class NamespacedStateMapViewWithKeysNotNull<N, MK, MV> extends StateMapViewWithKeysNotNull<N, MK, MV> {

		private static final long serialVersionUID = -2793150592169689571L;
		private final InternalMapState<?, N, MK, MV> internalMapState;

		public NamespacedStateMapViewWithKeysNotNull(InternalMapState<?, N, MK, MV> internalMapState) {
			super(internalMapState);
			this.internalMapState = internalMapState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			internalMapState.setCurrentNamespace(namespace);
		}
	}

	/**
	 * A state MapView which support nullable keys, but not support namespace.
	 */
	public static final class KeyedStateMapViewWithKeysNullable<N, MK, MV> extends StateMapViewWithKeysNullable<N, MK, MV> {

		private static final long serialVersionUID = -4222930534937318207L;

		public KeyedStateMapViewWithKeysNullable(MapState<MK, MV> mapState, ValueState<MV> nullState) {
			super(mapState, nullState);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * A state MapView which support nullable keys and namespace.
	 */
	public static final class NamespacedStateMapViewWithKeysNullable<N, MK, MV> extends StateMapViewWithKeysNullable<N, MK, MV> {

		private static final long serialVersionUID = -6915428707804508152L;
		private final InternalMapState<?, N, MK, MV> internalMapState;
		private final InternalValueState<?, N, MV> internalNullState;

		public NamespacedStateMapViewWithKeysNullable(InternalMapState<?, N, MK, MV> internalMapState, InternalValueState<?, N, MV> internalNullState) {
			super(internalMapState, internalNullState);
			this.internalMapState = internalMapState;
			this.internalNullState = internalNullState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			internalMapState.setCurrentNamespace(namespace);
			internalNullState.setCurrentNamespace(namespace);
		}
	}

}
