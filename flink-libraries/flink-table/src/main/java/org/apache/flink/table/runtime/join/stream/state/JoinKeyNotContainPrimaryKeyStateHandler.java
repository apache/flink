/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copysecond ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join.stream.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The wrap for {@link KeyedState}s whose values are a collection of
 * {@link Tuple3}. When iterating over the tuple3 under a given key,
 * the order in which the elements are iterated is not specified.
 *
 */
@Internal
public class JoinKeyNotContainPrimaryKeyStateHandler implements JoinStateHandler {
	//<joinKey, primaryKey, Tuple2<record,expire-time>>
	private final KeyedMapState<BaseRow, BaseRow, Tuple2<BaseRow, Long>> keyedMapState;

	//joinKey projection
	private final KeySelector<BaseRow, BaseRow> keySelector;

	//pk projection
	private final Projection<BaseRow, BaseRow> pkProjection;

	private transient BaseRow joinKey;

	private transient BaseRow pk;

	public JoinKeyNotContainPrimaryKeyStateHandler(
			KeyedMapState<BaseRow, BaseRow, Tuple2<BaseRow, Long>> keyedMapState,
			KeySelector<BaseRow, BaseRow> keySelector, Projection<BaseRow, BaseRow> pkProjection) {
		this.keyedMapState = keyedMapState;
		this.keySelector = keySelector;
		this.pkProjection = pkProjection;

	}

	@Override
	public void extractCurrentJoinKey(BaseRow row) throws Exception {
		this.joinKey = keySelector.getKey(row);
	}

	@Override
	public BaseRow getCurrentJoinKey() {
		return this.joinKey;
	}

	@Override
	public void extractCurrentPrimaryKey(BaseRow row) {
		this.pk = pkProjection.apply(row);
	}

	@Override
	public BaseRow getCurrentPrimaryKey() {
		return this.pk;
	}

	@Override
	public long add(BaseRow row, long expireTime) {
		keyedMapState.add(joinKey, pk, new Tuple2<>(row, expireTime));
		return 1;
	}

	@Override
	public long retract(BaseRow row) {
		keyedMapState.remove(joinKey, pk);
		return 0;
	}

	@Override
	public void batchGet(Collection<? extends BaseRow> keys) {
		throw new RuntimeException("batchGet is not supported for JoinKeyNotContainPrimaryKeyStateHandler");
	}

	@Override
	public void setCurrentJoinKey(BaseRow row) {
		joinKey = row;
	}

	@Override
	public Iterator<Tuple3<BaseRow, Long, Long>> getRecords(BaseRow key) {
		return new Tuple3Iterator(keyedMapState.iterator(key));
	}

	@Override
	public Iterator<Tuple3<BaseRow, Long, Long>> getRecordsFromCache(BaseRow key) {
		return getRecords(key);
	}

	@Override
	public boolean contains(BaseRow key, BaseRow row) {
		return keyedMapState.contains(key, pkProjection.apply(row));
	}

	@Override
	public void update(BaseRow key, BaseRow row, long count, long expireTime) {
		keyedMapState.add(key, pkProjection.apply(row), new Tuple2<>(row, expireTime));
	}

	@Override
	public long[] batchUpdate(BaseRow key, List<Tuple2<BaseRow, Long>> rows, long expireTime) {
		Map<BaseRow, Tuple2<BaseRow, Long>> addMap = new HashMap<>();
		Map<BaseRow, Integer> deleteMap = new HashMap<>();
		long[] updateStatus = new long[rows.size()];
		int idx = 0;
		for (Tuple2<BaseRow, Long> tuple2: rows) {
			if (tuple2.f1 < 0) {
				updateStatus[idx] = -1;
				deleteMap.put(pkProjection.apply(tuple2.f0), idx);
			} else {
				updateStatus[idx] = 1;
				addMap.put(pkProjection.apply(tuple2.f0), new Tuple2<>(tuple2.f0, expireTime));
			}
			idx++;
		}
		// remove data from deleteSet if first delete and add, set update status to 0 to ignore
		// match state first delete and add.
		for (Map.Entry<BaseRow, Tuple2<BaseRow, Long>> entry: addMap.entrySet()) {
			Integer index = deleteMap.get(entry.getKey());
			if (index != null) {
				deleteMap.remove(entry.getKey());
				updateStatus[index] = 0;
			}
		}

		keyedMapState.removeAll(key, deleteMap.keySet());
		keyedMapState.addAll(key, addMap);
		return updateStatus;
	}

	@Override
	public void putAll(Map<BaseRow, BaseRow> putMap) {
		throw new RuntimeException("MapState don't support putAll!");
	}

	@Override
	public void removeAll(Set<BaseRow> keys) {
		throw new RuntimeException("MapState don't support removeAll!");
	}

	@Override
	public void remove(BaseRow joinKey) {
		keyedMapState.remove(joinKey);
	}

	/**
	 * An iterator over the elements under the same joinKey which is backed by an
	 * iterator in the internal state.
	 */
	private class Tuple3Iterator implements Iterator<Tuple3<BaseRow, Long, Long>> {

		/**
		 * The iterator in the internal state which iterates over the joinKey-pair
		 * pairs with the same joinKey.
		 */
		private final Iterator<Map.Entry<BaseRow, Tuple2<BaseRow, Long>>> internalIterator;
		private final Tuple3<BaseRow, Long, Long> reuse;

		@SuppressWarnings("unchecked")
		Tuple3Iterator(Iterator<Map.Entry<BaseRow, Tuple2<BaseRow, Long>>> internalIterator) {
			Preconditions.checkNotNull(internalIterator);
			this.internalIterator = internalIterator;
			this.reuse = new Tuple3<>();
		}

		@Override
		public boolean hasNext() {
			return internalIterator.hasNext();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Tuple3<BaseRow, Long, Long> next() {
			Tuple2<BaseRow, Long> tuple2 = internalIterator.next().getValue();
			reuse.f0 = tuple2.f0;
			reuse.f1 = 1L;
			reuse.f2 = tuple2.f1;
			return reuse;
		}

		@Override
		public void remove() {
			internalIterator.remove();
		}
	}
}

