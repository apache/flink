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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;

/**
 * The wrap for {@link KeyedState}s whose values are a collection of
 * {@link Tuple3}. When iterating over the tuple3s under a given key,
 * the order in which the elements are iterated is not specified.
 * the keys are joinKeys.
 * the keys in the mappings are the records.
 * the values in the mappings are the count and the expire time.
 */
@Internal
public class WithoutPrimaryKeyStateHandler implements JoinStateHandler {
	//<joinKey, record, Tuple2<count, expire-time>>
	private final KeyedMapState<BaseRow, BaseRow, Tuple2<Long, Long>> keyedMapState;

	private final KeySelector<BaseRow, BaseRow> keySelector;

	private transient BaseRow joinKey;

	public WithoutPrimaryKeyStateHandler(
			KeyedMapState<BaseRow, BaseRow, Tuple2<Long, Long>> keyedMapState,
			KeySelector<BaseRow, BaseRow> keySelector) {
		this.keyedMapState = keyedMapState;
		this.keySelector = keySelector;
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
	}

	@Override
	public BaseRow getCurrentPrimaryKey() {
		return null;
	}

	@Override
	public long add(BaseRow row, long expireTime) {
		Tuple2<Long, Long> tuple2 = keyedMapState.get(joinKey, row);
		if (tuple2 == null) {
			tuple2 = new Tuple2<>();
		}
		if (tuple2.f0 == null || tuple2.f0 == 0) {
			tuple2.f0 = 1L;
		} else {
			tuple2.f0 += 1;
		}
		tuple2.f1 = expireTime;
		keyedMapState.add(joinKey, row, tuple2);
		return tuple2.f0;
	}

	@Override
	public long retract(BaseRow row) {
		//retract
		row.setHeader(ACCUMULATE_MSG);
		Tuple2<Long, Long> tuple2 = keyedMapState.get(joinKey, row);
		if (tuple2 == null) {
			tuple2 = new Tuple2<>();
		}
		if (tuple2.f0 == null || tuple2.f0 <= 1) {
			tuple2.f0 = 0L;
			keyedMapState.remove(joinKey, row);
		} else {
			tuple2.f0 -= 1;
			keyedMapState.add(joinKey, row, tuple2);
		}
		row.setHeader(RETRACT_MSG);
		return tuple2.f0;
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
		return keyedMapState.contains(key, row);
	}

	@Override
	public void update(BaseRow key, BaseRow row, long count, long expireTime) {
		keyedMapState.add(key, row, new Tuple2<>(count, expireTime));
	}

	@Override
	public void batchGet(Collection<? extends BaseRow> keys) {
		throw new RuntimeException("batchGet is not supported for WithoutPrimaryKeyStateHandler");
	}

	@Override
	public long[] batchUpdate(BaseRow key, List<Tuple2<BaseRow, Long>> rows, long expireTime) {
		// batch get old value
		Set<BaseRow> rowSet = new HashSet<>();
		long[] updateStatus = new long[rows.size()];
		long oldCnt = 0;
		int idx = 0;
		for (Tuple2<BaseRow, Long> tuple2: rows) {
			rowSet.add(tuple2.f0);
		}
		Set<BaseRow> deleteSet = new HashSet<>();
		Map<BaseRow, Tuple2<Long, Long>> oldMap = keyedMapState.getAll(key, rowSet);
		// update value
		for (Tuple2<BaseRow, Long> tuple2: rows) {
			Tuple2<Long, Long> cntAndTime = oldMap.get(tuple2.f0);
			if (cntAndTime == null) {
				cntAndTime = new Tuple2<>(0L, expireTime);
			}
			oldCnt = cntAndTime.f0;
			cntAndTime.f0 += tuple2.f1;
			if (cntAndTime.f0 == 0) {
				updateStatus[idx] = -1;
				deleteSet.add(tuple2.f0);
			} else {
				oldMap.put(tuple2.f0, cntAndTime);
				if (oldCnt == 0) {
					updateStatus[idx] = 1;
				}
			}
			idx++;
		}
		// batch update
		keyedMapState.addAll(key, oldMap);
		keyedMapState.removeAll(key, deleteSet);
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
	public void setCurrentJoinKey(BaseRow row) {
		joinKey = row;
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
		private final Iterator<Map.Entry<BaseRow, Tuple2<Long, Long>>> internalIterator;
		private final Tuple3<BaseRow, Long, Long> reuse;

		@SuppressWarnings("unchecked")
		Tuple3Iterator(Iterator<Map.Entry<BaseRow, Tuple2<Long, Long>>> internalIterator) {
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
			Map.Entry<BaseRow, Tuple2<Long, Long>> entry = internalIterator.next();
			reuse.f0 = entry.getKey();
			reuse.f1 = entry.getValue().f0;
			reuse.f2 = entry.getValue().f1;
			return reuse;
		}

		@Override
		public void remove() {
			internalIterator.remove();
		}
	}
}

