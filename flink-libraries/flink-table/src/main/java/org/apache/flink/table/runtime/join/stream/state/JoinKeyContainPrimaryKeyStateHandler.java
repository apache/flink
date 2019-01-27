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
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The state whose keys are joinKeys, and the values are {@link Tuple3}.
 */
@Internal
public class JoinKeyContainPrimaryKeyStateHandler implements JoinStateHandler {

	private final KeyedValueState<BaseRow, BaseRow> keyedValueState;

	private final KeySelector<BaseRow, BaseRow> keySelector;

	private transient BaseRow joinKey;

	// memState is used for miniBatch join, we can batch get different join key's value and put
	// in memState which can be used later
	private Map<BaseRow, BaseRow> memState;

	public JoinKeyContainPrimaryKeyStateHandler(
			KeyedValueState<BaseRow, BaseRow> keyedValueState,
			KeySelector<BaseRow, BaseRow> keySelector) {
		this.keySelector = keySelector;
		this.keyedValueState = keyedValueState;
		this.memState = new HashMap<>();
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
		keyedValueState.put(joinKey, row);
		return 1;
	}

	@Override
	public long retract(BaseRow row) {
		keyedValueState.remove(joinKey);
		return 0;
	}

	@Override
	public Iterator<Tuple3<BaseRow, Long, Long>> getRecords(BaseRow key) {
		return new Tuple3Iterator(keyedValueState, key);
	}

	@Override
	public Iterator<Tuple3<BaseRow, Long, Long>> getRecordsFromCache(BaseRow key) {
		return new Tuple3MemIterator(memState, key);
	}

	@Override
	public boolean contains(BaseRow key, BaseRow row) {
		return keyedValueState.contains(key);
	}

	@Override
	public void update(BaseRow key, BaseRow row, long count, long expireTime) {
		keyedValueState.put(key, row);
	}

	@Override
	public void remove(BaseRow joinKey) {
		keyedValueState.remove(joinKey);
	}

	// batch operators
	public void batchGet(Collection<? extends BaseRow> keys) {
		memState = keyedValueState.getAll(keys);
	}

	@Override
	public long[] batchUpdate(BaseRow key, List<Tuple2<BaseRow, Long>> rows, long expireTime) {
		// Note: value state batchUpdate will do nothing, we use putAll to batch update data. Because
		// every key contains only one rows for value state.

		long [] updateStatus = new long[rows.size()];
		int idx = 0;
		// Here we only care last value, can always set update status to 1 if f1 > 0. In this
		// way, we will always update match state, but we can avoid pre-read join state value.
		// Since write is faster than read, it is valuable to do it.
		Tuple2<BaseRow, Long> lastTuple = null;
		for (Tuple2<BaseRow, Long> tuple2: rows) {
			lastTuple = tuple2;
			idx++;
		}
		if (lastTuple != null) {
			if (lastTuple.f1 < 0) {
				updateStatus[rows.size() - 1] = -1;
			} else if (lastTuple.f1 > 0) {
				updateStatus[rows.size() - 1] = 1;
			}
		}
		return updateStatus;
	}

	@Override
	public void putAll(Map<BaseRow, BaseRow> putMap) {
		keyedValueState.putAll(putMap);
	}

	@Override
	public void removeAll(Set<BaseRow> keys) {
		keyedValueState.removeAll(keys);
	}

	@Override
	public void setCurrentJoinKey(BaseRow row) {
		joinKey = row;
	}

	/**
	 * An iterator over the elements under the same joinKey which is backed by an
	 * iterator in the internal state.
	 */
	private class Tuple3Iterator implements Iterator<Tuple3<BaseRow, Long, Long>> {
		private final KeyedValueState<BaseRow, BaseRow> keyedValueState;
		private final Tuple3<BaseRow, Long, Long> reuse;
		private final BaseRow key;
		private BaseRow curentValue;
		private int count;

		@SuppressWarnings("unchecked")
		Tuple3Iterator(KeyedValueState<BaseRow, BaseRow> keyedValueState, BaseRow key) {
			Preconditions.checkNotNull(keyedValueState);
			this.keyedValueState = keyedValueState;
			this.key = key;
			this.reuse = new Tuple3<>();
		}

		@Override
		public boolean hasNext() {
			if (count == 0 && keyedValueState.get(key) != null) {
				return true;
			} else {
				return false;
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Tuple3<BaseRow, Long, Long> next() {
			if (count == 0) {
				curentValue = keyedValueState.get(key);
				if (curentValue != null) {
					reuse.f0 = curentValue;
					reuse.f1 = 1L;
					reuse.f2 = Long.MAX_VALUE;
					count++;
					return reuse;
				}
			}
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			keyedValueState.remove(key);
		}
	}

	private class Tuple3MemIterator implements Iterator<Tuple3<BaseRow, Long, Long>> {

		private final Map<BaseRow, BaseRow> keyedValueState;
		private final Tuple3<BaseRow, Long, Long> reuse;
		private final BaseRow key;
		private BaseRow curentValue;
		private int count;

		@SuppressWarnings("unchecked")
		Tuple3MemIterator(Map<BaseRow, BaseRow> keyedValueState, BaseRow key) {
			Preconditions.checkNotNull(keyedValueState);
			this.keyedValueState = keyedValueState;
			this.key = key;
			this.reuse = new Tuple3<>();
		}

		@Override
		public boolean hasNext() {
			if (count == 0 && keyedValueState.get(key) != null) {
				return true;
			} else {
				return false;
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Tuple3<BaseRow, Long, Long> next() {
			if (count == 0) {
				curentValue = keyedValueState.get(key);
				if (curentValue != null) {
					reuse.f0 = curentValue;
					reuse.f1 = 1L;
					reuse.f2 = Long.MAX_VALUE;
					count++;
					return reuse;
				}
			}
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			keyedValueState.remove(key);
		}
	}
}
