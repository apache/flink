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
import org.apache.flink.table.util.StateUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The state count the size of eache join key, and the values are {@link Tuple3}.
 */
@Internal
public class CountKeySizeStateHandler implements JoinStateHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CountKeySizeStateHandler.class);

	private final KeyedValueState<BaseRow, Long> keyedValueState;

	private final KeySelector<BaseRow, BaseRow> keySelector;

	private transient BaseRow joinKey;

	// memState is used for miniBatch join, we can batch get different join key's value and put
	// in memState which can be used later
	private Map<BaseRow, Long> memState;

	public CountKeySizeStateHandler(
			KeyedValueState<BaseRow, Long> keyedValueState,
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
		Long oldValue = keyedValueState.get(joinKey);
		if (oldValue == null) {
			oldValue = 0L;
		}
		keyedValueState.put(joinKey, oldValue + 1);
		return 1;
	}

	@Override
	public long retract(BaseRow row) {
		Long oldValue = keyedValueState.get(joinKey);
		if (oldValue == null) {
			oldValue = 0L;
		}

		if (oldValue <= 1) {
			keyedValueState.remove(joinKey);
		} else {
			keyedValueState.put(joinKey, oldValue - 1);
		}

		return 0;
	}

	@Override
	public Iterator<Tuple3<BaseRow, Long, Long>> getRecords(BaseRow key) {
		return new Tuple3Iterator(keyedValueState, key);
	}

	public Iterator<Tuple3<BaseRow, Long, Long>> getRecordsFromCache(BaseRow key) {
		return new Tuple3MemIterator(memState, key);
	}

	@Override
	public boolean contains(BaseRow key, BaseRow row) {
		return keyedValueState.contains(key);
	}

	@Override
	public void update(BaseRow key, BaseRow row, long count, long expireTime) {
		Long oldValue = keyedValueState.get(key);
		if (oldValue == null) {
			oldValue = 0L;
		}
		Long newValue = oldValue + count;
		if (newValue <= 0) {
			keyedValueState.remove(key);
		} else {
			keyedValueState.put(key, newValue);
		}
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
		long [] updateStatus = new long[rows.size()];
		int idx = 0;
		Long cnt = keyedValueState.get(key);
		if (cnt == null) {
			// Assume the history count is 0 if state value is cleared.
			LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG);
			cnt = 0L;
		}
		for (Tuple2<BaseRow, Long> tuple2: rows) {
			cnt += tuple2.f1;
			idx++;
		}
		keyedValueState.put(key, cnt);
		return updateStatus;
	}

	@Override
	public void putAll(Map<BaseRow, BaseRow> putMap) {
		throw new RuntimeException("CountKeySizeState don't support putAll!");
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
		private final KeyedValueState<BaseRow, Long> keyedValueState;
		private final Tuple3<BaseRow, Long, Long> reuse;
		private final BaseRow key;
		private Long curentValue;
		private int count;

		@SuppressWarnings("unchecked")
		Tuple3Iterator(KeyedValueState<BaseRow, Long> keyedValueState, BaseRow key) {
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
					reuse.f0 = key;
					reuse.f1 = curentValue;
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

		private final Map<BaseRow, Long> keyedValueState;
		private final Tuple3<BaseRow, Long, Long> reuse;
		private final BaseRow key;
		private Long curentValue;
		private int count;

		@SuppressWarnings("unchecked")
		Tuple3MemIterator(Map<BaseRow, Long> keyedValueState, BaseRow key) {
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
					reuse.f0 = key;
					reuse.f1 = curentValue;
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
