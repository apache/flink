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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.table.dataformat.BaseRow;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * TopNBuffer stores mapping from sort key to records list, sortKey is BaseRow type, each record is BaseRow type.
 * TopNBuffer could also track rank number of each record.
 */
class TopNBuffer implements Serializable {

	private static final long serialVersionUID = 6824488508991990228L;

	private final Supplier<Collection<BaseRow>> valueSupplier;
	private final Comparator<BaseRow> sortKeyComparator;
	private int currentTopNum = 0;
	private TreeMap<BaseRow, Collection<BaseRow>> treeMap;

	TopNBuffer(Comparator<BaseRow> sortKeyComparator, Supplier<Collection<BaseRow>> valueSupplier) {
		this.valueSupplier = valueSupplier;
		this.sortKeyComparator = sortKeyComparator;
		this.treeMap = new TreeMap(sortKeyComparator);
	}

	/**
	 * Appends a record into the buffer.
	 *
	 * @param sortKey sort key with which the specified value is to be associated
	 * @param value record which is to be appended
	 * @return the size of the collection under the sortKey.
	 */
	public int put(BaseRow sortKey, BaseRow value) {
		currentTopNum += 1;
		// update treeMap
		Collection<BaseRow> collection = treeMap.get(sortKey);
		if (collection == null) {
			collection = valueSupplier.get();
			treeMap.put(sortKey, collection);
		}
		collection.add(value);
		return collection.size();
	}

	/**
	 * Puts a record list into the buffer under the sortKey.
	 * Note: if buffer already contains sortKey, putAll will overwrite the previous value
	 *
	 * @param sortKey sort key with which the specified values are to be associated
	 * @param values record lists to be associated with the specified key
	 */
	void putAll(BaseRow sortKey, Collection<BaseRow> values) {
		Collection<BaseRow> oldValues = treeMap.get(sortKey);
		if (oldValues != null) {
			currentTopNum -= oldValues.size();
		}
		treeMap.put(sortKey, values);
		currentTopNum += values.size();
	}

	/**
	 * Gets the record list from the buffer under the sortKey.
	 *
	 * @param sortKey key to get
	 * @return the record list from the buffer under the sortKey
	 */
	public Collection<BaseRow> get(BaseRow sortKey) {
		return treeMap.get(sortKey);
	}

	public void remove(BaseRow sortKey, BaseRow value) {
		Collection<BaseRow> list = treeMap.get(sortKey);
		if (list != null) {
			if (list.remove(value)) {
				currentTopNum -= 1;
			}
			if (list.size() == 0) {
				treeMap.remove(sortKey);
			}
		}
	}

	/**
	 * Removes all record list from the buffer under the sortKey.
	 *
	 * @param sortKey key to remove
	 */
	void removeAll(BaseRow sortKey) {
		Collection<BaseRow> list = treeMap.get(sortKey);
		if (list != null) {
			currentTopNum -= list.size();
			treeMap.remove(sortKey);
		}
	}

	/**
	 * Removes the last record of the last Entry in the buffer.
	 *
	 * @return removed record
	 */
	BaseRow removeLast() {
		Map.Entry<BaseRow, Collection<BaseRow>> last = treeMap.lastEntry();
		BaseRow lastElement = null;
		if (last != null) {
			Collection<BaseRow> list = last.getValue();
			lastElement = getLastElement(list);
			if (lastElement != null) {
				if (list.remove(lastElement)) {
					currentTopNum -= 1;
				}
				if (list.size() == 0) {
					treeMap.remove(last.getKey());
				}
			}
		}
		return lastElement;
	}

	/**
	 * Gets record which rank is given value.
	 *
	 * @param rank rank value to search
	 * @return the record which rank is given value
	 */
	BaseRow getElement(int rank) {
		int curRank = 0;
		for (Map.Entry<BaseRow, Collection<BaseRow>> entry : treeMap.entrySet()) {
			Collection<BaseRow> list = entry.getValue();

			if (curRank + list.size() >= rank) {
				for (BaseRow elem : list) {
					curRank += 1;
					if (curRank == rank) {
						return elem;
					}
				}
			} else {
				curRank += list.size();
			}
		}
		return null;
	}

	private BaseRow getLastElement(Collection<BaseRow> list) {
		BaseRow element = null;
		if (list != null && !list.isEmpty()) {
			Iterator<BaseRow> iter = list.iterator();
			while (iter.hasNext()) {
				element = iter.next();
			}
		}
		return element;
	}

	/**
	 * Returns a {@link Set} view of the mappings contained in the buffer.
	 */
	Set<Map.Entry<BaseRow, Collection<BaseRow>>> entrySet() {
		return treeMap.entrySet();
	}

	/**
	 * Returns the last Entry in the buffer. Returns null if the TreeMap is empty.
	 */
	Map.Entry<BaseRow, Collection<BaseRow>> lastEntry() {
		return treeMap.lastEntry();
	}

	/**
	 * Returns {@code true} if the buffer contains a mapping for the specified key.
	 *
	 * @param key key whose presence in the buffer is to be tested
	 * @return {@code true} if the buffer contains a mapping for the specified key
	 */
	boolean containsKey(BaseRow key) {
		return treeMap.containsKey(key);
	}

	/**
	 * Gets number of total records.
	 *
	 * @return the number of total records.
	 */
	int getCurrentTopNum() {
		return currentTopNum;
	}

	/**
	 * Gets sort key comparator used by buffer.
	 *
	 * @return sort key comparator used by buffer
	 */
	Comparator<BaseRow> getSortKeyComparator() {
		return sortKeyComparator;
	}
}
