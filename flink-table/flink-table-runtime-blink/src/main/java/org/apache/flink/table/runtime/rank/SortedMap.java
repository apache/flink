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

package org.apache.flink.table.runtime.rank;

import org.apache.flink.table.dataformat.BaseRow;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * SortedMap stores mapping from sort key to records list, each record is BaseRow type.
 * SortedMap could also track rank number of each records.
 *
 * @param <T> Type of the sort key
 */
public class SortedMap<T> {

	private final Supplier<Collection<BaseRow>> valueSupplier;
	private int currentTopNum = 0;
	private TreeMap<T, Collection<BaseRow>> treeMap;

	public SortedMap(Comparator<T> sortKeyComparator, Supplier<Collection<BaseRow>> valueSupplier) {
		this.valueSupplier = valueSupplier;
		this.treeMap = new TreeMap(sortKeyComparator);
	}

	/**
	 * Appends a record into the SortedMap under the sortKey.
	 *
	 * @param sortKey sort key with which the specified value is to be associated
	 * @param value   record which is to be appended
	 *
	 * @return the size of the collection under the sortKey.
	 */
	public int put(T sortKey, BaseRow value) {
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
	 * Puts a record list into the SortedMap under the sortKey.
	 * Note: if SortedMap already contains sortKey, putAll will overwrite the previous value
	 *
	 * @param sortKey sort key with which the specified values are to be associated
	 * @param values  record lists to be associated with the specified key
	 */
	public void putAll(T sortKey, Collection<BaseRow> values) {
		treeMap.put(sortKey, values);
		currentTopNum += values.size();
	}

	/**
	 * Get the record list from SortedMap under the sortKey.
	 *
	 * @param sortKey key to get
	 *
	 * @return the record list from SortedMap under the sortKey
	 */
	public Collection<BaseRow> get(T sortKey) {
		return treeMap.get(sortKey);
	}

	public void remove(T sortKey, BaseRow value) {
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
	 * Remove all record list from SortedMap under the sortKey.
	 *
	 * @param sortKey key to remove
	 */
	public void removeAll(T sortKey) {
		Collection<BaseRow> list = treeMap.get(sortKey);
		if (list != null) {
			currentTopNum -= list.size();
			treeMap.remove(sortKey);
		}
	}

	/**
	 * Remove the last record of the last Entry in the TreeMap (according to the TreeMap's
	 * key-sort function).
	 *
	 * @return removed record
	 */
	public BaseRow removeLast() {
		Map.Entry<T, Collection<BaseRow>> last = treeMap.lastEntry();
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
	 * Get record which rank is given value.
	 *
	 * @param rank rank value to search
	 *
	 * @return the record which rank is given value
	 */
	public BaseRow getElement(int rank) {
		int curRank = 0;
		Iterator<Map.Entry<T, Collection<BaseRow>>> iter = treeMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<T, Collection<BaseRow>> entry = iter.next();
			Collection<BaseRow> list = entry.getValue();

			Iterator<BaseRow> listIter = list.iterator();
			while (listIter.hasNext()) {
				BaseRow elem = listIter.next();
				curRank += 1;
				if (curRank == rank) {
					return elem;
				}
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
	 * Returns a {@link Set} view of the mappings contained in this map.
	 */
	public Set<Map.Entry<T, Collection<BaseRow>>> entrySet() {
		return treeMap.entrySet();
	}

	/**
	 * Returns the last Entry in the TreeMap (according to the TreeMap's
	 * key-sort function).  Returns null if the TreeMap is empty.
	 */
	public Map.Entry<T, Collection<BaseRow>> lastEntry() {
		return treeMap.lastEntry();
	}

	/**
	 * Returns {@code true} if this map contains a mapping for the specified
	 * key.
	 *
	 * @param key key whose presence in this map is to be tested
	 *
	 * @return {@code true} if this map contains a mapping for the
	 * specified key
	 */
	public boolean containsKey(T key) {
		return treeMap.containsKey(key);
	}

	/**
	 * Get number of total records.
	 *
	 * @return the number of total records.
	 */
	public int getCurrentTopNum() {
		return currentTopNum;
	}

}
