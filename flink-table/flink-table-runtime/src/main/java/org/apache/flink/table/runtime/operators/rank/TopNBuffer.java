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

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * TopNBuffer stores mapping from sort key to records list, sortKey is RowData type, each record is
 * RowData type. TopNBuffer could also track rank number of each record.
 */
public class TopNBuffer implements Serializable {

    private static final long serialVersionUID = 6824488508991990228L;

    private final Supplier<Collection<RowData>> valueSupplier;
    private final Comparator<RowData> sortKeyComparator;
    private int currentTopNum = 0;
    private TreeMap<RowData, Collection<RowData>> treeMap;

    public TopNBuffer(
            Comparator<RowData> sortKeyComparator, Supplier<Collection<RowData>> valueSupplier) {
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
    public int put(RowData sortKey, RowData value) {
        currentTopNum += 1;
        // update treeMap
        Collection<RowData> collection = treeMap.get(sortKey);
        if (collection == null) {
            collection = valueSupplier.get();
            treeMap.put(sortKey, collection);
        }
        collection.add(value);
        return collection.size();
    }

    /**
     * Puts a record list into the buffer under the sortKey. Note: if buffer already contains
     * sortKey, putAll will overwrite the previous value
     *
     * @param sortKey sort key with which the specified values are to be associated
     * @param values record lists to be associated with the specified key
     */
    public void putAll(RowData sortKey, Collection<RowData> values) {
        Collection<RowData> oldValues = treeMap.get(sortKey);
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
    public Collection<RowData> get(RowData sortKey) {
        return treeMap.get(sortKey);
    }

    public void remove(RowData sortKey, RowData value) {
        Collection<RowData> collection = treeMap.get(sortKey);
        if (collection != null) {
            if (collection.remove(value)) {
                currentTopNum -= 1;
            }
            if (collection.size() == 0) {
                treeMap.remove(sortKey);
            }
        }
    }

    /**
     * Removes all record list from the buffer under the sortKey.
     *
     * @param sortKey key to remove
     */
    public void removeAll(RowData sortKey) {
        Collection<RowData> collection = treeMap.get(sortKey);
        if (collection != null) {
            currentTopNum -= collection.size();
            treeMap.remove(sortKey);
        }
    }

    /**
     * Removes the last record of the last Entry in the buffer.
     *
     * @return removed record
     */
    public RowData removeLast() {
        Map.Entry<RowData, Collection<RowData>> last = treeMap.lastEntry();
        RowData lastElement = null;
        if (last != null) {
            Collection<RowData> collection = last.getValue();
            if (collection != null) {
                if (collection instanceof List) {
                    // optimization for List
                    List<RowData> list = (List<RowData>) collection;
                    if (!list.isEmpty()) {
                        lastElement = list.remove(list.size() - 1);
                        currentTopNum -= 1;
                        if (list.isEmpty()) {
                            treeMap.remove(last.getKey());
                        }
                    }
                } else {
                    lastElement = getLastElement(collection);
                    if (lastElement != null) {
                        if (collection.remove(lastElement)) {
                            currentTopNum -= 1;
                        }
                        if (collection.size() == 0) {
                            treeMap.remove(last.getKey());
                        }
                    }
                }
            }
        }
        return lastElement;
    }

    /** Returns the last record of the last Entry in the buffer. */
    public RowData lastElement() {
        Map.Entry<RowData, Collection<RowData>> last = treeMap.lastEntry();
        RowData lastElement = null;
        if (last != null) {
            Collection<RowData> collection = last.getValue();
            lastElement = getLastElement(collection);
        }
        return lastElement;
    }

    /**
     * Gets record which rank is given value.
     *
     * @param rank rank value to search
     * @return the record which rank is given value
     */
    public RowData getElement(int rank) {
        int curRank = 0;
        for (Map.Entry<RowData, Collection<RowData>> entry : treeMap.entrySet()) {
            Collection<RowData> collection = entry.getValue();
            if (curRank + collection.size() >= rank) {
                for (RowData elem : collection) {
                    curRank += 1;
                    if (curRank == rank) {
                        return elem;
                    }
                }
            } else {
                curRank += collection.size();
            }
        }
        return null;
    }

    private RowData getLastElement(Collection<RowData> collection) {
        RowData element = null;
        if (collection != null && !collection.isEmpty()) {
            if (collection instanceof List) {
                // optimize for List
                List<RowData> list = (List<RowData>) collection;
                return list.get(list.size() - 1);
            } else {
                for (RowData data : collection) {
                    element = data;
                }
            }
        }
        return element;
    }

    /** Returns a {@link Set} view of the mappings contained in the buffer. */
    public Set<Map.Entry<RowData, Collection<RowData>>> entrySet() {
        return treeMap.entrySet();
    }

    /** Returns the last Entry in the buffer. Returns null if the TreeMap is empty. */
    public Map.Entry<RowData, Collection<RowData>> lastEntry() {
        return treeMap.lastEntry();
    }

    /**
     * Returns {@code true} if the buffer contains a mapping for the specified key.
     *
     * @param key key whose presence in the buffer is to be tested
     * @return {@code true} if the buffer contains a mapping for the specified key
     */
    public boolean containsKey(RowData key) {
        return treeMap.containsKey(key);
    }

    /**
     * Gets number of total records.
     *
     * @return the number of total records.
     */
    public int getCurrentTopNum() {
        return currentTopNum;
    }

    /**
     * Gets sort key comparator used by buffer.
     *
     * @return sort key comparator used by buffer
     */
    public Comparator<RowData> getSortKeyComparator() {
        return sortKeyComparator;
    }

    /**
     * Checks whether the record should be put into the buffer.
     *
     * @param sortKey sortKey to test
     * @param topNum buffer to add
     * @return true if the record should be put into the buffer.
     */
    public boolean checkSortKeyInBufferRange(RowData sortKey, long topNum) {
        Comparator<RowData> comparator = getSortKeyComparator();
        Map.Entry<RowData, Collection<RowData>> worstEntry = lastEntry();
        if (worstEntry == null) {
            // return true if the buffer is empty.
            return true;
        } else {
            RowData worstKey = worstEntry.getKey();
            int compare = comparator.compare(sortKey, worstKey);
            if (compare < 0) {
                return true;
            } else {
                return getCurrentTopNum() < topNum;
            }
        }
    }
}
