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

package org.apache.flink.table.runtime.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An LRU cache, based on <code>LinkedHashMap</code>.
 *
 * <p>This cache has a fixed maximum number of elements (<code>cacheSize</code>).
 * If the cache is full and another entry is added, the LRU (least recently
 * used) entry is dropped.
 *
 * <p>Note: This class is not thread-safe.
 */
public class LRUMap<K, V> extends LinkedHashMap<K, V> {

	private static final long serialVersionUID = 6148230381293590639L;

	private final int cacheSize;
	private final RemovalListener<K, V> removalListener;

	public LRUMap(int cacheSize) {
		this(cacheSize, null);
	}

	public LRUMap(int cacheSize, RemovalListener<K, V> removalListener) {
		super((int) Math.ceil(cacheSize / 0.75) + 1, 0.75F, true);
		this.cacheSize = cacheSize;
		this.removalListener = removalListener;
	}

	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		if (size() > cacheSize) {
			if (removalListener != null) {
				removalListener.onRemoval(eldest);
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * An object that can receive a notification when an entry is removed from a LRUMap.
	 * @param <K> the type of keys maintained by this map
	 * @param <V> the type of mapped values
	 */
	public interface RemovalListener<K, V> {
		void onRemoval(Map.Entry<K, V> eldest);
	}
}
