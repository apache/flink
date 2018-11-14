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

package org.apache.flink.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A LRUCache by LinkedHashMap.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> implements java.io.Serializable {

	private final int maxCacheSize;

	public LRUCache(int cacheSize) {
		super((int) Math.ceil(cacheSize / 0.75) + 1, 0.75f, true);
		maxCacheSize = cacheSize;
	}

	@Override
	protected boolean removeEldestEntry(Map.Entry eldest) {
		return size() > maxCacheSize;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<K, V> entry : entrySet()) {
			sb.append(String.format("%s:%s ", entry.getKey(), entry.getValue()));
		}
		return sb.toString();
	}
}
