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

package org.apache.flink.runtime.util;

import javax.annotation.Nullable;

/**
 * This {@link GroupCache} can cache group, key and value. The group and key are cache key, each key
 * belongs to a certain group. All corresponding keys and values will be cleared if a group is
 * cleared.
 *
 * @param <G> The group.
 * @param <K> The key.
 * @param <V> The value.
 */
public interface GroupCache<G, K, V> {

    /** clear all cache. */
    void clear();

    /**
     * Get value in cache.
     *
     * @return value in cache if exists, otherwise null
     */
    @Nullable
    V get(G group, K key);

    /** Put group, key and value to cache. */
    void put(G group, K key, V value);

    /** Clear all caches of the corresponding group. */
    void clearCacheForGroup(G group);
}
