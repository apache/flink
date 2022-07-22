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

package org.apache.flink.table.connector.source.lookup.cache;

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Managing shared caches across different subtasks.
 *
 * <p>In order to reduce the memory usage of cache, different subtasks of the same lookup join
 * runner will share the same cache instance. Caches are managed by the identifier of the lookup
 * table for which it is serving.
 */
@Internal
public class LookupCacheManager {
    private static LookupCacheManager instance;
    private final Map<String, LookupCache> cachesByTableIdentifier = new HashMap<>();

    /** Get the shared instance of {@link LookupCacheManager}. */
    public static synchronized LookupCacheManager getInstance() {
        if (instance == null) {
            instance = new LookupCacheManager();
        }
        return instance;
    }

    /**
     * Register a cache instance with table identifier to the manager.
     *
     * <p>If the cache with the given table identifier is already registered in the manager, this
     * method will return the registered one, otherwise this method will register the given cache
     * into the manager then return.
     *
     * @param tableIdentifier table identifier
     * @param cache instance of cache trying to register
     * @return instance of the shared cache
     */
    public synchronized LookupCache registerCache(String tableIdentifier, LookupCache cache) {
        checkNotNull(cache, "Could not register null cache in the manager");
        if (cachesByTableIdentifier.containsKey(tableIdentifier)) {
            return cachesByTableIdentifier.get(tableIdentifier);
        } else {
            cachesByTableIdentifier.put(tableIdentifier, cache);
            return cache;
        }
    }
}
