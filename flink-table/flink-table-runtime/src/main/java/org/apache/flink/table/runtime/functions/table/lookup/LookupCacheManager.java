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

package org.apache.flink.table.runtime.functions.table.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.util.RefCounted;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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
    private static boolean keepCacheOnRelease = false;
    private final Map<String, RefCountedCache> managedCaches = new HashMap<>();

    /** Default constructor is not allowed to use. */
    private LookupCacheManager() {}

    /** Get the shared instance of {@link LookupCacheManager}. */
    public static synchronized LookupCacheManager getInstance() {
        if (instance == null) {
            instance = new LookupCacheManager();
        }
        return instance;
    }

    /**
     * Register a cache instance with identifier to the manager.
     *
     * <p>If the cache with the given identifier is already registered in the manager, this method
     * will return the registered one, otherwise this method will register the given cache into the
     * manager then return.
     *
     * @param cacheIdentifier identifier of the cache
     * @param cache instance of cache trying to register
     * @return instance of the shared cache
     */
    public synchronized LookupCache registerCacheIfAbsent(
            String cacheIdentifier, LookupCache cache) {
        checkNotNull(cache, "Could not register null cache in the manager");
        RefCountedCache refCountedCache =
                managedCaches.computeIfAbsent(
                        cacheIdentifier, identifier -> new RefCountedCache(cache));
        refCountedCache.retain();
        return refCountedCache.cache;
    }

    /**
     * Release the cache with the given identifier from the manager.
     *
     * <p>The manager will track a reference count of managed caches, and will close the cache if
     * the reference count reaches 0.
     */
    public synchronized void unregisterCache(String cacheIdentifier) {
        RefCountedCache refCountedCache =
                checkNotNull(
                        managedCaches.get(cacheIdentifier),
                        "Cache identifier '%s' is not registered",
                        cacheIdentifier);
        if (refCountedCache.release()) {
            managedCaches.remove(cacheIdentifier);
        }
    }

    /**
     * A wrapper class of {@link LookupCache} which also tracks the reference count of it.
     *
     * <p>This class is exposed as public for testing purpose and not thread safe. Concurrent
     * accesses should be guarded by synchronized methods provided by {@link LookupCacheManager}.
     */
    @NotThreadSafe
    @VisibleForTesting
    public static class RefCountedCache implements RefCounted {
        private final LookupCache cache;
        private int refCount;

        public RefCountedCache(LookupCache cache) {
            this.cache = cache;
            this.refCount = 0;
        }

        @Override
        public void retain() {
            refCount++;
        }

        @Override
        public boolean release() {
            checkState(refCount > 0, "Could not release a cache with refCount = 0");
            if (--refCount == 0 && !keepCacheOnRelease) {
                closeCache();
                return true;
            }
            return false;
        }

        public LookupCache getCache() {
            return cache;
        }

        private void closeCache() {
            try {
                cache.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close the cache", e);
            }
        }
    }

    // ---------------------------- For testing purpose ------------------------------
    public static void keepCacheOnRelease(boolean toKeep) {
        keepCacheOnRelease = toKeep;
    }

    public void checkAllReleased() {
        if (managedCaches.isEmpty()) {
            return;
        }
        String leakedCaches =
                managedCaches.entrySet().stream()
                        .filter(entry -> entry.getValue().refCount != 0)
                        .map(
                                entry ->
                                        String.format(
                                                "#Reference: %d with ID: %s",
                                                entry.getValue().refCount, entry.getKey()))
                        .collect(Collectors.joining("\n"));
        if (!leakedCaches.isEmpty()) {
            throw new IllegalStateException(
                    "Cache leak detected. Unreleased caches: \n" + leakedCaches);
        }
    }

    public void clear() {
        managedCaches.forEach((identifier, cache) -> cache.closeCache());
        managedCaches.clear();
    }

    public Map<String, RefCountedCache> getManagedCaches() {
        return managedCaches;
    }
}
