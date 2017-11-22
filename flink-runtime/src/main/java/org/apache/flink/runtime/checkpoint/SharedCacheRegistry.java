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
package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCache.CacheEntry;
import org.apache.flink.runtime.checkpoint.CheckpointCache.CacheKey;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Used by {@link CheckpointCache} to support incremental Cache. SharedCacheRegistry maintains
 * all CacheEntries, {@link CacheEntry} is deleted only if the reference count become zero.
 */
public class SharedCacheRegistry {

	private static Logger LOG = LoggerFactory.getLogger(SharedCacheRegistry.class);
	private final Map<CacheKey, CacheEntry> registeredCacheEntry;

	/** Executor for async state deletion */
	private final Executor asyncDisposalExecutor;

	public SharedCacheRegistry(Executor asyncDisposalExecutor) {
		this.registeredCacheEntry = new ConcurrentHashMap<>();
		this.asyncDisposalExecutor = asyncDisposalExecutor;
	}

	public void registerReference(CacheKey key, CacheEntry value) {
		synchronized (registeredCacheEntry) {
			CacheEntry entry = registeredCacheEntry.get(key);
			CacheEntry entryToDelete = null;
			if (entry == null || entry.isRot()) {
				if (entry != null) {
					value.setReference(entry.getReferenceCount());
					entryToDelete = entry;
				}
				entry = value;
				registeredCacheEntry.put(key, value);
			} else {
				if (!Objects.equals(entry, value)) {
					entryToDelete = value;
				}
			}
			entry.increaseReferenceCount();

			scheduleAsyncDelete(entryToDelete);
		}
	}

	public void unregisterReference(CacheKey key) {
		synchronized (registeredCacheEntry) {
			CacheEntry entry = registeredCacheEntry.get(key);
			if (entry.decreaseReferenceCount() <= 0) {
				registeredCacheEntry.remove(key);
				scheduleAsyncDelete(entry);
			}
		}
	}

	public CacheEntry getCacheEntry(CacheKey key) {
		synchronized (registeredCacheEntry) {
			return registeredCacheEntry.get(key);
		}
	}

	private void scheduleAsyncDelete(CacheEntry entry) {
		// We do the small optimization to not issue discards for placeholders, which are NOPs.
		if (entry != null) {
			LOG.trace("Scheduled delete of cache entry {}.", entry);
			asyncDisposalExecutor.execute(() -> {
				try {
					entry.discard();
				} catch (Exception e) {
					LOG.warn("A problem occurred during asynchronous disposal of a cache entry: {}", entry, e);
				}
			});
		}
	}

	@VisibleForTesting
	protected Iterable<Map.Entry<CacheKey, CacheEntry>> getIterable() {
		final Iterator<Map.Entry<CacheKey, CacheEntry>> iterator = registeredCacheEntry.entrySet().iterator();
		return new Iterable<Map.Entry<CacheKey, CacheEntry>>() {
			@Override
			public Iterator<Map.Entry<CacheKey, CacheEntry>> iterator() {
				return new Iterator<Map.Entry<CacheKey, CacheEntry>>()	{

					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Map.Entry<CacheKey, CacheEntry> next() {
						return iterator.next();
					}

					@Override
					public void remove() {
						throw new FlinkRuntimeException("unsupported method.");
					}
				};
			}
		};
	}
}
