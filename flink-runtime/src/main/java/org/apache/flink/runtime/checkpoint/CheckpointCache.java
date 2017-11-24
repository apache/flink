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
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created from {@link CheckpointCacheManager} and used by {@link org.apache.flink.runtime.taskmanager.Task} to cache checkpoint data locally,
 * it maintains the last success completed checkpoint cache and uses {@link SharedCacheRegistry}
 * to manage the cache entry, backend uses the output stream to transmission data to both network and local,
 * local data maintained by {@link PendingCheckpointCache}, we turn {@link PendingCheckpointCache} into {@link CompletedCheckpointCache}
 * in notifyCheckpointComplete().
 */
public class CheckpointCache {

	private static Logger LOG = LoggerFactory.getLogger(CheckpointCache.class);

	// a special checkpoint id used when do re-cache for incremental checkpoint
	public static final long CHECKPOINT_ID_FOR_RESTORE = -1L;

	// max retain completed cache num
	public static final int MAX_RETAIN_NUM = 1;

	// cache manager
	private final CheckpointCacheManager cacheManager;
	private final JobID jobID;
	private final Path basePath;
	private final AtomicInteger reference;
	private final Object lock = new Object();

	// pending checkpoint cache timeout
	private final long pendingCheckpointCacheTimeout;

	//checkpoint cache lease ttl
	private final long leaseTimeout;

	// executor for dispose resource
	private final Executor executor;

	private final ScheduledThreadPoolExecutor timer;

	// pending cache map
	private final Map<Long, PendingCheckpointCache> pendingCacheMap;

	// completed checkpoint cache map
	private final ArrayDeque<CompletedCheckpointCache> completedCheckpointCaches;

	// shared cache registry
	private final SharedCacheRegistry sharedCacheRegistry;

	public CheckpointCache(JobID jobID, String basePath, long pendingCheckpointCacheTimeout, long leaseTimeout, CheckpointCacheManager manager, Executor executor) {
		this.basePath = new Path(basePath + File.separator + "checkpoint_cache" + File.separator + jobID);
		try {
			this.basePath.getFileSystem().mkdirs(this.basePath);
		} catch (IOException e) {
			throw new FlinkRuntimeException("init checkpoint cache manager failed:{}", e);
		}
		this.cacheManager = manager;
		this.jobID = jobID;
		this.reference = new AtomicInteger(0);
		this.pendingCheckpointCacheTimeout = pendingCheckpointCacheTimeout;
		this.leaseTimeout = leaseTimeout;
		this.executor = executor;
		this.timer = new ScheduledThreadPoolExecutor(1,
			new DispatcherThreadFactory(Thread.currentThread().getThreadGroup(), "Checkpoint Cache Timer"));
		this.pendingCacheMap = new ConcurrentHashMap<>();
		this.completedCheckpointCaches = new ArrayDeque<>(MAX_RETAIN_NUM + 1);
		this.sharedCacheRegistry = new SharedCacheRegistry(executor);

		LOG.info("new checkpoint cache, pendingCacheTimeout: {}, leaseTimeout: {}", pendingCheckpointCacheTimeout, leaseTimeout);
	}

	protected void registerCacheEntry(long checkpointID, StateHandleID handleID, String filePath) {
		synchronized (lock) {
			LOG.debug("register cache entry: { cpkID:[{}] handleID:[{}] }", checkpointID, handleID);
			PendingCheckpointCache pendingCheckpointCache = pendingCacheMap.get(checkpointID);
			if (pendingCheckpointCache == null) {
				PendingCheckpointCache newPendingCheckpointCache = new PendingCheckpointCache(executor, checkpointID);
				LOG.debug("add pending cache map: { cpkID:[{}] }", checkpointID);
				pendingCacheMap.put(checkpointID, newPendingCheckpointCache);
				pendingCheckpointCache = newPendingCheckpointCache;

				// schedule the timer that will clean up the expired checkpoints
				ScheduledFuture<?> cancellerHandle = timer.schedule(
					() -> {
						synchronized (lock) {
							if (!newPendingCheckpointCache.isDiscarded()) {
								LOG.info("Checkpoint cache " + checkpointID + " expired before completing.");
								newPendingCheckpointCache.abortExpired();
								pendingCacheMap.remove(checkpointID);
							}
						}
					},
					pendingCheckpointCacheTimeout, TimeUnit.MILLISECONDS);

				if (!newPendingCheckpointCache.setCancellerHandle(cancellerHandle)) {
					cancellerHandle.cancel(false);
				}
			}

			pendingCheckpointCache.addEntry(
				new CacheKey(handleID),
				new CacheEntry(new FileStateHandle(new Path(filePath), getFileSize(filePath))));
		}
	}

	private long getFileSize(String filePath) {
		File file = new File(filePath);
		if (file.exists() && file.isFile()) {
			return file.length();
		} else {
			return 0L;
		}
	}

	protected void abortPendingCache(long checkpointID) {
		LOG.info("abort pending cache: {}", checkpointID);
		synchronized (lock) {
			PendingCheckpointCache pendingCheckpointCache = pendingCacheMap.get(checkpointID);
			if (pendingCheckpointCache != null) {
				pendingCheckpointCache.abortSubsumed();
			}
		}
	}

	public void commitCache(long checkpointID) {
		commitCache(checkpointID, true);
	}

	public void commitCache(long checkpointID, boolean dropUnRetainCheckpointCache) {
		synchronized (lock) {
			final PendingCheckpointCache pendingCheckpointCache;
			pendingCheckpointCache = pendingCacheMap.remove(checkpointID);
			if (pendingCheckpointCache != null) {
				LOG.info("commit pending checkpoint cache: {}", checkpointID);
				// here will build reference on cache entry
				CompletedCheckpointCache completedCheckpointCache = new CompletedCheckpointCache(sharedCacheRegistry, checkpointID);
				for (Map.Entry<CacheKey, CacheEntry> entry : pendingCheckpointCache.getEntryIterable()) {
					completedCheckpointCache.addCacheEntry(entry.getKey(), entry.getValue());
				}

				this.completedCheckpointCaches.add(completedCheckpointCache);
				pendingCheckpointCache.cancelCanceller();

				if (dropUnRetainCheckpointCache) {
					// only maintain the last complete checkpoint
					dropUnRetainCheckpointCache(MAX_RETAIN_NUM);
				}

				// subsume pending checkpoint cache
				dropSubsumedPendingCheckpointCache(checkpointID);
			} else {
				LOG.debug("{} pending checkpoint cache is not exists. This means it has been committed or expired", checkpointID);
			}
		}
	}

	public int getPendingCheckpointCacheSize() {
		synchronized (lock) {
			return pendingCacheMap.size();
		}
	}

	public int getCompletedCheckpointCacheSize() {
		synchronized (lock) {
			return completedCheckpointCaches.size();
		}
	}

	private void dropUnRetainCheckpointCache(int maxRetainNum) {
		while (this.completedCheckpointCaches.size() > maxRetainNum) {
			CompletedCheckpointCache completedCheckpointCache = completedCheckpointCaches.removeFirst();
			LOG.debug("remove checkpoint cache:{}", completedCheckpointCache.getCheckpointID());
			completedCheckpointCache.discard();
		}
	}

	private void dropSubsumedPendingCheckpointCache(long checkpointID) {
		Iterator<Map.Entry<Long, PendingCheckpointCache>> entries = this.pendingCacheMap.entrySet().iterator();
		while (entries.hasNext()) {
			PendingCheckpointCache p = entries.next().getValue();
			// remove all pending checkpoints that are lesser than the current completed checkpoint
			if (p.getCheckpointID() < checkpointID) {
				LOG.debug("remove subsumed pending checkpoint: {} < {}", p.getCheckpointID(), checkpointID);
				p.abortSubsumed();
				entries.remove();
			}
		}
	}

	public String getBasePath() {
		return basePath.getPath();
	}

	public void discard() {
		cacheManager.unregisterCheckpointCache(jobID);
	}

	public void release() {
		synchronized (lock) {
			dropSubsumedPendingCheckpointCache(Long.MAX_VALUE);
			dropUnRetainCheckpointCache(0);
		}
	}

	@VisibleForTesting
	protected SharedCacheRegistry getSharedCacheRegister() {
		return this.sharedCacheRegistry;
	}

	public CachedOutputStream createOutputStream(long checkpointID, StateHandleID handleID) {
		return createOutputStream(checkpointID, handleID, false);
	}

	public CachedOutputStream createOutputStream(long checkpointID, StateHandleID handleID, boolean placeholder) {
		LOG.debug("create cache output: {} {}", checkpointID, placeholder);
		try {
			File basePathDir = new File(this.basePath.getPath());
			//sanity check
			if (!basePathDir.exists()) {
				if (!basePathDir.mkdirs()) {
					LOG.warn("init checkpoint cache base path {} failed.", this.basePath.getPath());
					return null;
				}
			}
			final String cacheFilePath = basePath + File.separator + handleID + "_" + UUID.randomUUID();
			return new CachedOutputStream(checkpointID, handleID, cacheFilePath, this, placeholder);
		} catch (Exception ignore) {
			// warning
			LOG.warn("create output stream failed: {}", ignore);
		}
		return null;
	}

	public FSDataInputStream openInputStream(StateHandleID cacheId) {
		LOG.debug("try to open input stream from cache with cacheID:" + cacheId);
		CacheEntry entry = sharedCacheRegistry.getCacheEntry(new CacheKey(cacheId));
		if (entry != null) {
			try {
				LOG.debug("entry path: {}", entry.getHandle().getFilePath());
				return entry.getHandle().openInputStream();
			} catch (Exception ignore) {
				entry.rot(true);
				return null;
			}
		}
		return null;
	}

	public int reference() {
		return this.reference.incrementAndGet();
	}

	public int dereference() {
		return this.reference.decrementAndGet();
	}

	public int getReference() {
		return this.reference.get();
	}

	public static class CachedOutputStream extends OutputStream {

		private final OutputStream outputStream;
		private final StateHandleID cacheID;
		private final long checkpointID;

		private final String cacheFilePath;
		private final CheckpointCache cache;
		private boolean discarded;

		public CachedOutputStream(
			long checkpointID,
			StateHandleID cacheID,
			String cacheFilePath,
			CheckpointCache cache,
			boolean placeholder
		) throws FileNotFoundException {
			this.checkpointID = checkpointID;
			this.cacheID = cacheID;
			this.cacheFilePath = cacheFilePath;
			if (!placeholder) {
				this.outputStream = new FileOutputStream(cacheFilePath);
			} else {
				this.outputStream = null;
			}
			this.cache = cache;
			this.discarded = false;
		}

		public long getCheckpointID() {
			return this.checkpointID;
		}

		public StateHandleID getCacheID() {
			return this.cacheID;
		}

		public boolean isDiscarded() {
			return this.discarded;
		}

		public void discard() {
			LOG.info("cache output stream discard: {}", checkpointID);
			discarded = true;
		}

		@Override
		public void write(int b) throws IOException {
			if (!discarded && outputStream != null) {
				outputStream.write(b);
			}
		}

		@Override
		public void write(byte[] b) throws IOException {
			if (!discarded && outputStream != null) {
				outputStream.write(b);
			}
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (!discarded && outputStream != null) {
				outputStream.write(b, off, len);
			}
		}

		@Override
		public void flush() throws IOException {
			if (!discarded && outputStream != null) {
				outputStream.flush();
			}
		}

		@Override
		public void close() throws IOException {
			if (outputStream != null) {
				outputStream.close();
			}
		}

		public void end() {
			if (!discarded) {
				this.cache.registerCacheEntry(checkpointID, cacheID, cacheFilePath);
			} else {
				this.cache.abortPendingCache(checkpointID);
			}
		}

		public String getCacheFilePath() {
			return cacheFilePath;
		}
	}

	public long getLeaseTimeout() {
		return leaseTimeout;
	}

	public static class CacheKey {
		private StateHandleID handleID;
		public CacheKey(StateHandleID handleID) {
			this.handleID = handleID;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			CacheKey cacheKey = (CacheKey) o;

			return handleID != null ? handleID.equals(cacheKey.handleID) : cacheKey.handleID == null;
		}

		@Override
		public int hashCode() {
			int result = handleID != null ? handleID.hashCode() : 0;
			return result;
		}

		@Override
		public String toString() {
			return handleID.toString();
		}
	}

	public static class CacheEntry {
		private final FileStateHandle handle;
		private final AtomicInteger reference;

		public boolean isRot() {
			return rot;
		}

		public void rot(boolean isRot) {
			this.rot = isRot;
		}

		private boolean rot;

		public CacheEntry(FileStateHandle handle) {
			this.handle = handle;
			this.reference = new AtomicInteger(0);
			this.rot = false;
		}

		public FileStateHandle getHandle() {
			return handle;
		}

		public void discard() {
			try {
				//TODO: this should discard with future.
				this.handle.discardState();
			} catch (Exception e) {
				LOG.warn("discard handle failed: {}", e);
			}
		}

		public int increaseReferenceCount() {
			return reference.incrementAndGet();
		}

		public int decreaseReferenceCount() {
			return reference.decrementAndGet();
		}

		public int getReferenceCount() {
			return reference.get();
		}

		public void setReference(int refer) {
			reference.set(refer);
		}
	}

	public static class CompletedCheckpointCache {
		private final long checkpointID;
		private final Set<CacheKey> cacheKeys;
		private final SharedCacheRegistry registry;

		public CompletedCheckpointCache(SharedCacheRegistry registry, long checkpointID) {
			this.checkpointID = checkpointID;
			this.cacheKeys = new HashSet<>();
			this.registry = registry;
		}

		public void addCacheEntry(CacheKey key, CacheEntry value) {
			registry.registerReference(key, value);
			cacheKeys.add(key);
		}

		public long getCheckpointID() {
			return checkpointID;
		}

		public void discard() {
			for (CacheKey key : cacheKeys) {
				registry.unregisterReference(key);
			}
		}
	}

	public static class PendingCheckpointCache {
		private final long checkpointID;
		private boolean discarded;
		private final Map<CacheKey, CacheEntry> pendingEntry;
		private ScheduledFuture<?> cancellerHandle;
		private final Executor executor;

		public PendingCheckpointCache(Executor executor, long checkpointID) {
			this.checkpointID = checkpointID;
			this.pendingEntry = new HashMap<>();
			this.discarded = false;
			this.executor = executor;
		}

		public void addEntry(CacheKey key, CacheEntry entry) {
			if (!discarded) {
				CacheEntry preEntry = pendingEntry.get(key);
				if (preEntry != null) {
					throw new FlinkRuntimeException("register twice in pending cache map with the same key: { "  + key.handleID + "}");
				}
				pendingEntry.put(key, entry);
			}
		}

		public Iterable<Map.Entry<CacheKey, CacheEntry>> getEntryIterable() {
			return discarded ? null : pendingEntry.entrySet();
		}

		public long getCheckpointID() {
			return checkpointID;
		}

		public void abortSubsumed() {
			dispose();
		}

		public void abortExpired() {
			dispose();
		}

		public boolean isDiscarded() {
			return discarded;
		}

		public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
			if (this.cancellerHandle == null) {
				if (!discarded) {
					this.cancellerHandle = cancellerHandle;
					return true;
				} else {
					return false;
				}
			}
			else {
				throw new IllegalStateException("A canceller handle was already set");
			}
		}

		private void cancelCanceller() {
			try {
				final ScheduledFuture<?> canceller = this.cancellerHandle;
				if (canceller != null) {
					canceller.cancel(false);
				}
			}
			catch (Exception e) {
				// this code should not throw exceptions
				LOG.warn("Error while cancelling checkpoint cache timeout task", e);
			}
		}

		private void dispose() {
			try {
				executor.execute(() -> {
					for (CacheEntry entry : pendingEntry.values()) {
						entry.discard();
					}
					pendingEntry.clear();
				});
			} finally {
				discarded = true;
				cancelCanceller();
			}
		}
	}
}
