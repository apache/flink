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
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * It is responsible for managing all {@link CheckpointCache} instances on a TM,
 * whenever the TM receives the task submit message, it registers a CheckpointCache
 * from CheckpointCacheManager for the coming Task. It is initialized in the form of
 * service during TM initialization and runs on TM until TM exits.
 */
public class CheckpointCacheManager {

	private static Logger LOG = LoggerFactory.getLogger(CheckpointCacheManager.class);

	private final Object lock = new Object();
	private final Path basePath;
	private final ScheduledExecutorService scheduledExecutorService;

	private final Map<JobID, CheckpointCache> checkpointCaches;
	private final Map<JobID, ScheduledFuture<?>> cacheClearFutures;

	private final Executor executor;
	private boolean isShutdown;

	public CheckpointCacheManager(ScheduledExecutorService scheduledExecutorService, Executor executor, String basePath) {
		this.scheduledExecutorService = scheduledExecutorService;
		this.executor = executor;
		this.basePath = new Path(basePath);
		this.checkpointCaches = new ConcurrentHashMap<>();
		this.cacheClearFutures = new ConcurrentHashMap<>();
		this.isShutdown = false;
	}

	/** Reregister a {@linke CheckpointCache} from {@link CheckpointCacheManager}, create a new one or refer a exists one. */
	public CheckpointCache registerCheckpointCache(JobID jobID, long pendingCheckpointCacheTimeout, long leaseTimeout) {
		if (pendingCheckpointCacheTimeout == -1) {
			return null;
		}
		synchronized (lock) {
			CheckpointCache checkpointCache = checkpointCaches.get(jobID);
			if (checkpointCache == null) {
				LOG.info("jobID: {} create checkpoint cache", jobID);
				checkpointCache = new CheckpointCache(jobID,
					basePath.getPath(),
					pendingCheckpointCacheTimeout,
					leaseTimeout,
					this,
					executor);
				checkpointCaches.put(jobID, checkpointCache);
			} else {
				ScheduledFuture<?> cacheClearRunner = cacheClearFutures.get(jobID);
				if (cacheClearRunner != null && !cacheClearRunner.isDone()) {
					LOG.info("checkpoint cache {}, reassign a lease", jobID);
					cacheClearRunner.cancel(false);
					cacheClearFutures.remove(jobID);
				}
			}
			int reference = checkpointCache.reference();
			LOG.info("jobID: {} registered, current reference: {}", jobID, reference);
			return checkpointCache;
		}
	}

	/** Unregister {@link CheckpointCache} with jobId, try to release it when it's reference equal zero. */
	public void unregisterCheckpointCache(JobID jobID) {
		synchronized (lock) {
			final CheckpointCache checkpointCache = checkpointCaches.get(jobID);
			int reference = checkpointCache.dereference();
			if (reference <= 0) {
				ScheduledFuture<?> cacheClearRunner = cacheClearFutures.get(jobID);
				if (cacheClearRunner == null || cacheClearRunner.isDone()) {
					cacheClearRunner = scheduledExecutorService.schedule(() -> {
							synchronized (lock) {
								LOG.info("try to remove checkpoint cache for jobID:{}", jobID);
								if (checkpointCache.getReference() <= 0) {
									checkpointCaches.remove(jobID);
									checkpointCache.release();
									cacheClearFutures.remove(jobID);
									LOG.info("remove checkpoint cache for jobID:{}", jobID);
								} else {
									LOG.info("failed to remove checkpoint cache for jobID:{}", jobID);
								}
							}
						}, checkpointCache.getLeaseTimeout(), TimeUnit.SECONDS
					);
					cacheClearFutures.put(jobID, cacheClearRunner);
				}
			}
			LOG.info("jobID: {} unregistered, current reference: {}", jobID, reference);
		}
	}

	/** Shutdown service, called when TM shutdown. */
	public void shutdown() {
		synchronized (lock) {
			for (CheckpointCache checkpointCache : checkpointCaches.values()) {
				checkpointCache.release();
			}
			checkpointCaches.clear();
			isShutdown = true;
		}
	}

	@VisibleForTesting
	protected CheckpointCache getCheckpointCache(JobID jobID) {
		synchronized (lock) {
			return checkpointCaches.get(jobID);
		}
	}

	@VisibleForTesting
	protected int getCheckpointCacheSize() {
		synchronized (lock) {
			return checkpointCaches.size();
		}
	}

	@VisibleForTesting
	protected ScheduledFuture getCacheClearFuture(JobID jobID) {
		return cacheClearFutures.get(jobID);
	}

	public boolean isShutdown() {
		return this.isShutdown;
	}
}
