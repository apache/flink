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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

/**
 * A size-based cache of accessed checkpoints for completed and failed
 * checkpoints.
 *
 * <p>Having this cache in place for accessed stats improves the user
 * experience quite a bit as accessed checkpoint stats stay available
 * and don't expire. For example if you manage to click on the last
 * checkpoint in the history, it is not available via the stats as soon
 * as another checkpoint is triggered. With the cache in place, the
 * checkpoint will still be available for investigation.
 */
public class CheckpointStatsCache {

	@Nullable
	private final Cache<Long, AbstractCheckpointStats> cache;

	public CheckpointStatsCache(int maxNumEntries) {
		if (maxNumEntries > 0) {
			this.cache = CacheBuilder.<Long, AbstractCheckpointStats>newBuilder()
				.maximumSize(maxNumEntries)
				.build();
		} else {
			this.cache = null;
		}
	}

	/**
	 * Try to add the checkpoint to the cache.
	 *
	 * @param checkpoint Checkpoint to be added.
	 */
	public void tryAdd(AbstractCheckpointStats checkpoint) {
		// Don't add in progress checkpoints as they will be replaced by their
		// completed/failed version eventually.
		if (cache != null && checkpoint != null && !checkpoint.getStatus().isInProgress()) {
			cache.put(checkpoint.getCheckpointId(), checkpoint);
		}
	}

	/**
	 * Try to look up a checkpoint by it's ID in the cache.
	 *
	 * @param checkpointId ID of the checkpoint to look up.
	 * @return The checkpoint or <code>null</code> if checkpoint not found.
	 */
	public AbstractCheckpointStats tryGet(long checkpointId) {
		if (cache != null) {
			return cache.getIfPresent(checkpointId);
		} else {
			return null;
		}
	}

}
