/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongPredicate;

/**
 * This class implements a {@link TaskLocalStateStore} with no functionality and is used when local recovery is
 * disabled.
 */
public final class NoOpTaskLocalStateStoreImpl implements OwnedTaskLocalStateStore {

	/** The configuration for local recovery. */
	@Nonnull
	private final LocalRecoveryConfig localRecoveryConfig;

	NoOpTaskLocalStateStoreImpl(@Nonnull LocalRecoveryConfig localRecoveryConfig) {
		this.localRecoveryConfig = localRecoveryConfig;
	}

	@Nonnull
	@Override
	public LocalRecoveryConfig getLocalRecoveryConfig() {
		return localRecoveryConfig;
	}

	@Override
	public CompletableFuture<Void> dispose() {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void storeLocalState(long checkpointId, @Nullable TaskStateSnapshot localState) {
	}

	@Nullable
	@Override
	public TaskStateSnapshot retrieveLocalState(long checkpointID) {
		return null;
	}

	@Override
	public void confirmCheckpoint(long confirmedCheckpointId) {
	}

	@Override
	public void abortCheckpoint(long abortedCheckpointId) {
	}

	@Override
	public void pruneMatchingCheckpoints(LongPredicate matcher) {
	}
}
