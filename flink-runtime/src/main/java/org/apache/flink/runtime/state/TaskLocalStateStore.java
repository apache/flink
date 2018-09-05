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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.function.LongPredicate;

/**
 * Classes  that implement this interface serve as a task-manager-level local storage for local checkpointed state.
 * The purpose is to provide  access to a state that is stored locally for a faster recovery compared to the state that
 * is stored remotely in a stable store DFS. For now, this storage is only complementary to the stable storage and local
 * state is typically lost in case of machine failures. In such cases (and others), client code of this class must fall
 * back to using the slower but highly available store.
 */
@Internal
public interface TaskLocalStateStore {
	/**
	 * Stores the local state for the given checkpoint id.
	 *
	 * @param checkpointId id for the checkpoint that created the local state that will be stored.
	 * @param localState the local state to store.
	 */
	void storeLocalState(
		@Nonnegative long checkpointId,
		@Nullable TaskStateSnapshot localState);

	/**
	 * Returns the local state that is stored under the given checkpoint id or null if nothing was stored under the id.
	 *
	 * @param checkpointID the checkpoint id by which we search for local state.
	 * @return the local state found for the given checkpoint id. Can be null
	 */
	@Nullable
	TaskStateSnapshot retrieveLocalState(long checkpointID);

	/**
	 * Returns the {@link LocalRecoveryConfig} for this task local state store.
	 */
	@Nonnull
	LocalRecoveryConfig getLocalRecoveryConfig();

	/**
	 * Notifies that the checkpoint with the given id was confirmed as complete. This prunes the checkpoint history
	 * and removes all local states with a checkpoint id that is smaller than the newly confirmed checkpoint id.
	 */
	void confirmCheckpoint(long confirmedCheckpointId);

	/**
	 * Remove all checkpoints from the store that match the given predicate.
	 * @param matcher the predicate that selects the checkpoints for pruning.
	 */
	void pruneMatchingCheckpoints(LongPredicate matcher);
}
