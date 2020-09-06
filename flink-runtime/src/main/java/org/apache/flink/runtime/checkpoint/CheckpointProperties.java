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
import org.apache.flink.api.common.JobStatus;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The configuration of a checkpoint. This describes whether
 * <ul>
 *     <li>The checkpoint is s regular checkpoint or a savepoint.</li>
 *     <li>When the checkpoint should be garbage collected.</li>
 * </ul>
 */
public class CheckpointProperties implements Serializable {

	private static final long serialVersionUID = 2L;

	/** Type - checkpoint / savepoint. */
	private final CheckpointType checkpointType;

	/** This has a misleading name and actually means whether the snapshot must be triggered,
	 * or whether it may be rejected by the checkpoint coordinator if too many checkpoints are
	 * currently in progress. */
	private final boolean forced;

	private final boolean discardSubsumed;
	private final boolean discardFinished;
	private final boolean discardCancelled;
	private final boolean discardFailed;
	private final boolean discardSuspended;

	@VisibleForTesting
	CheckpointProperties(
			boolean forced,
			CheckpointType checkpointType,
			boolean discardSubsumed,
			boolean discardFinished,
			boolean discardCancelled,
			boolean discardFailed,
			boolean discardSuspended) {

		this.forced = forced;
		this.checkpointType = checkNotNull(checkpointType);
		this.discardSubsumed = discardSubsumed;
		this.discardFinished = discardFinished;
		this.discardCancelled = discardCancelled;
		this.discardFailed = discardFailed;
		this.discardSuspended = discardSuspended;
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns whether the checkpoint should be forced.
	 *
	 * <p>Forced checkpoints ignore the configured maximum number of concurrent
	 * checkpoints and minimum time between checkpoints. Furthermore, they are
	 * not subsumed by more recent checkpoints as long as they are pending.
	 *
	 * @return <code>true</code> if the checkpoint should be forced;
	 * <code>false</code> otherwise.
	 *
	 * @see CheckpointCoordinator
	 * @see PendingCheckpoint
	 */
	boolean forceCheckpoint() {
		return forced;
	}

	// ------------------------------------------------------------------------
	// Garbage collection behaviour
	// ------------------------------------------------------------------------

	/**
	 * Returns whether the checkpoint should be discarded when it is subsumed.
	 *
	 * <p>A checkpoint is subsumed when the maximum number of retained
	 * checkpoints is reached and a more recent checkpoint completes..
	 *
	 * @return <code>true</code> if the checkpoint should be discarded when it
	 * is subsumed; <code>false</code> otherwise.
	 *
	 * @see CompletedCheckpointStore
	 */
	boolean discardOnSubsumed() {
		return discardSubsumed;
	}

	/**
	 * Returns whether the checkpoint should be discarded when the owning job
	 * reaches the {@link JobStatus#FINISHED} state.
	 *
	 * @return <code>true</code> if the checkpoint should be discarded when the
	 * owning job reaches the {@link JobStatus#FINISHED} state; <code>false</code>
	 * otherwise.
	 *
	 * @see CompletedCheckpointStore
	 */
	boolean discardOnJobFinished() {
		return discardFinished;
	}

	/**
	 * Returns whether the checkpoint should be discarded when the owning job
	 * reaches the {@link JobStatus#CANCELED} state.
	 *
	 * @return <code>true</code> if the checkpoint should be discarded when the
	 * owning job reaches the {@link JobStatus#CANCELED} state; <code>false</code>
	 * otherwise.
	 *
	 * @see CompletedCheckpointStore
	 */
	boolean discardOnJobCancelled() {
		return discardCancelled;
	}

	/**
	 * Returns whether the checkpoint should be discarded when the owning job
	 * reaches the {@link JobStatus#FAILED} state.
	 *
	 * @return <code>true</code> if the checkpoint should be discarded when the
	 * owning job reaches the {@link JobStatus#FAILED} state; <code>false</code>
	 * otherwise.
	 *
	 * @see CompletedCheckpointStore
	 */
	boolean discardOnJobFailed() {
		return discardFailed;
	}

	/**
	 * Returns whether the checkpoint should be discarded when the owning job
	 * reaches the {@link JobStatus#SUSPENDED} state.
	 *
	 * @return <code>true</code> if the checkpoint should be discarded when the
	 * owning job reaches the {@link JobStatus#SUSPENDED} state; <code>false</code>
	 * otherwise.
	 *
	 * @see CompletedCheckpointStore
	 */
	boolean discardOnJobSuspended() {
		return discardSuspended;
	}

	/**
	 * Gets the type of the checkpoint (checkpoint / savepoint).
	 */
	public CheckpointType getCheckpointType() {
		return checkpointType;
	}

	/**
	 * Returns whether the checkpoint properties describe a standard savepoint.
	 *
	 * @return <code>true</code> if the properties describe a savepoint, <code>false</code> otherwise.
	 */
	public boolean isSavepoint() {
		return checkpointType.isSavepoint();
	}

	/**
	 * Returns whether the checkpoint properties describe a synchronous savepoint/checkpoint.
	 *
	 * @return <code>true</code> if the properties describe a synchronous operation, <code>false</code> otherwise.
	 */
	public boolean isSynchronous() {
		return checkpointType.isSynchronous();
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CheckpointProperties that = (CheckpointProperties) o;
		return forced == that.forced &&
				checkpointType == that.checkpointType &&
				discardSubsumed == that.discardSubsumed &&
				discardFinished == that.discardFinished &&
				discardCancelled == that.discardCancelled &&
				discardFailed == that.discardFailed &&
				discardSuspended == that.discardSuspended;
	}

	@Override
	public int hashCode() {
		int result = (forced ? 1 : 0);
		result = 31 * result + checkpointType.hashCode();
		result = 31 * result + (discardSubsumed ? 1 : 0);
		result = 31 * result + (discardFinished ? 1 : 0);
		result = 31 * result + (discardCancelled ? 1 : 0);
		result = 31 * result + (discardFailed ? 1 : 0);
		result = 31 * result + (discardSuspended ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		return "CheckpointProperties{" +
				"forced=" + forced +
				", checkpointType=" + checkpointType +
				", discardSubsumed=" + discardSubsumed +
				", discardFinished=" + discardFinished +
				", discardCancelled=" + discardCancelled +
				", discardFailed=" + discardFailed +
				", discardSuspended=" + discardSuspended +
				'}';
	}

	// ------------------------------------------------------------------------
	//  Factories and pre-configured properties
	// ------------------------------------------------------------------------

	private static final CheckpointProperties SYNC_SAVEPOINT = new CheckpointProperties(
			true,
			CheckpointType.SYNC_SAVEPOINT,
			false,
			false,
			false,
			false,
			false);

	private static final CheckpointProperties SYNC_SAVEPOINT_NOT_FORCED = new CheckpointProperties(
			false,
			CheckpointType.SYNC_SAVEPOINT,
			false,
			false,
			false,
			false,
			false);

	private static final CheckpointProperties SAVEPOINT = new CheckpointProperties(
			true,
			CheckpointType.SAVEPOINT,
			false,
			false,
			false,
			false,
			false);

	private static final CheckpointProperties SAVEPOINT_NO_FORCE = new CheckpointProperties(
			false,
			CheckpointType.SAVEPOINT,
			false,
			false,
			false,
			false,
			false);

	private static final CheckpointProperties CHECKPOINT_NEVER_RETAINED = new CheckpointProperties(
			false,
			CheckpointType.CHECKPOINT,
			true,
			true,  // Delete on success
			true,  // Delete on cancellation
			true,  // Delete on failure
			true); // Delete on suspension

	private static final CheckpointProperties CHECKPOINT_RETAINED_ON_FAILURE = new CheckpointProperties(
			false,
			CheckpointType.CHECKPOINT,
			true,
			true,  // Delete on success
			true,  // Delete on cancellation
			false, // Retain on failure
			true); // Delete on suspension

	private static final CheckpointProperties CHECKPOINT_RETAINED_ON_CANCELLATION = new CheckpointProperties(
			false,
			CheckpointType.CHECKPOINT,
			true,
			true,   // Delete on success
			false,  // Retain on cancellation
			false,  // Retain on failure
			false); // Retain on suspension


	/**
	 * Creates the checkpoint properties for a (manually triggered) savepoint.
	 *
	 * <p>Savepoints are not queued due to time trigger limits. They have to be
	 * garbage collected manually.
	 *
	 * @return Checkpoint properties for a (manually triggered) savepoint.
	 */
	public static CheckpointProperties forSavepoint(boolean forced) {
		return forced ? SAVEPOINT : SAVEPOINT_NO_FORCE;
	}

	public static CheckpointProperties forSyncSavepoint(boolean forced) {
		return forced ? SYNC_SAVEPOINT : SYNC_SAVEPOINT_NOT_FORCED;
	}

	/**
	 * Creates the checkpoint properties for a checkpoint.
	 *
	 * <p>Checkpoints may be queued in case too many other checkpoints are currently happening.
	 * They are garbage collected automatically, except when the owning job
	 * terminates in state {@link JobStatus#FAILED}. The user is required to
	 * configure the clean up behaviour on job cancellation.
	 *
	 * @return Checkpoint properties for an external checkpoint.
	 */
	public static CheckpointProperties forCheckpoint(CheckpointRetentionPolicy policy) {
		switch (policy) {
			case NEVER_RETAIN_AFTER_TERMINATION:
				return CHECKPOINT_NEVER_RETAINED;
			case RETAIN_ON_FAILURE:
				return CHECKPOINT_RETAINED_ON_FAILURE;
			case RETAIN_ON_CANCELLATION:
				return CHECKPOINT_RETAINED_ON_CANCELLATION;
			default:
				throw new IllegalArgumentException("unknown policy: " + policy);
		}
	}
}
