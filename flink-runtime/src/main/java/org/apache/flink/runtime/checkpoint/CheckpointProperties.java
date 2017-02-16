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

import org.apache.flink.runtime.jobgraph.JobStatus;

import java.io.Serializable;

/**
 * The configuration of a checkpoint, such as whether
 * <ul>
 *     <li>The checkpoint should be persisted</li>
 *     <li>The checkpoint must be full, or may be incremental (not yet implemented)</li>
 *     <li>The checkpoint format must be the common (cross backend) format,
 *     or may be state-backend specific (not yet implemented)</li>
 *     <li>when the checkpoint should be garbage collected</li>
 * </ul>
 */
public class CheckpointProperties implements Serializable {

	private static final long serialVersionUID = -8835900655844879469L;

	private final boolean forced;

	private final boolean externalize;

	private final boolean discardSubsumed;
	private final boolean discardFinished;
	private final boolean discardCancelled;
	private final boolean discardFailed;
	private final boolean discardSuspended;

	CheckpointProperties(
			boolean forced,
			boolean externalize,
			boolean discardSubsumed,
			boolean discardFinished,
			boolean discardCancelled,
			boolean discardFailed,
			boolean discardSuspended) {

		this.forced = forced;
		this.externalize = externalize;
		this.discardSubsumed = discardSubsumed;
		this.discardFinished = discardFinished;
		this.discardCancelled = discardCancelled;
		this.discardFailed = discardFailed;
		this.discardSuspended = discardSuspended;

		// Not persisted, but needs manual clean up
		if (!externalize && !(discardSubsumed && discardFinished && discardCancelled
				&& discardFailed && discardSuspended)) {
			throw new IllegalStateException("CheckpointProperties say to *not* persist the " +
					"checkpoint, but the checkpoint requires manual cleanup.");
		}
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

	/**
	 * Returns whether the checkpoint should be persisted externally.
	 *
	 * @return <code>true</code> if the checkpoint should be persisted
	 * externally; <code>false</code> otherwise.
	 *
	 * @see PendingCheckpoint
	 */
	boolean externalizeCheckpoint() {
		return externalize;
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
	 * Returns whether the checkpoint properties describe a standard savepoint.
	 *
	 * @return <code>true</code> if the properties describe a savepoint, <code>false</code> otherwise.
	 */
	public boolean isSavepoint() {
		return this == STANDARD_SAVEPOINT;
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
				externalize == that.externalize &&
				discardSubsumed == that.discardSubsumed &&
				discardFinished == that.discardFinished &&
				discardCancelled == that.discardCancelled &&
				discardFailed == that.discardFailed &&
				discardSuspended == that.discardSuspended;
	}

	@Override
	public int hashCode() {
		int result = (forced ? 1 : 0);
		result = 31 * result + (externalize ? 1 : 0);
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
				", externalize=" + externalizeCheckpoint() +
				", discardSubsumed=" + discardSubsumed +
				", discardFinished=" + discardFinished +
				", discardCancelled=" + discardCancelled +
				", discardFailed=" + discardFailed +
				", discardSuspended=" + discardSuspended +
				'}';
	}

	// ------------------------------------------------------------------------

	private static final CheckpointProperties STANDARD_SAVEPOINT = new CheckpointProperties(
			true,
			true,
			false,
			false,
			false,
			false,
			false);

	private static final CheckpointProperties STANDARD_CHECKPOINT = new CheckpointProperties(
			false,
			false,
			true,
			true,
			true,
			true,
			true);

	private static final CheckpointProperties EXTERNALIZED_CHECKPOINT_RETAINED = new CheckpointProperties(
			false,
			true,
			true,
			true,
			false, // Retain on cancellation
			false,
			false); // Retain on suspension

	private static final CheckpointProperties EXTERNALIZED_CHECKPOINT_DELETED = new CheckpointProperties(
			false,
			true,
			true,
			true,
			true, // Delete on cancellation
			false,
			true); // Delete on suspension

	/**
	 * Creates the checkpoint properties for a (manually triggered) savepoint.
	 *
	 * <p>Savepoints are forced and persisted externally. They have to be
	 * garbage collected manually.
	 *
	 * @return Checkpoint properties for a (manually triggered) savepoint.
	 */
	public static CheckpointProperties forStandardSavepoint() {
		return STANDARD_SAVEPOINT;
	}

	/**
	 * Creates the checkpoint properties for a regular checkpoint.
	 *
	 * <p>Regular checkpoints are not forced and not persisted externally. They
	 * are garbage collected automatically.
	 *
	 * @return Checkpoint properties for a regular checkpoint.
	 */
	public static CheckpointProperties forStandardCheckpoint() {
		return STANDARD_CHECKPOINT;
	}

	/**
	 * Creates the checkpoint properties for an external checkpoint.
	 *
	 * <p>External checkpoints are not forced, but persisted externally. They
	 * are garbage collected automatically, except when the owning job
	 * terminates in state {@link JobStatus#FAILED}. The user is required to
	 * configure the clean up behaviour on job cancellation.
	 *
	 * @param deleteOnCancellation Flag indicating whether to discard on cancellation.
	 *
	 * @return Checkpoint properties for an external checkpoint.
	 */
	public static CheckpointProperties forExternalizedCheckpoint(boolean deleteOnCancellation) {
		if (deleteOnCancellation) {
			return EXTERNALIZED_CHECKPOINT_DELETED;
		} else {
			return EXTERNALIZED_CHECKPOINT_RETAINED;
		}
	}

}
