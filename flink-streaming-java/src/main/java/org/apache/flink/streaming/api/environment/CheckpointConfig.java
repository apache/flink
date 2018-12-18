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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.streaming.api.CheckpointingMode;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration that captures all checkpointing related settings.
 */
@Public
public class CheckpointConfig implements java.io.Serializable {

	private static final long serialVersionUID = -750378776078908147L;

	/** The default checkpoint mode: exactly once. */
	public static final CheckpointingMode DEFAULT_MODE = CheckpointingMode.EXACTLY_ONCE;

	/** The default timeout of a checkpoint attempt: 10 minutes. */
	public static final long DEFAULT_TIMEOUT = 10 * 60 * 1000;

	/** The default minimum pause to be made between checkpoints: none. */
	public static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 0;

	/** The default limit of concurrently happening checkpoints: one. */
	public static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;

	// ------------------------------------------------------------------------

	/** Checkpointing mode (exactly-once vs. at-least-once). */
	private CheckpointingMode checkpointingMode = DEFAULT_MODE;

	/** Periodic checkpoint triggering interval. */
	private long checkpointInterval = -1; // disabled

	/** Maximum time checkpoint may take before being discarded. */
	private long checkpointTimeout = DEFAULT_TIMEOUT;

	/** Minimal pause between checkpointing attempts. */
	private long minPauseBetweenCheckpoints = DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS;

	/** Maximum number of checkpoint attempts in progress at the same time. */
	private int maxConcurrentCheckpoints = DEFAULT_MAX_CONCURRENT_CHECKPOINTS;

	/** Flag to force checkpointing in iterative jobs. */
	private boolean forceCheckpointing;

	/** Cleanup behaviour for persistent checkpoints. */
	private ExternalizedCheckpointCleanup externalizedCheckpointCleanup;

	/** Determines if a tasks are failed or not if there is an error in their checkpointing. Default: true */
	private boolean failOnCheckpointingErrors = true;

	// ------------------------------------------------------------------------

	/**
	 * Checks whether checkpointing is enabled.
	 *
	 * @return True if checkpointing is enables, false otherwise.
	 */
	public boolean isCheckpointingEnabled() {
		return checkpointInterval > 0;
	}

	/**
	 * Gets the checkpointing mode (exactly-once vs. at-least-once).
	 *
	 * @return The checkpointing mode.
	 */
	public CheckpointingMode getCheckpointingMode() {
		return checkpointingMode;
	}

	/**
	 * Sets the checkpointing mode (exactly-once vs. at-least-once).
	 *
	 * @param checkpointingMode The checkpointing mode.
	 */
	public void setCheckpointingMode(CheckpointingMode checkpointingMode) {
		this.checkpointingMode = requireNonNull(checkpointingMode);
	}

	/**
	 * Gets the interval in which checkpoints are periodically scheduled.
	 *
	 * <p>This setting defines the base interval. Checkpoint triggering may be delayed by the settings
	 * {@link #getMaxConcurrentCheckpoints()} and {@link #getMinPauseBetweenCheckpoints()}.
	 *
	 * @return The checkpoint interval, in milliseconds.
	 */
	public long getCheckpointInterval() {
		return checkpointInterval;
	}

	/**
	 * Sets the interval in which checkpoints are periodically scheduled.
	 *
	 * <p>This setting defines the base interval. Checkpoint triggering may be delayed by the settings
	 * {@link #setMaxConcurrentCheckpoints(int)} and {@link #setMinPauseBetweenCheckpoints(long)}.
	 *
	 * @param checkpointInterval The checkpoint interval, in milliseconds.
	 */
	public void setCheckpointInterval(long checkpointInterval) {
		if (checkpointInterval <= 0) {
			throw new IllegalArgumentException("Checkpoint interval must be larger than zero");
		}
		this.checkpointInterval = checkpointInterval;
	}

	/**
	 * Gets the maximum time that a checkpoint may take before being discarded.
	 *
	 * @return The checkpoint timeout, in milliseconds.
	 */
	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	/**
	 * Sets the maximum time that a checkpoint may take before being discarded.
	 *
	 * @param checkpointTimeout The checkpoint timeout, in milliseconds.
	 */
	public void setCheckpointTimeout(long checkpointTimeout) {
		if (checkpointTimeout <= 0) {
			throw new IllegalArgumentException("Checkpoint timeout must be larger than zero");
		}
		this.checkpointTimeout = checkpointTimeout;
	}

	/**
	 * Gets the minimal pause between checkpointing attempts. This setting defines how soon the
	 * checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger
	 * another checkpoint with respect to the maximum number of concurrent checkpoints
	 * (see {@link #getMaxConcurrentCheckpoints()}).
	 *
	 * @return The minimal pause before the next checkpoint is triggered.
	 */
	public long getMinPauseBetweenCheckpoints() {
		return minPauseBetweenCheckpoints;
	}

	/**
	 * Sets the minimal pause between checkpointing attempts. This setting defines how soon the
	 * checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger
	 * another checkpoint with respect to the maximum number of concurrent checkpoints
	 * (see {@link #setMaxConcurrentCheckpoints(int)}).
	 *
	 * <p>If the maximum number of concurrent checkpoints is set to one, this setting makes effectively sure
	 * that a minimum amount of time passes where no checkpoint is in progress at all.
	 *
	 * @param minPauseBetweenCheckpoints The minimal pause before the next checkpoint is triggered.
	 */
	public void setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
		if (minPauseBetweenCheckpoints < 0) {
			throw new IllegalArgumentException("Pause value must be zero or positive");
		}
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
	}

	/**
	 * Gets the maximum number of checkpoint attempts that may be in progress at the same time. If this
	 * value is <i>n</i>, then no checkpoints will be triggered while <i>n</i> checkpoint attempts are
	 * currently in flight. For the next checkpoint to be triggered, one checkpoint attempt would need
	 * to finish or expire.
	 *
	 * @return The maximum number of concurrent checkpoint attempts.
	 */
	public int getMaxConcurrentCheckpoints() {
		return maxConcurrentCheckpoints;
	}

	/**
	 * Sets the maximum number of checkpoint attempts that may be in progress at the same time. If this
	 * value is <i>n</i>, then no checkpoints will be triggered while <i>n</i> checkpoint attempts are
	 * currently in flight. For the next checkpoint to be triggered, one checkpoint attempt would need
	 * to finish or expire.
	 *
	 * @param maxConcurrentCheckpoints The maximum number of concurrent checkpoint attempts.
	 */
	public void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
		if (maxConcurrentCheckpoints < 1) {
			throw new IllegalArgumentException("The maximum number of concurrent attempts must be at least one.");
		}
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
	}

	/**
	 * Checks whether checkpointing is forced, despite currently non-checkpointable iteration feedback.
	 *
	 * @return True, if checkpointing is forced, false otherwise.
	 *
	 * @deprecated This will be removed once iterations properly participate in checkpointing.
	 */
	@Deprecated
	@PublicEvolving
	public boolean isForceCheckpointing() {
		return forceCheckpointing;
	}

	/**
	 * Checks whether checkpointing is forced, despite currently non-checkpointable iteration feedback.
	 *
	 * @param forceCheckpointing The flag to force checkpointing.
	 *
	 * @deprecated This will be removed once iterations properly participate in checkpointing.
	 */
	@Deprecated
	@PublicEvolving
	public void setForceCheckpointing(boolean forceCheckpointing) {
		this.forceCheckpointing = forceCheckpointing;
	}

	/**
	 * This determines the behaviour of tasks if there is an error in their local checkpointing. If this returns true,
	 * tasks will fail as a reaction. If this returns false, task will only decline the failed checkpoint.
	 */
	public boolean isFailOnCheckpointingErrors() {
		return failOnCheckpointingErrors;
	}

	/**
	 * Sets the expected behaviour for tasks in case that they encounter an error in their checkpointing procedure.
	 * If this is set to true, the task will fail on checkpointing error. If this is set to false, the task will only
	 * decline a the checkpoint and continue running. The default is true.
	 */
	public void setFailOnCheckpointingErrors(boolean failOnCheckpointingErrors) {
		this.failOnCheckpointingErrors = failOnCheckpointingErrors;
	}

	/**
	 * Enables checkpoints to be persisted externally.
	 *
	 * <p>Externalized checkpoints write their meta data out to persistent
	 * storage and are <strong>not</strong> automatically cleaned up when
	 * the owning job fails or is suspended (terminating with job status
	 * {@link JobStatus#FAILED} or {@link JobStatus#SUSPENDED}). In this
	 * case, you have to manually clean up the checkpoint state, both
	 * the meta data and actual program state.
	 *
	 * <p>The {@link ExternalizedCheckpointCleanup} mode defines how an
	 * externalized checkpoint should be cleaned up on job cancellation. If you
	 * choose to retain externalized checkpoints on cancellation you have you
	 * handle checkpoint clean up manually when you cancel the job as well
	 * (terminating with job status {@link JobStatus#CANCELED}).
	 *
	 * <p>The target directory for externalized checkpoints is configured
	 * via {@link org.apache.flink.configuration.CheckpointingOptions#CHECKPOINTS_DIRECTORY}.
	 *
	 * @param cleanupMode Externalized checkpoint cleanup behaviour.
	 */
	@PublicEvolving
	public void enableExternalizedCheckpoints(ExternalizedCheckpointCleanup cleanupMode) {
		this.externalizedCheckpointCleanup = checkNotNull(cleanupMode);
	}

	/**
	 * Returns whether checkpoints should be persisted externally.
	 *
	 * @return <code>true</code> if checkpoints should be externalized.
	 */
	@PublicEvolving
	public boolean isExternalizedCheckpointsEnabled() {
		return externalizedCheckpointCleanup != null;
	}

	/**
	 * Returns the cleanup behaviour for externalized checkpoints.
	 *
	 * @return The cleanup behaviour for externalized checkpoints or
	 * <code>null</code> if none is configured.
	 */
	@PublicEvolving
	public ExternalizedCheckpointCleanup getExternalizedCheckpointCleanup() {
		return externalizedCheckpointCleanup;
	}

	/**
	 * Cleanup behaviour for externalized checkpoints when the job is cancelled.
	 */
	@PublicEvolving
	public enum ExternalizedCheckpointCleanup {

		/**
		 * Delete externalized checkpoints on job cancellation.
		 *
		 * <p>All checkpoint state will be deleted when you cancel the owning
		 * job, both the meta data and actual program state. Therefore, you
		 * cannot resume from externalized checkpoints after the job has been
		 * cancelled.
		 *
		 * <p>Note that checkpoint state is always kept if the job terminates
		 * with state {@link JobStatus#FAILED}.
		 */
		DELETE_ON_CANCELLATION(true),

		/**
		 * Retain externalized checkpoints on job cancellation.
		 *
		 * <p>All checkpoint state is kept when you cancel the owning job. You
		 * have to manually delete both the checkpoint meta data and actual
		 * program state after cancelling the job.
		 *
		 * <p>Note that checkpoint state is always kept if the job terminates
		 * with state {@link JobStatus#FAILED}.
		 */
		RETAIN_ON_CANCELLATION(false);

		private final boolean deleteOnCancellation;

		ExternalizedCheckpointCleanup(boolean deleteOnCancellation) {
			this.deleteOnCancellation = deleteOnCancellation;
		}

		/**
		 * Returns whether persistent checkpoints shall be discarded on
		 * cancellation of the job.
		 *
		 * @return <code>true</code> if persistent checkpoints shall be discarded
		 * on cancellation of the job.
		 */
		public boolean deleteOnCancellation() {
			return deleteOnCancellation;
		}
	}
}
