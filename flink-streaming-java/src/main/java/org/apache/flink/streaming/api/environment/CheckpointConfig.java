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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureManager.UNLIMITED_TOLERABLE_FAILURE_NUMBER;
import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Configuration that captures all checkpointing related settings. */
@Public
public class CheckpointConfig implements java.io.Serializable {

    private static final long serialVersionUID = -750378776078908147L;

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointConfig.class);

    /** The default checkpoint mode: exactly once. */
    public static final CheckpointingMode DEFAULT_MODE = CheckpointingMode.EXACTLY_ONCE;

    /** The default timeout of a checkpoint attempt: 10 minutes. */
    public static final long DEFAULT_TIMEOUT = 10 * 60 * 1000;

    /** The default minimum pause to be made between checkpoints: none. */
    public static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 0;

    /** The default limit of concurrently happening checkpoints: one. */
    public static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;

    public static final int UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER = -1;

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

    /** Flag to force checkpointing in iterative jobs. */
    private boolean forceUnalignedCheckpoints;

    /** Flag to enable unaligned checkpoints. */
    private boolean unalignedCheckpointsEnabled;

    private long alignmentTimeout =
            ExecutionCheckpointingOptions.ALIGNMENT_TIMEOUT.defaultValue().toMillis();

    /** Flag to enable approximate local recovery. */
    private boolean approximateLocalRecovery;

    /** Cleanup behaviour for persistent checkpoints. */
    private ExternalizedCheckpointCleanup externalizedCheckpointCleanup;

    /**
     * Task would not fail if there is an error in their checkpointing.
     *
     * <p>{@link #tolerableCheckpointFailureNumber} would always overrule this deprecated field if
     * they have conflicts.
     *
     * @deprecated Use {@link #tolerableCheckpointFailureNumber}.
     */
    @Deprecated private boolean failOnCheckpointingErrors = true;

    /** Determines if a job will fallback to checkpoint when there is a more recent savepoint. * */
    private boolean preferCheckpointForRecovery = false;

    /**
     * Determines the threshold that we tolerance declined checkpoint failure number. The default
     * value is -1 meaning undetermined and not set via {@link
     * #setTolerableCheckpointFailureNumber(int)}.
     */
    private int tolerableCheckpointFailureNumber = UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER;

    /**
     * The checkpoint storage for this application. This field is marked as transient because it may
     * contain user-code.
     */
    private transient CheckpointStorage storage;

    /**
     * Creates a deep copy of the provided {@link CheckpointConfig}.
     *
     * @param checkpointConfig the config to copy.
     */
    public CheckpointConfig(final CheckpointConfig checkpointConfig) {
        checkNotNull(checkpointConfig);

        this.checkpointInterval = checkpointConfig.checkpointInterval;
        this.checkpointingMode = checkpointConfig.checkpointingMode;
        this.checkpointTimeout = checkpointConfig.checkpointTimeout;
        this.maxConcurrentCheckpoints = checkpointConfig.maxConcurrentCheckpoints;
        this.minPauseBetweenCheckpoints = checkpointConfig.minPauseBetweenCheckpoints;
        this.preferCheckpointForRecovery = checkpointConfig.preferCheckpointForRecovery;
        this.tolerableCheckpointFailureNumber = checkpointConfig.tolerableCheckpointFailureNumber;
        this.unalignedCheckpointsEnabled = checkpointConfig.isUnalignedCheckpointsEnabled();
        this.alignmentTimeout = checkpointConfig.alignmentTimeout;
        this.approximateLocalRecovery = checkpointConfig.isApproximateLocalRecoveryEnabled();
        this.externalizedCheckpointCleanup = checkpointConfig.externalizedCheckpointCleanup;
        this.forceCheckpointing = checkpointConfig.forceCheckpointing;
        this.forceUnalignedCheckpoints = checkpointConfig.forceUnalignedCheckpoints;
        this.storage = checkpointConfig.getCheckpointStorage();
    }

    public CheckpointConfig() {}

    // ------------------------------------------------------------------------

    /** Disables checkpointing. */
    public void disableCheckpointing() {
        this.checkpointInterval = -1;
    }

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
     * <p>This setting defines the base interval. Checkpoint triggering may be delayed by the
     * settings {@link #getMaxConcurrentCheckpoints()} and {@link #getMinPauseBetweenCheckpoints()}.
     *
     * @return The checkpoint interval, in milliseconds.
     */
    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    /**
     * Sets the interval in which checkpoints are periodically scheduled.
     *
     * <p>This setting defines the base interval. Checkpoint triggering may be delayed by the
     * settings {@link #setMaxConcurrentCheckpoints(int)} and {@link
     * #setMinPauseBetweenCheckpoints(long)}.
     *
     * @param checkpointInterval The checkpoint interval, in milliseconds.
     */
    public void setCheckpointInterval(long checkpointInterval) {
        if (checkpointInterval < MINIMAL_CHECKPOINT_TIME) {
            throw new IllegalArgumentException(
                    String.format(
                            "Checkpoint interval must be larger than or equal to %s ms",
                            MINIMAL_CHECKPOINT_TIME));
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
        if (checkpointTimeout < MINIMAL_CHECKPOINT_TIME) {
            throw new IllegalArgumentException(
                    String.format(
                            "Checkpoint timeout must be larger than or equal to %s ms",
                            MINIMAL_CHECKPOINT_TIME));
        }
        this.checkpointTimeout = checkpointTimeout;
    }

    /**
     * Gets the minimal pause between checkpointing attempts. This setting defines how soon the
     * checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger
     * another checkpoint with respect to the maximum number of concurrent checkpoints (see {@link
     * #getMaxConcurrentCheckpoints()}).
     *
     * @return The minimal pause before the next checkpoint is triggered.
     */
    public long getMinPauseBetweenCheckpoints() {
        return minPauseBetweenCheckpoints;
    }

    /**
     * Sets the minimal pause between checkpointing attempts. This setting defines how soon the
     * checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger
     * another checkpoint with respect to the maximum number of concurrent checkpoints (see {@link
     * #setMaxConcurrentCheckpoints(int)}).
     *
     * <p>If the maximum number of concurrent checkpoints is set to one, this setting makes
     * effectively sure that a minimum amount of time passes where no checkpoint is in progress at
     * all.
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
     * Gets the maximum number of checkpoint attempts that may be in progress at the same time. If
     * this value is <i>n</i>, then no checkpoints will be triggered while <i>n</i> checkpoint
     * attempts are currently in flight. For the next checkpoint to be triggered, one checkpoint
     * attempt would need to finish or expire.
     *
     * @return The maximum number of concurrent checkpoint attempts.
     */
    public int getMaxConcurrentCheckpoints() {
        return maxConcurrentCheckpoints;
    }

    /**
     * Sets the maximum number of checkpoint attempts that may be in progress at the same time. If
     * this value is <i>n</i>, then no checkpoints will be triggered while <i>n</i> checkpoint
     * attempts are currently in flight. For the next checkpoint to be triggered, one checkpoint
     * attempt would need to finish or expire.
     *
     * @param maxConcurrentCheckpoints The maximum number of concurrent checkpoint attempts.
     */
    public void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
        if (maxConcurrentCheckpoints < 1) {
            throw new IllegalArgumentException(
                    "The maximum number of concurrent attempts must be at least one.");
        }
        this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
    }

    /**
     * Checks whether checkpointing is forced, despite currently non-checkpointable iteration
     * feedback.
     *
     * @return True, if checkpointing is forced, false otherwise.
     * @deprecated This will be removed once iterations properly participate in checkpointing.
     */
    @Deprecated
    @PublicEvolving
    public boolean isForceCheckpointing() {
        return forceCheckpointing;
    }

    /**
     * Checks whether checkpointing is forced, despite currently non-checkpointable iteration
     * feedback.
     *
     * @param forceCheckpointing The flag to force checkpointing.
     * @deprecated This will be removed once iterations properly participate in checkpointing.
     */
    @Deprecated
    @PublicEvolving
    public void setForceCheckpointing(boolean forceCheckpointing) {
        this.forceCheckpointing = forceCheckpointing;
    }

    /**
     * Checks whether Unaligned Checkpoints are forced, despite iteration feedback.
     *
     * @return True, if Unaligned Checkpoints are forced, false otherwise.
     */
    @PublicEvolving
    public boolean isForceUnalignedCheckpoints() {
        return forceUnalignedCheckpoints;
    }

    /**
     * Checks whether Unaligned Checkpoints are forced, despite currently non-checkpointable
     * iteration feedback.
     *
     * @param forceUnalignedCheckpoints The flag to force checkpointing.
     */
    @PublicEvolving
    public void setForceUnalignedCheckpoints(boolean forceUnalignedCheckpoints) {
        this.forceUnalignedCheckpoints = forceUnalignedCheckpoints;
    }

    /**
     * This determines the behaviour when meeting checkpoint errors. If this returns true, which is
     * equivalent to get tolerableCheckpointFailureNumber as zero, job manager would fail the whole
     * job once it received a decline checkpoint message. If this returns false, which is equivalent
     * to get tolerableCheckpointFailureNumber as the maximum of integer (means unlimited), job
     * manager would not fail the whole job no matter how many declined checkpoints it received.
     *
     * @deprecated Use {@link #getTolerableCheckpointFailureNumber()}.
     */
    @Deprecated
    public boolean isFailOnCheckpointingErrors() {
        return failOnCheckpointingErrors;
    }

    /**
     * Sets the expected behaviour for tasks in case that they encounter an error when
     * checkpointing. If this is set as true, which is equivalent to set
     * tolerableCheckpointFailureNumber as zero, job manager would fail the whole job once it
     * received a decline checkpoint message. If this is set as false, which is equivalent to set
     * tolerableCheckpointFailureNumber as the maximum of integer (means unlimited), job manager
     * would not fail the whole job no matter how many declined checkpoints it received.
     *
     * <p>{@link #setTolerableCheckpointFailureNumber(int)} would always overrule this deprecated
     * method if they have conflicts.
     *
     * @deprecated Use {@link #setTolerableCheckpointFailureNumber(int)}.
     */
    @Deprecated
    public void setFailOnCheckpointingErrors(boolean failOnCheckpointingErrors) {
        if (tolerableCheckpointFailureNumber != UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER) {
            LOG.warn(
                    "Since tolerableCheckpointFailureNumber has been configured as {}, deprecated #setFailOnCheckpointingErrors(boolean) "
                            + "method would not take any effect and please use #setTolerableCheckpointFailureNumber(int) method to "
                            + "determine your expected behaviour when checkpoint errors on task side.",
                    tolerableCheckpointFailureNumber);
            return;
        }
        this.failOnCheckpointingErrors = failOnCheckpointingErrors;
        if (failOnCheckpointingErrors) {
            this.tolerableCheckpointFailureNumber = 0;
        } else {
            this.tolerableCheckpointFailureNumber = UNLIMITED_TOLERABLE_FAILURE_NUMBER;
        }
    }

    /**
     * Get the tolerable checkpoint failure number which used by the checkpoint failure manager to
     * determine when we need to fail the job.
     *
     * <p>If the {@link #tolerableCheckpointFailureNumber} has not been configured, this method
     * would return 0 which means the checkpoint failure manager would not tolerate any declined
     * checkpoint failure.
     */
    public int getTolerableCheckpointFailureNumber() {
        if (tolerableCheckpointFailureNumber == UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER) {
            return 0;
        }
        return tolerableCheckpointFailureNumber;
    }

    /**
     * Set the tolerable checkpoint failure number, the default value is 0 that means we do not
     * tolerance any checkpoint failure.
     */
    public void setTolerableCheckpointFailureNumber(int tolerableCheckpointFailureNumber) {
        if (tolerableCheckpointFailureNumber < 0) {
            throw new IllegalArgumentException(
                    "The tolerable failure checkpoint number must be non-negative.");
        }
        this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
    }

    /**
     * Enables checkpoints to be persisted externally.
     *
     * <p>Externalized checkpoints write their meta data out to persistent storage and are
     * <strong>not</strong> automatically cleaned up when the owning job fails or is suspended
     * (terminating with job status {@link JobStatus#FAILED} or {@link JobStatus#SUSPENDED}). In
     * this case, you have to manually clean up the checkpoint state, both the meta data and actual
     * program state.
     *
     * <p>The {@link ExternalizedCheckpointCleanup} mode defines how an externalized checkpoint
     * should be cleaned up on job cancellation. If you choose to retain externalized checkpoints on
     * cancellation you have you handle checkpoint clean up manually when you cancel the job as well
     * (terminating with job status {@link JobStatus#CANCELED}).
     *
     * <p>The target directory for externalized checkpoints is configured via {@link
     * org.apache.flink.configuration.CheckpointingOptions#CHECKPOINTS_DIRECTORY}.
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
     * Returns whether a job recovery should fallback to checkpoint when there is a more recent
     * savepoint.
     *
     * @return <code>true</code> if a job recovery should fallback to checkpoint.
     * @deprecated Don't activate prefer checkpoints for recovery because it can lead to data loss
     *     and duplicate output. This option will soon be removed. See <a
     *     href="https://issues.apache.org/jira/browse/FLINK-20427">FLINK-20427</a> for more
     *     information.
     */
    @PublicEvolving
    @Deprecated
    public boolean isPreferCheckpointForRecovery() {
        return preferCheckpointForRecovery;
    }

    /**
     * Sets whether a job recovery should fallback to checkpoint when there is a more recent
     * savepoint.
     *
     * @deprecated Don't activate prefer checkpoints for recovery because it can lead to data loss
     *     and duplicate output. This option will soon be removed. See <a
     *     href="https://issues.apache.org/jira/browse/FLINK-20427">FLINK-20427</a> for more
     *     information.
     */
    @PublicEvolving
    @Deprecated
    public void setPreferCheckpointForRecovery(boolean preferCheckpointForRecovery) {
        this.preferCheckpointForRecovery = preferCheckpointForRecovery;
    }

    /**
     * Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.
     *
     * <p>Unaligned checkpoints contain data stored in buffers as part of the checkpoint state,
     * which allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration
     * becomes independent of the current throughput as checkpoint barriers are effectively not
     * embedded into the stream of data anymore.
     *
     * <p>Unaligned checkpoints can only be enabled if {@link #checkpointingMode} is {@link
     * CheckpointingMode#EXACTLY_ONCE}.
     *
     * @param enabled Flag to indicate whether unaligned are enabled.
     */
    @PublicEvolving
    public void enableUnalignedCheckpoints(boolean enabled) {
        unalignedCheckpointsEnabled = enabled;
    }

    /**
     * Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.
     *
     * <p>Unaligned checkpoints contain data stored in buffers as part of the checkpoint state,
     * which allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration
     * becomes independent of the current throughput as checkpoint barriers are effectively not
     * embedded into the stream of data anymore.
     *
     * <p>Unaligned checkpoints can only be enabled if {@link #checkpointingMode} is {@link
     * CheckpointingMode#EXACTLY_ONCE}.
     */
    @PublicEvolving
    public void enableUnalignedCheckpoints() {
        enableUnalignedCheckpoints(true);
    }

    /**
     * Returns whether unaligned checkpoints are enabled.
     *
     * @return <code>true</code> if unaligned checkpoints are enabled.
     */
    @PublicEvolving
    public boolean isUnalignedCheckpointsEnabled() {
        return unalignedCheckpointsEnabled;
    }

    /**
     * Only relevant if {@link #unalignedCheckpointsEnabled} is enabled.
     *
     * <p>If {@link #alignmentTimeout} has value equal to <code>0</code>, checkpoints will always
     * start unaligned.
     *
     * <p>If {@link #alignmentTimeout} has value greater then <code>0</code>, checkpoints will start
     * aligned. If during checkpointing, checkpoint start delay exceeds this {@link
     * #alignmentTimeout}, alignment will timeout and checkpoint will start working as unaligned
     * checkpoint.
     */
    @PublicEvolving
    public void setAlignmentTimeout(long alignmentTimeout) {
        this.alignmentTimeout = alignmentTimeout;
    }

    /**
     * @return value of alignment timeout, as configured via {@link #setAlignmentTimeout(long)} or
     *     {@link ExecutionCheckpointingOptions#ALIGNMENT_TIMEOUT}.
     */
    @PublicEvolving
    public long getAlignmentTimeout() {
        return alignmentTimeout;
    }

    /**
     * Returns whether approximate local recovery is enabled.
     *
     * @return <code>true</code> if approximate local recovery is enabled.
     */
    @Experimental
    public boolean isApproximateLocalRecoveryEnabled() {
        return approximateLocalRecovery;
    }

    /**
     * Enables the approximate local recovery mode.
     *
     * <p>In this recovery mode, when a task fails, the entire downstream of the tasks (including
     * the failed task) restart.
     *
     * <p>Notice that 1. Approximate recovery may lead to data loss. The amount of data which leads
     * the failed task from the state of the last completed checkpoint to the state when the task
     * fails is lost. 2. In the next version, we will support restarting the set of failed set of
     * tasks only. In this version, we only support downstream restarts when a task fails. 3. It is
     * only an internal feature for now.
     *
     * @param enabled Flag to indicate whether approximate local recovery is enabled .
     */
    @Experimental
    public void enableApproximateLocalRecovery(boolean enabled) {
        approximateLocalRecovery = enabled;
    }

    /**
     * Returns the cleanup behaviour for externalized checkpoints.
     *
     * @return The cleanup behaviour for externalized checkpoints or <code>null</code> if none is
     *     configured.
     */
    @PublicEvolving
    public ExternalizedCheckpointCleanup getExternalizedCheckpointCleanup() {
        return externalizedCheckpointCleanup;
    }

    /**
     * CheckpointStorage defines how {@link StateBackend}'s checkpoint their state for fault
     * tolerance in streaming applications. Various implementations store their checkpoints in
     * different fashions and have different requirements and availability guarantees.
     *
     * <p>For example, {@link org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage
     * JobManagerCheckpointStorage} stores checkpoints in the memory of the JobManager. It is
     * lightweight and without additional dependencies but is not highly available and only supports
     * small state sizes. This checkpoint storage policy is convenient for local testing and
     * development.
     *
     * <p>{@link org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
     * FileSystemCheckpointStorage} stores checkpoints in a filesystem. For systems like HDFS, NFS
     * Drives, S3, and GCS, this storage policy supports large state size, in the magnitude of many
     * terabytes while providing a highly available foundation for stateful applications. This
     * checkpoint storage policy is recommended for most production deployments.
     *
     * @param storage The checkpoint storage policy.
     */
    @PublicEvolving
    public void setCheckpointStorage(CheckpointStorage storage) {
        Preconditions.checkNotNull(storage, "Checkpoint storage must not be null");
        this.storage = storage;
    }

    /**
     * Configures the application to write out checkpoint snapshots to the configured directory. See
     * {@link FileSystemCheckpointStorage} for more details on checkpointing to a file system.
     *
     * @param checkpointDirectory The path to write checkpoint metadata to.
     * @see #setCheckpointStorage(CheckpointStorage)
     */
    @PublicEvolving
    public void setCheckpointStorage(String checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        this.storage = new FileSystemCheckpointStorage(checkpointDirectory);
    }

    /**
     * Configures the application to write out checkpoint snapshots to the configured directory. See
     * {@link FileSystemCheckpointStorage} for more details on checkpointing to a file system.
     *
     * @param checkpointDirectory The path to write checkpoint metadata to.
     * @see #setCheckpointStorage(CheckpointStorage)
     */
    @PublicEvolving
    public void setCheckpointStorage(URI checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        this.storage = new FileSystemCheckpointStorage(checkpointDirectory);
    }

    /**
     * Configures the application to write out checkpoint snapshots to the configured directory. See
     * {@link FileSystemCheckpointStorage} for more details on checkpointing to a file system.
     *
     * @param checkpointDirectory The path to write checkpoint metadata to.
     * @see #setCheckpointStorage(String)
     */
    @PublicEvolving
    public void setCheckpointStorage(Path checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        this.storage = new FileSystemCheckpointStorage(checkpointDirectory);
    }

    /**
     * @return The {@link CheckpointStorage} that has been configured for the job. Or {@code null}
     *     if none has been set.
     * @see #setCheckpointStorage(CheckpointStorage)
     */
    @Nullable
    @PublicEvolving
    public CheckpointStorage getCheckpointStorage() {
        return this.storage;
    }

    /** Cleanup behaviour for externalized checkpoints when the job is cancelled. */
    @PublicEvolving
    public enum ExternalizedCheckpointCleanup {

        /**
         * Delete externalized checkpoints on job cancellation.
         *
         * <p>All checkpoint state will be deleted when you cancel the owning job, both the meta
         * data and actual program state. Therefore, you cannot resume from externalized checkpoints
         * after the job has been cancelled.
         *
         * <p>Note that checkpoint state is always kept if the job terminates with state {@link
         * JobStatus#FAILED}.
         */
        DELETE_ON_CANCELLATION(true),

        /**
         * Retain externalized checkpoints on job cancellation.
         *
         * <p>All checkpoint state is kept when you cancel the owning job. You have to manually
         * delete both the checkpoint meta data and actual program state after cancelling the job.
         *
         * <p>Note that checkpoint state is always kept if the job terminates with state {@link
         * JobStatus#FAILED}.
         */
        RETAIN_ON_CANCELLATION(false);

        private final boolean deleteOnCancellation;

        ExternalizedCheckpointCleanup(boolean deleteOnCancellation) {
            this.deleteOnCancellation = deleteOnCancellation;
        }

        /**
         * Returns whether persistent checkpoints shall be discarded on cancellation of the job.
         *
         * @return <code>true</code> if persistent checkpoints shall be discarded on cancellation of
         *     the job.
         */
        public boolean deleteOnCancellation() {
            return deleteOnCancellation;
        }
    }

    /**
     * Sets all relevant options contained in the {@link ReadableConfig} such as e.g. {@link
     * ExecutionCheckpointingOptions#CHECKPOINTING_MODE}.
     *
     * <p>It will change the value of a setting only if a corresponding option was set in the {@code
     * configuration}. If a key is not present, the current value of a field will remain untouched.
     *
     * @param configuration a configuration to read the values from
     */
    public void configure(ReadableConfig configuration) {
        configuration
                .getOptional(ExecutionCheckpointingOptions.CHECKPOINTING_MODE)
                .ifPresent(this::setCheckpointingMode);
        configuration
                .getOptional(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL)
                .ifPresent(i -> this.setCheckpointInterval(i.toMillis()));
        configuration
                .getOptional(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT)
                .ifPresent(t -> this.setCheckpointTimeout(t.toMillis()));
        configuration
                .getOptional(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS)
                .ifPresent(this::setMaxConcurrentCheckpoints);
        configuration
                .getOptional(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS)
                .ifPresent(m -> this.setMinPauseBetweenCheckpoints(m.toMillis()));
        configuration
                .getOptional(ExecutionCheckpointingOptions.PREFER_CHECKPOINT_FOR_RECOVERY)
                .ifPresent(this::setPreferCheckpointForRecovery);
        configuration
                .getOptional(ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER)
                .ifPresent(this::setTolerableCheckpointFailureNumber);
        configuration
                .getOptional(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT)
                .ifPresent(this::enableExternalizedCheckpoints);
        configuration
                .getOptional(ExecutionCheckpointingOptions.ENABLE_UNALIGNED)
                .ifPresent(this::enableUnalignedCheckpoints);
        configuration
                .getOptional(ExecutionCheckpointingOptions.ALIGNMENT_TIMEOUT)
                .ifPresent(timeout -> setAlignmentTimeout(timeout.toMillis()));
        configuration
                .getOptional(ExecutionCheckpointingOptions.FORCE_UNALIGNED)
                .ifPresent(this::setForceUnalignedCheckpoints);
    }
}
