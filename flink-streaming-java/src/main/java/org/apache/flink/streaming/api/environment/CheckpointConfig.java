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
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureManager.UNLIMITED_TOLERABLE_FAILURE_NUMBER;
import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Configuration that captures all checkpointing related settings. */
@Public
public class CheckpointConfig implements java.io.Serializable {

    // NOTE TO IMPLEMENTERS:
    // Please do not add further fields to this class. Use the ConfigOption stack instead!
    // It is currently very tricky to keep this kind of POJO classes in sync with instances of
    // org.apache.flink.configuration.Configuration. Instances of Configuration are way easier to
    // pass, layer, merge, restrict, copy, filter, etc.
    // See ExecutionOptions.RUNTIME_MODE for a reference implementation. If the option is very
    // crucial for the API, we can add a dedicated setter to StreamExecutionEnvironment. Otherwise,
    // introducing a ConfigOption should be enough.

    private static final long serialVersionUID = -750378776078908147L;

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointConfig.class);

    @Deprecated
    /**
     * The default checkpoint mode: exactly once.
     *
     * @deprecated This field is no longer used. Please use {@link
     *     ExecutionCheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE} instead.
     */
    public static final org.apache.flink.streaming.api.CheckpointingMode DEFAULT_MODE =
            ExecutionCheckpointingOptions.CHECKPOINTING_MODE.defaultValue();

    /**
     * The default timeout of a checkpoint attempt: 10 minutes.
     *
     * @deprecated This field is no longer used. Please use {@link
     *     ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT} instead.
     */
    @Deprecated
    public static final long DEFAULT_TIMEOUT =
            ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.defaultValue().toMillis();

    /**
     * The default minimum pause to be made between checkpoints: none.
     *
     * @deprecated This field is no longer used. Please use {@link
     *     ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS} instead.
     */
    @Deprecated
    public static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS =
            ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS.defaultValue().toMillis();

    /**
     * The default limit of concurrently happening checkpoints: one.
     *
     * @deprecated This field is no longer used. Please use {@link
     *     ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS} instead.
     */
    @Deprecated
    public static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS =
            ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS.defaultValue();

    /** @deprecated This field is no longer used. */
    @Deprecated public static final int UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER = -1;

    /**
     * Default id of checkpoint for which in-flight data should be ignored on recovery.
     *
     * @deprecated This field is no longer used. Please use {@link
     *     StateRecoveryOptions.CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA} instead.
     */
    @Deprecated
    public static final int DEFAULT_CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA =
            StateRecoveryOptions.CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA.defaultValue().intValue();

    // --------------------------------------------------------------------------------------------

    /**
     * In the long run, this field should be somehow merged with the {@link Configuration} from
     * {@link StreamExecutionEnvironment}.
     */
    private final Configuration configuration;

    /**
     * The checkpoint storage for this application. This field is marked as transient because it may
     * contain user-code.
     *
     * @deprecated this should be moved somehow to {@link #configuration}.
     */
    @Deprecated private transient CheckpointStorage storage;

    /**
     * Creates a deep copy of the provided {@link CheckpointConfig}.
     *
     * @param checkpointConfig the config to copy.
     */
    public CheckpointConfig(final CheckpointConfig checkpointConfig) {
        checkNotNull(checkpointConfig);

        this.configuration = new Configuration(checkpointConfig.configuration);
        this.storage = checkpointConfig.getCheckpointStorage();
    }

    public CheckpointConfig() {
        configuration = new Configuration();
    }

    @Internal
    public CheckpointConfig(Configuration configuration) {
        this.configuration = configuration;
    }

    // ------------------------------------------------------------------------

    /** Disables checkpointing. */
    public void disableCheckpointing() {
        configuration.removeConfig(CheckpointingOptions.CHECKPOINTING_INTERVAL);
    }

    /**
     * Checks whether checkpointing is enabled.
     *
     * @return True if checkpointing is enables, false otherwise.
     */
    public boolean isCheckpointingEnabled() {
        return getCheckpointInterval() > 0;
    }

    /**
     * Gets the checkpointing mode (exactly-once vs. at-least-once).
     *
     * @return The checkpointing mode.
     * @deprecated Use {@link #getCheckpointingConsistencyMode} instead.
     */
    @Deprecated
    public org.apache.flink.streaming.api.CheckpointingMode getCheckpointingMode() {
        return configuration.get(ExecutionCheckpointingOptions.CHECKPOINTING_MODE);
    }

    /**
     * Sets the checkpointing mode (exactly-once vs. at-least-once).
     *
     * @param checkpointingMode The checkpointing mode.
     * @deprecated Use {@link #setCheckpointingConsistencyMode} instead.
     */
    @Deprecated
    public void setCheckpointingMode(
            org.apache.flink.streaming.api.CheckpointingMode checkpointingMode) {
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, checkpointingMode);
    }

    /**
     * Gets the checkpointing consistency mode (exactly-once vs. at-least-once).
     *
     * @return The checkpointing mode.
     */
    public CheckpointingMode getCheckpointingConsistencyMode() {
        return configuration.get(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE);
    }

    /**
     * Sets the checkpointing consistency mode (exactly-once vs. at-least-once).
     *
     * @param checkpointingMode The checkpointing mode.
     */
    public void setCheckpointingConsistencyMode(CheckpointingMode checkpointingMode) {
        configuration.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, checkpointingMode);
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
        return configuration
                .getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL)
                .map(Duration::toMillis)
                .orElse(-1L);
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
        configuration.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(checkpointInterval));
    }

    /**
     * Gets the interval in which checkpoints are periodically scheduled during backlog.
     *
     * <p>This setting defines the base interval. Checkpoint triggering may be delayed by the
     * settings {@link #getMaxConcurrentCheckpoints()} and {@link #getMinPauseBetweenCheckpoints()}.
     *
     * <p>If not explicitly configured, checkpoint interval during backlog will be the same as that
     * in normal situation(see {@link #getCheckpointInterval()}). If the return value is zero, it
     * means that checkpoints would be disabled during backlog.
     *
     * @return The checkpoint interval, in milliseconds.
     */
    public long getCheckpointIntervalDuringBacklog() {
        long intervalDuringBacklog =
                configuration
                        .getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG)
                        .map(Duration::toMillis)
                        .orElseGet(this::getCheckpointInterval);

        if (intervalDuringBacklog < MINIMAL_CHECKPOINT_TIME) {
            intervalDuringBacklog = CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
        }

        long checkpointInterval = getCheckpointInterval();
        if (checkpointInterval < MINIMAL_CHECKPOINT_TIME) {
            checkpointInterval = CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
        }
        if (intervalDuringBacklog < checkpointInterval) {
            throw new IllegalArgumentException(
                    "Checkpoint interval during backlog must "
                            + "be larger than or equal to that in normal situation.");
        }

        return intervalDuringBacklog;
    }

    /**
     * Sets the interval in which checkpoints are periodically scheduled during backlog.
     *
     * <p>This setting defines the base interval. Checkpoint triggering may be delayed by the
     * settings {@link #setMaxConcurrentCheckpoints(int)} and {@link
     * #setMinPauseBetweenCheckpoints(long)}.
     *
     * <p>If not explicitly configured, checkpoint interval during backlog will be the same as that
     * in normal situation(see {@link #setCheckpointInterval(long)}). If configured to zero,
     * checkpoints would be disabled during backlog.
     *
     * @param checkpointInterval The checkpoint interval, in milliseconds.
     */
    public void setCheckpointIntervalDuringBacklog(long checkpointInterval) {
        if (checkpointInterval != 0 && checkpointInterval < MINIMAL_CHECKPOINT_TIME) {
            throw new IllegalArgumentException(
                    String.format(
                            "Checkpoint interval must be zero or larger than or equal to %s ms",
                            MINIMAL_CHECKPOINT_TIME));
        }
        configuration.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG,
                Duration.ofMillis(checkpointInterval));
    }

    /**
     * Gets the maximum time that a checkpoint may take before being discarded.
     *
     * @return The checkpoint timeout, in milliseconds.
     */
    public long getCheckpointTimeout() {
        return configuration.get(CheckpointingOptions.CHECKPOINTING_TIMEOUT).toMillis();
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
        configuration.set(
                CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(checkpointTimeout));
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
        return configuration.get(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS).toMillis();
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
        configuration.set(
                CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS,
                Duration.ofMillis(minPauseBetweenCheckpoints));
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
        return configuration.get(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS);
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
        configuration.set(
                CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, maxConcurrentCheckpoints);
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
        return configuration.get(ExecutionCheckpointingOptions.FORCE_CHECKPOINTING);
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
        configuration.set(ExecutionCheckpointingOptions.FORCE_CHECKPOINTING, forceCheckpointing);
    }

    /**
     * Checks whether unaligned checkpoints are forced, despite iteration feedback.
     *
     * @return True, if unaligned checkpoints are forced, false otherwise.
     */
    @PublicEvolving
    public boolean isForceUnalignedCheckpoints() {
        return configuration.get(CheckpointingOptions.FORCE_UNALIGNED);
    }

    /**
     * Checks whether unaligned checkpoints are forced, despite currently non-checkpointable
     * iteration feedback or custom partitioners.
     *
     * @param forceUnalignedCheckpoints The flag to force unaligned checkpoints.
     */
    @PublicEvolving
    public void setForceUnalignedCheckpoints(boolean forceUnalignedCheckpoints) {
        configuration.set(CheckpointingOptions.FORCE_UNALIGNED, forceUnalignedCheckpoints);
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
        return getTolerableCheckpointFailureNumber() == 0;
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
        if (configuration.getOptional(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER).isPresent()) {
            LOG.warn(
                    "Since CheckpointingOptions.TOLERABLE_FAILURE_NUMBER has been configured as {}, deprecated "
                            + "#setFailOnCheckpointingErrors(boolean) method would not take any effect and please use "
                            + "#setTolerableCheckpointFailureNumber(int) method to "
                            + "determine your expected behaviour when checkpoint errors on task side.",
                    getTolerableCheckpointFailureNumber());
            return;
        }
        if (failOnCheckpointingErrors) {
            setTolerableCheckpointFailureNumber(0);
        } else {
            setTolerableCheckpointFailureNumber(UNLIMITED_TOLERABLE_FAILURE_NUMBER);
        }
    }

    /**
     * Get the defined number of consecutive checkpoint failures that will be tolerated, before the
     * whole job is failed over.
     *
     * <p>If the {@link CheckpointingOptions#TOLERABLE_FAILURE_NUMBER} has not been configured, this
     * method would return 0 which means the checkpoint failure manager would not tolerate any
     * declined checkpoint failure.
     */
    public int getTolerableCheckpointFailureNumber() {
        return configuration.get(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER);
    }

    /**
     * This defines how many consecutive checkpoint failures will be tolerated, before the whole job
     * is failed over. The default value is `0`, which means no checkpoint failures will be
     * tolerated, and the job will fail on first reported checkpoint failure.
     */
    public void setTolerableCheckpointFailureNumber(int tolerableCheckpointFailureNumber) {
        if (tolerableCheckpointFailureNumber < 0) {
            throw new IllegalArgumentException(
                    "The tolerable failure checkpoint number must be non-negative.");
        }
        configuration.set(
                CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, tolerableCheckpointFailureNumber);
    }

    /**
     * Sets the mode for externalized checkpoint clean-up. Externalized checkpoints will be enabled
     * automatically unless the mode is set to {@link
     * ExternalizedCheckpointCleanup#NO_EXTERNALIZED_CHECKPOINTS}.
     *
     * <p>Externalized checkpoints write their meta data out to persistent storage and are
     * <strong>not</strong> automatically cleaned up when the owning job fails or is suspended
     * (terminating with job status {@link JobStatus#FAILED} or {@link JobStatus#SUSPENDED}). In
     * this case, you have to manually clean up the checkpoint state, both the meta data and actual
     * program state.
     *
     * <p>The {@link ExternalizedCheckpointCleanup} mode defines how an externalized checkpoint
     * should be cleaned up on job cancellation. If you choose to retain externalized checkpoints on
     * cancellation you have to handle checkpoint clean-up manually when you cancel the job as well
     * (terminating with job status {@link JobStatus#CANCELED}).
     *
     * <p>The target directory for externalized checkpoints is configured via {@link
     * CheckpointingOptions#CHECKPOINTS_DIRECTORY}.
     *
     * @param cleanupMode Externalized checkpoint clean-up behaviour.
     * @deprecated Use {@link #setExternalizedCheckpointRetention} instead.
     */
    @Deprecated
    @PublicEvolving
    public void setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup cleanupMode) {
        configuration.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, cleanupMode);
    }

    /**
     * Sets the mode for externalized checkpoint clean-up. Externalized checkpoints will be enabled
     * automatically unless the mode is set to {@link
     * ExternalizedCheckpointRetention#NO_EXTERNALIZED_CHECKPOINTS}.
     *
     * <p>Externalized checkpoints write their meta data out to persistent storage and are
     * <strong>not</strong> automatically cleaned up when the owning job fails or is suspended
     * (terminating with job status {@link JobStatus#FAILED} or {@link JobStatus#SUSPENDED}). In
     * this case, you have to manually clean up the checkpoint state, both the meta data and actual
     * program state.
     *
     * <p>The {@link ExternalizedCheckpointRetention} mode defines how an externalized checkpoint
     * should be cleaned up on job cancellation. If you choose to retain externalized checkpoints on
     * cancellation you have to handle checkpoint clean-up manually when you cancel the job as well
     * (terminating with job status {@link JobStatus#CANCELED}).
     *
     * <p>The target directory for externalized checkpoints is configured via {@link
     * CheckpointingOptions#CHECKPOINTS_DIRECTORY}.
     *
     * @param cleanupMode Externalized checkpoint clean-up behaviour.
     */
    @PublicEvolving
    public void setExternalizedCheckpointRetention(ExternalizedCheckpointRetention cleanupMode) {
        configuration.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION, cleanupMode);
    }

    /**
     * Sets the mode for externalized checkpoint clean-up. Externalized checkpoints will be enabled
     * automatically unless the mode is set to {@link
     * ExternalizedCheckpointCleanup#NO_EXTERNALIZED_CHECKPOINTS}.
     *
     * <p>Externalized checkpoints write their meta data out to persistent storage and are
     * <strong>not</strong> automatically cleaned up when the owning job fails or is suspended
     * (terminating with job status {@link JobStatus#FAILED} or {@link JobStatus#SUSPENDED}). In
     * this case, you have to manually clean up the checkpoint state, both the meta data and actual
     * program state.
     *
     * <p>The {@link ExternalizedCheckpointCleanup} mode defines how an externalized checkpoint
     * should be cleaned up on job cancellation. If you choose to retain externalized checkpoints on
     * cancellation you have to handle checkpoint clean-up manually when you cancel the job as well
     * (terminating with job status {@link JobStatus#CANCELED}).
     *
     * <p>The target directory for externalized checkpoints is configured via {@link
     * CheckpointingOptions#CHECKPOINTS_DIRECTORY}.
     *
     * @param cleanupMode Externalized checkpoint clean-up behaviour.
     * @deprecated use {@link #setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup)}
     *     instead.
     */
    @PublicEvolving
    @Deprecated
    public void enableExternalizedCheckpoints(ExternalizedCheckpointCleanup cleanupMode) {
        setExternalizedCheckpointCleanup(cleanupMode);
    }

    /**
     * Returns whether checkpoints should be persisted externally.
     *
     * @return <code>true</code> if checkpoints should be externalized.
     */
    @PublicEvolving
    public boolean isExternalizedCheckpointsEnabled() {
        return getExternalizedCheckpointCleanup()
                != ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS;
    }

    /**
     * Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.
     *
     * <p>Unaligned checkpoints contain data stored in buffers as part of the checkpoint state,
     * which allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration
     * becomes independent of the current throughput as checkpoint barriers are effectively not
     * embedded into the stream of data anymore.
     *
     * <p>Unaligned checkpoints can only be enabled if {@link
     * CheckpointingOptions#CHECKPOINTING_CONSISTENCY_MODE} is {@link
     * CheckpointingMode#EXACTLY_ONCE}.
     *
     * @param enabled Flag to indicate whether unaligned are enabled.
     */
    @PublicEvolving
    public void enableUnalignedCheckpoints(boolean enabled) {
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED, enabled);
    }

    /**
     * Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.
     *
     * <p>Unaligned checkpoints contain data stored in buffers as part of the checkpoint state,
     * which allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration
     * becomes independent of the current throughput as checkpoint barriers are effectively not
     * embedded into the stream of data anymore.
     *
     * <p>Unaligned checkpoints can only be enabled if {@link
     * CheckpointingOptions#CHECKPOINTING_CONSISTENCY_MODE} is {@link
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
        return configuration.get(CheckpointingOptions.ENABLE_UNALIGNED);
    }

    @Experimental
    public void enableUnalignedCheckpointsInterruptibleTimers(boolean enabled) {
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, enabled);
    }

    @Experimental
    public boolean isUnalignedCheckpointsInterruptibleTimersEnabled() {
        return configuration.get(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS);
    }

    /**
     * Only relevant if {@link #isUnalignedCheckpointsEnabled} is enabled.
     *
     * <p>If {@link ExecutionCheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT} has value equal to
     * <code>0</code>, checkpoints will always start unaligned.
     *
     * <p>If {@link ExecutionCheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT} has value greater then
     * <code>0</code>, checkpoints will start aligned. If during checkpointing, checkpoint start
     * delay exceeds this {@link ExecutionCheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT},
     * alignment will timeout and checkpoint will start working as unaligned checkpoint.
     *
     * @deprecated Use {@link #setAlignedCheckpointTimeout(Duration)} instead.
     */
    @Deprecated
    @PublicEvolving
    public void setAlignmentTimeout(Duration alignmentTimeout) {
        setAlignedCheckpointTimeout(alignmentTimeout);
    }

    /**
     * @return value of alignment timeout, as configured via {@link #setAlignmentTimeout(Duration)}
     *     or {@link ExecutionCheckpointingOptions#ALIGNMENT_TIMEOUT}.
     * @deprecated User {@link #getAlignedCheckpointTimeout()} instead.
     */
    @Deprecated
    @PublicEvolving
    public Duration getAlignmentTimeout() {
        return getAlignedCheckpointTimeout();
    }

    /**
     * @return value of alignment timeout, as configured via {@link
     *     #setAlignedCheckpointTimeout(Duration)} or {@link
     *     CheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT}.
     */
    @PublicEvolving
    public Duration getAlignedCheckpointTimeout() {
        return configuration.get(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT);
    }

    /**
     * Only relevant if {@link CheckpointingOptions.ENABLE_UNALIGNED} is enabled.
     *
     * <p>If {@link CheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT} has value equal to <code>0
     * </code>, checkpoints will
     *
     * <p>always start unaligned.
     *
     * <p>If {@link CheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT} has value greater then <code>0
     * </code>, checkpoints will start aligned. If during checkpointing, checkpoint start delay
     * exceeds this {@link CheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT}, alignment will timeout
     * and checkpoint will start working as unaligned checkpoint.
     */
    @PublicEvolving
    public void setAlignedCheckpointTimeout(Duration alignedCheckpointTimeout) {
        configuration.set(
                CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, alignedCheckpointTimeout);
    }

    /**
     * @return the number of subtasks to share the same channel state file, as configured via {@link
     *     #setMaxSubtasksPerChannelStateFile(int)} or {@link
     *     CheckpointingOptions#UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE}.
     */
    @PublicEvolving
    public int getMaxSubtasksPerChannelStateFile() {
        return configuration.get(
                CheckpointingOptions.UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE);
    }

    /**
     * The number of subtasks to share the same channel state file. If {@link
     * CheckpointingOptions#UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE} has value equal to <code>
     * 1</code>, each subtask will create a new channel state file.
     */
    @PublicEvolving
    public void setMaxSubtasksPerChannelStateFile(int maxSubtasksPerChannelStateFile) {
        configuration.set(
                CheckpointingOptions.UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE,
                maxSubtasksPerChannelStateFile);
    }

    /**
     * Returns whether approximate local recovery is enabled.
     *
     * @return <code>true</code> if approximate local recovery is enabled.
     */
    @Experimental
    public boolean isApproximateLocalRecoveryEnabled() {
        return configuration.get(StateRecoveryOptions.APPROXIMATE_LOCAL_RECOVERY);
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
        configuration.set(StateRecoveryOptions.APPROXIMATE_LOCAL_RECOVERY, enabled);
    }

    /**
     * Returns the cleanup behaviour for externalized checkpoints.
     *
     * @return The cleanup behaviour for externalized checkpoints or <code>null</code> if none is
     *     configured.
     * @deprecated Use {@link #getExternalizedCheckpointRetention} instead.
     */
    @Deprecated
    @PublicEvolving
    public ExternalizedCheckpointCleanup getExternalizedCheckpointCleanup() {
        return configuration.get(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT);
    }

    /**
     * Returns the cleanup behaviour for externalized checkpoints.
     *
     * @return The cleanup behaviour for externalized checkpoints or <code>null</code> if none is
     *     configured.
     */
    @PublicEvolving
    public ExternalizedCheckpointRetention getExternalizedCheckpointRetention() {
        return configuration.get(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION);
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
     * @deprecated The method is marked as deprecated because starting from Flink 1.19, the usage of
     *     all complex Java objects related to configuration, including their getter and setter
     *     methods, should be replaced by ConfigOption. In a future major version of Flink, this
     *     method will be removed entirely. It is recommended to switch to using the ConfigOptions
     *     provided for configuring checkpoint storage like the following code snippet:
     *     <pre>{@code
     * Configuration config = new Configuration();
     * config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
     * config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoints");
     * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
     * }</pre>
     *     For more details on using ConfigOption for checkpoint storage configuration, please refer
     *     to the Flink documentation: <a
     *     href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints">Checkpoints</a>
     * @param storage The checkpoint storage policy.
     */
    @Deprecated
    @PublicEvolving
    public void setCheckpointStorage(CheckpointStorage storage) {
        Preconditions.checkNotNull(storage, "Checkpoint storage must not be null");
        this.storage = storage;
    }

    /**
     * Configures the application to write out checkpoint snapshots to the configured directory. See
     * {@link FileSystemCheckpointStorage} for more details on checkpointing to a file system.
     *
     * @deprecated The method is marked as deprecated because starting from Flink 1.19, the usage of
     *     all complex Java objects related to configuration, including their getter and setter
     *     methods, should be replaced by ConfigOption. In a future major version of Flink, this
     *     method will be removed entirely. It is recommended to switch to using the ConfigOptions
     *     provided for configuring checkpoint storage like the following code snippet:
     *     <pre>{@code
     * Configuration config = new Configuration();
     * config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
     * config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoints");
     * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
     * }</pre>
     *     For more details on using ConfigOption for checkpoint storage configuration, please refer
     *     to the Flink documentation: <a
     *     href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints">Checkpoints</a>
     * @param checkpointDirectory The path to write checkpoint metadata to.
     * @see #setCheckpointStorage(CheckpointStorage)
     */
    @Deprecated
    @PublicEvolving
    public void setCheckpointStorage(String checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        this.configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);
        this.storage = new FileSystemCheckpointStorage(checkpointDirectory);
    }

    /**
     * Configures the application to write out checkpoint snapshots to the configured directory. See
     * {@link FileSystemCheckpointStorage} for more details on checkpointing to a file system.
     *
     * @deprecated The method is marked as deprecated because starting from Flink 1.19, the usage of
     *     all complex Java objects related to configuration, including their getter and setter
     *     methods, should be replaced by ConfigOption. In a future major version of Flink, this
     *     method will be removed entirely. It is recommended to switch to using the ConfigOptions
     *     provided for configuring checkpoint storage like the following code snippet:
     *     <pre>{@code
     * Configuration config = new Configuration();
     * config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
     * config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoints");
     * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
     * }</pre>
     *     For more details on using ConfigOption for checkpoint storage configuration, please refer
     *     to the Flink documentation: <a
     *     href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints">Checkpoints</a>
     * @param checkpointDirectory The path to write checkpoint metadata to.
     * @see #setCheckpointStorage(CheckpointStorage)
     */
    @Deprecated
    @PublicEvolving
    public void setCheckpointStorage(URI checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        this.configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory.toString());
        this.storage = new FileSystemCheckpointStorage(checkpointDirectory);
    }

    /**
     * Configures the application to write out checkpoint snapshots to the configured directory. See
     * {@link FileSystemCheckpointStorage} for more details on checkpointing to a file system.
     *
     * @deprecated The method is marked as deprecated because starting from Flink 1.19, the usage of
     *     all complex Java objects related to configuration, including their getter and setter
     *     methods, should be replaced by ConfigOption. In a future major version of Flink, this
     *     method will be removed entirely. It is recommended to switch to using the ConfigOptions
     *     provided for configuring checkpoint storage like the following code snippet:
     *     <pre>{@code
     * Configuration config = new Configuration();
     * config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
     * config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoints");
     * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
     * }</pre>
     *     For more details on using ConfigOption for checkpoint storage configuration, please refer
     *     to the Flink documentation: <a
     *     href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints">Checkpoints</a>
     * @param checkpointDirectory The path to write checkpoint metadata to.
     * @see #setCheckpointStorage(String)
     */
    @Deprecated
    @PublicEvolving
    public void setCheckpointStorage(Path checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        this.configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory.toString());
        this.storage = new FileSystemCheckpointStorage(checkpointDirectory);
    }

    /**
     * @deprecated The method is marked as deprecated because starting from Flink 1.19, the usage of
     *     all complex Java objects related to configuration, including their getter and setter
     *     methods, should be replaced by ConfigOption. In a future major version of Flink, this
     *     method will be removed entirely. It is recommended to find which checkpoint storage is
     *     used by checkpoint storage ConfigOption. For more details on using ConfigOption for
     *     checkpoint storage configuration, please refer to the Flink documentation: <a
     *     href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints">Checkpoints</a>
     * @return The {@link CheckpointStorage} that has been configured for the job. Or {@code null}
     *     if none has been set.
     * @see #setCheckpointStorage(CheckpointStorage)
     */
    @Deprecated
    @Nullable
    @PublicEvolving
    public CheckpointStorage getCheckpointStorage() {
        return this.storage;
    }

    /**
     * Setup the checkpoint id for which the in-flight data will be ignored for all operators in
     * case of the recovery from this checkpoint.
     *
     * @param checkpointIdOfIgnoredInFlightData Checkpoint id for which in-flight data should be
     *     ignored.
     * @see #setCheckpointIdOfIgnoredInFlightData
     */
    @PublicEvolving
    public void setCheckpointIdOfIgnoredInFlightData(long checkpointIdOfIgnoredInFlightData) {
        configuration.set(
                StateRecoveryOptions.CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA,
                checkpointIdOfIgnoredInFlightData);
    }

    /**
     * @return Checkpoint id for which in-flight data should be ignored.
     * @see #setCheckpointIdOfIgnoredInFlightData
     */
    @PublicEvolving
    public long getCheckpointIdOfIgnoredInFlightData() {
        return configuration.get(StateRecoveryOptions.CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA);
    }

    /**
     * Cleanup behaviour for externalized checkpoints when the job is cancelled.
     *
     * @deprecated This class has been moved to {@link ExternalizedCheckpointRetention}.
     */
    @Deprecated
    @PublicEvolving
    public enum ExternalizedCheckpointCleanup implements DescribedEnum {

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
        DELETE_ON_CANCELLATION(
                text(
                        "Checkpoint state is only kept when the owning job fails. It is deleted if "
                                + "the job is cancelled.")),

        /**
         * Retain externalized checkpoints on job cancellation.
         *
         * <p>All checkpoint state is kept when you cancel the owning job. You have to manually
         * delete both the checkpoint meta data and actual program state after cancelling the job.
         *
         * <p>Note that checkpoint state is always kept if the job terminates with state {@link
         * JobStatus#FAILED}.
         */
        RETAIN_ON_CANCELLATION(
                text("Checkpoint state is kept when the owning job is cancelled or fails.")),

        /** Externalized checkpoints are disabled completely. */
        NO_EXTERNALIZED_CHECKPOINTS(text("Externalized checkpoints are disabled."));

        private final InlineElement description;

        ExternalizedCheckpointCleanup(InlineElement description) {
            this.description = description;
        }

        /**
         * Returns whether persistent checkpoints shall be discarded on cancellation of the job.
         *
         * @return <code>true</code> if persistent checkpoints shall be discarded on cancellation of
         *     the job.
         */
        public boolean deleteOnCancellation() {
            return this == DELETE_ON_CANCELLATION;
        }

        @Override
        @Internal
        public InlineElement getDescription() {
            return description;
        }
    }

    /**
     * Sets all relevant options contained in the {@link ReadableConfig} such as e.g. {@link
     * CheckpointingOptions#CHECKPOINTING_CONSISTENCY_MODE}.
     *
     * <p>It will change the value of a setting only if a corresponding option was set in the {@code
     * configuration}. If a key is not present, the current value of a field will remain untouched.
     *
     * @param configuration a configuration to read the values from
     */
    public void configure(ReadableConfig configuration) {
        configuration
                .getOptional(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE)
                .ifPresent(this::setCheckpointingConsistencyMode);
        configuration
                .getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL)
                .ifPresent(i -> this.setCheckpointInterval(i.toMillis()));
        configuration
                .getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG)
                .ifPresent(i -> this.setCheckpointIntervalDuringBacklog(i.toMillis()));
        configuration
                .getOptional(CheckpointingOptions.CHECKPOINTING_TIMEOUT)
                .ifPresent(t -> this.setCheckpointTimeout(t.toMillis()));
        configuration
                .getOptional(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS)
                .ifPresent(this::setMaxConcurrentCheckpoints);
        configuration
                .getOptional(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS)
                .ifPresent(m -> this.setMinPauseBetweenCheckpoints(m.toMillis()));
        configuration
                .getOptional(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER)
                .ifPresent(this::setTolerableCheckpointFailureNumber);
        configuration
                .getOptional(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT)
                .ifPresent(this::setExternalizedCheckpointCleanup);
        configuration
                .getOptional(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION)
                .ifPresent(this::setExternalizedCheckpointRetention);
        configuration
                .getOptional(CheckpointingOptions.ENABLE_UNALIGNED)
                .ifPresent(this::enableUnalignedCheckpoints);
        configuration
                .getOptional(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS)
                .ifPresent(this::enableUnalignedCheckpointsInterruptibleTimers);
        configuration
                .getOptional(StateRecoveryOptions.CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA)
                .ifPresent(this::setCheckpointIdOfIgnoredInFlightData);
        configuration
                .getOptional(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT)
                .ifPresent(this::setAlignedCheckpointTimeout);
        configuration
                .getOptional(CheckpointingOptions.UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE)
                .ifPresent(this::setMaxSubtasksPerChannelStateFile);
        configuration
                .getOptional(CheckpointingOptions.FORCE_UNALIGNED)
                .ifPresent(this::setForceUnalignedCheckpoints);
        configuration
                .getOptional(CheckpointingOptions.CHECKPOINTS_DIRECTORY)
                .ifPresent(this::setCheckpointStorage);
        // reset checkpoint storage for backward compatibility
        configuration
                .getOptional(CheckpointingOptions.CHECKPOINT_STORAGE)
                .ifPresent(ignored -> this.storage = null);
    }

    /**
     * @return A copy of internal {@link #configuration}. Note it is missing all options that are
     *     stored as plain java fields in {@link CheckpointConfig}, for example {@link #storage}.
     */
    @Internal
    public Configuration toConfiguration() {
        return new Configuration(configuration);
    }
}
