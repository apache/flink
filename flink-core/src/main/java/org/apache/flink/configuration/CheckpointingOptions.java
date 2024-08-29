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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.core.execution.CheckpointingMode;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;

/** A collection of all configuration options that relate to checkpoints and savepoints. */
@PublicEvolving
public class CheckpointingOptions {

    // ------------------------------------------------------------------------
    //  general checkpoint options
    // ------------------------------------------------------------------------

    /**
     * The checkpoint storage used to store operator state locally within the cluster during
     * execution.
     *
     * <p>The implementation can be specified either via their shortcut name, or via the class name
     * of a {@code StateBackendFactory}. If a StateBackendFactory class name is specified, the
     * factory is instantiated (via its zero-argument constructor) and its {@code
     * StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
     *
     * <p>Recognized shortcut names are 'hashmap' and 'rocksdb'.
     *
     * @deprecated Use {@link StateBackendOptions#STATE_BACKEND}.
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS)
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    @Deprecated
    public static final ConfigOption<String> STATE_BACKEND =
            ConfigOptions.key("state.backend.type")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("state.backend")
                    .withDescription(
                            Description.builder()
                                    .text("The state backend to be used to store state.")
                                    .linebreak()
                                    .text(
                                            "The implementation can be specified either via their shortcut "
                                                    + " name, or via the class name of a %s. "
                                                    + "If a factory is specified it is instantiated via its "
                                                    + "zero argument constructor and its %s "
                                                    + "method is called.",
                                            TextElement.code("StateBackendFactory"),
                                            TextElement.code(
                                                    "StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)"))
                                    .linebreak()
                                    .text("Recognized shortcut names are 'hashmap' and 'rocksdb'.")
                                    .build());

    /**
     * The checkpoint storage used to checkpoint state for recovery.
     *
     * <p>The implementation can be specified either via their shortcut name, or via the class name
     * of a {@code CheckpointStorageFactory}. If a CheckpointStorageFactory class name is specified,
     * the factory is instantiated (via its zero-argument constructor) and its {@code
     * CheckpointStorageFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
     *
     * <p>Recognized shortcut names are 'jobmanager' and 'filesystem'.
     *
     * <p>{@link #CHECKPOINT_STORAGE} and {@link #CHECKPOINTS_DIRECTORY} are usually combined to
     * configure the checkpoint location. By default, the checkpoint meta data and actual program
     * state will be stored in the JobManager's memory directly.
     * <li>When {@link #CHECKPOINT_STORAGE} is set to 'jobmanager', if {@link
     *     #CHECKPOINTS_DIRECTORY} is configured, the meta data of checkpoints will be persisted to
     *     the path specified by {@link #CHECKPOINTS_DIRECTORY}. Otherwise, the meta data will be
     *     stored in the JobManager's memory.
     * <li>When {@link #CHECKPOINT_STORAGE} is set to 'filesystem', a valid path must be configured
     *     to {@link #CHECKPOINTS_DIRECTORY}, and the checkpoint meta data and actual program state
     *     will both be persisted to the path.
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_CHECKPOINTING, position = 2)
    public static final ConfigOption<String> CHECKPOINT_STORAGE =
            ConfigOptions.key("execution.checkpointing.storage")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("state.checkpoint-storage")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The checkpoint storage implementation to be used to checkpoint state.")
                                    .linebreak()
                                    .text(
                                            "The implementation can be specified either via their shortcut "
                                                    + " name, or via the class name of a %s. "
                                                    + "If a factory is specified it is instantiated via its "
                                                    + "zero argument constructor and its %s "
                                                    + " method is called.",
                                            TextElement.code("CheckpointStorageFactory"),
                                            TextElement.code(
                                                    "CheckpointStorageFactory#createFromConfig(ReadableConfig, ClassLoader)"))
                                    .linebreak()
                                    .text(
                                            "Recognized shortcut names are 'jobmanager' and 'filesystem'.")
                                    .linebreak()
                                    .text(
                                            "'execution.checkpointing.storage' and 'execution.checkpointing.dir' are usually combined to configure the checkpoint location."
                                                    + " By default,  the checkpoint meta data and actual program state will be stored in the JobManager's memory directly."
                                                    + " When 'execution.checkpointing.storage' is set to 'jobmanager', if 'execution.checkpointing.dir' is configured,"
                                                    + " the meta data of checkpoints will be persisted to the path specified by 'execution.checkpointing.dir'."
                                                    + " Otherwise, the meta data will be stored in the JobManager's memory."
                                                    + " When 'execution.checkpointing.storage' is set to 'filesystem', a valid path must be configured to 'execution.checkpointing.dir',"
                                                    + " and the checkpoint meta data and actual program state will both be persisted to the path.")
                                    .build());

    /** The maximum number of completed checkpoints to retain. */
    @Documentation.Section(Documentation.Sections.COMMON_CHECKPOINTING)
    public static final ConfigOption<Integer> MAX_RETAINED_CHECKPOINTS =
            ConfigOptions.key("execution.checkpointing.num-retained")
                    .intType()
                    .defaultValue(1)
                    .withDeprecatedKeys("state.checkpoints.num-retained")
                    .withDescription("The maximum number of completed checkpoints to retain.");

    /* Option whether to clean individual checkpoint's operatorstates in parallel. If enabled,
     * operator states are discarded in parallel using the ExecutorService passed to the cleaner.
     * This speeds up checkpoints cleaning, but adds load to the IO.
     */
    @Documentation.Section(Documentation.Sections.COMMON_CHECKPOINTING)
    public static final ConfigOption<Boolean> CLEANER_PARALLEL_MODE =
            ConfigOptions.key("execution.checkpointing.cleaner.parallel-mode")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("state.checkpoint.cleaner.parallel-mode")
                    .withDescription(
                            "Option whether to discard a checkpoint's states in parallel using"
                                    + " the ExecutorService passed into the cleaner");

    /** @deprecated Checkpoints are always asynchronous. */
    @Deprecated
    public static final ConfigOption<Boolean> ASYNC_SNAPSHOTS =
            ConfigOptions.key("state.backend.async")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Deprecated option. All state snapshots are asynchronous.");

    /**
     * Option whether to create incremental checkpoints, if possible. For an incremental checkpoint,
     * only a diff from the previous checkpoint is stored, rather than the complete checkpoint
     * state.
     *
     * <p>Once enabled, the state size shown in web UI or fetched from rest API only represents the
     * delta checkpoint size instead of full checkpoint size.
     *
     * <p>Some state backends may not support incremental checkpoints and ignore this option.
     */
    @Documentation.Section(Documentation.Sections.COMMON_CHECKPOINTING)
    public static final ConfigOption<Boolean> INCREMENTAL_CHECKPOINTS =
            ConfigOptions.key("execution.checkpointing.incremental")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("state.backend.incremental")
                    .withDescription(
                            "Option whether to create incremental checkpoints, if possible. For"
                                    + " an incremental checkpoint, only a diff from the previous checkpoint is stored, rather than the"
                                    + " complete checkpoint state. Once enabled, the state size shown in web UI or fetched from rest API"
                                    + " only represents the delta checkpoint size instead of full checkpoint size."
                                    + " Some state backends may not support incremental checkpoints and ignore this option.");

    /**
     * This option configures local recovery for this state backend. By default, local recovery is
     * deactivated.
     *
     * <p>Local recovery currently only covers keyed state backends (including both the
     * EmbeddedRocksDBStateBackend and the HashMapStateBackend).
     *
     * @deprecated use {@link StateRecoveryOptions#LOCAL_RECOVERY} and {@link
     *     CheckpointingOptions#LOCAL_BACKUP_ENABLED} instead.
     */
    @Documentation.Section(Documentation.Sections.COMMON_STATE_BACKENDS)
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    @Deprecated
    public static final ConfigOption<Boolean> LOCAL_RECOVERY =
            ConfigOptions.key("state.backend.local-recovery")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "This option configures local recovery for this state backend. By default, local recovery is "
                                    + "deactivated. Local recovery currently only covers keyed state backends "
                                    + "(including both the EmbeddedRocksDBStateBackend and the HashMapStateBackend).");

    /**
     * The config parameter defining the root directories for storing file-based state for local
     * recovery.
     *
     * <p>Local recovery currently only covers keyed state backends. Currently, MemoryStateBackend
     * does not support local recovery and ignore this option.
     */
    @Documentation.Section(Documentation.Sections.COMMON_CHECKPOINTING)
    public static final ConfigOption<String> LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS =
            ConfigOptions.key("execution.checkpointing.local-backup.dirs")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("taskmanager.state.local.root-dirs")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The config parameter defining the root directories for storing file-based "
                                                    + "state for local recovery. Local recovery currently only covers keyed "
                                                    + "state backends. If not configured it will default to <WORKING_DIR>/localState. "
                                                    + "The <WORKING_DIR> can be configured via %s",
                                            TextElement.code(
                                                    ClusterOptions
                                                            .TASK_MANAGER_PROCESS_WORKING_DIR_BASE
                                                            .key()))
                                    .build());

    // ------------------------------------------------------------------------
    //  Options specific to the file-system-based state backends
    // ------------------------------------------------------------------------

    /**
     * The default directory for savepoints. Used by the state backends that write savepoints to
     * file systems (HashMapStateBackend, EmbeddedRocksDBStateBackend).
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_CHECKPOINTING, position = 4)
    public static final ConfigOption<String> SAVEPOINT_DIRECTORY =
            ConfigOptions.key("execution.checkpointing.savepoint-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("state.savepoints.dir", "savepoints.state.backend.fs.dir")
                    .withDescription(
                            "The default directory for savepoints. Used by the state backends that write savepoints to"
                                    + " file systems (HashMapStateBackend, EmbeddedRocksDBStateBackend).");

    /**
     * The default directory used for storing the data files and meta data of checkpoints in a Flink
     * supported filesystem. The storage path must be accessible from all participating
     * processes/nodes(i.e. all TaskManagers and JobManagers). If {@link #CHECKPOINT_STORAGE} is set
     * to 'jobmanager', only the meta data of checkpoints will be stored in this directory.
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_CHECKPOINTING, position = 3)
    public static final ConfigOption<String> CHECKPOINTS_DIRECTORY =
            ConfigOptions.key("execution.checkpointing.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("state.checkpoints.dir", "state.backend.fs.checkpointdir")
                    .withDescription(
                            "The default directory used for storing the data files and meta data of checkpoints "
                                    + "in a Flink supported filesystem. The storage path must be accessible from all participating processes/nodes"
                                    + "(i.e. all TaskManagers and JobManagers). If the '"
                                    + CHECKPOINT_STORAGE.key()
                                    + "' is set to 'jobmanager', only the meta data of checkpoints will be stored in this directory.");

    /**
     * Whether to create sub-directories named by job id to store the data files and meta data of
     * checkpoints. The default value is true to enable user could run several jobs with the same
     * checkpoint directory at the same time. If this value is set to false, pay attention not to
     * run several jobs with the same directory simultaneously.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_CHECKPOINTING)
    public static final ConfigOption<Boolean> CREATE_CHECKPOINT_SUB_DIR =
            ConfigOptions.key("execution.checkpointing.create-subdir")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("state.checkpoints.create-subdir")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to create sub-directories named by job id under the '%s' to store the data files and meta data "
                                                    + "of checkpoints. The default value is true to enable user could run several jobs with the same "
                                                    + "checkpoint directory at the same time. If this value is set to false, pay attention not to "
                                                    + "run several jobs with the same directory simultaneously. ",
                                            TextElement.code(CHECKPOINTS_DIRECTORY.key()))
                                    .linebreak()
                                    .text(
                                            "WARNING: This is an advanced configuration. If set to false, users must ensure that no multiple jobs are run "
                                                    + "with the same checkpoint directory, and that no files exist other than those necessary for the "
                                                    + "restoration of the current job when starting a new job.")
                                    .build());

    /**
     * The minimum size of state data files. All state chunks smaller than that are stored inline in
     * the root checkpoint metadata file.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_CHECKPOINTING)
    public static final ConfigOption<MemorySize> FS_SMALL_FILE_THRESHOLD =
            ConfigOptions.key("execution.checkpointing.data-inline-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("20kb"))
                    .withDescription(
                            "The minimum size of state data files. All state chunks smaller than that are stored"
                                    + " inline in the root checkpoint metadata file. The max memory threshold for this configuration is 1MB.")
                    .withDeprecatedKeys(
                            "state.storage.fs.memory-threshold",
                            "state.backend.fs.memory-threshold");

    /**
     * The default size of the write buffer for the checkpoint streams that write to file systems.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_CHECKPOINTING)
    public static final ConfigOption<Integer> FS_WRITE_BUFFER_SIZE =
            ConfigOptions.key("execution.checkpointing.write-buffer-size")
                    .intType()
                    .defaultValue(4 * 1024)
                    .withDescription(
                            String.format(
                                    "The default size of the write buffer for the checkpoint streams that write to file systems. "
                                            + "The actual write buffer size is determined to be the maximum of the value of this option and option '%s'.",
                                    FS_SMALL_FILE_THRESHOLD.key()))
                    .withDeprecatedKeys(
                            "state.storage.fs.write-buffer-size",
                            "state.backend.fs.write-buffer-size");

    /**
     * This option configures local backup for the state backend, which indicates whether to make
     * backup checkpoint on local disk. If not configured, fallback to {@link
     * StateRecoveryOptions#LOCAL_RECOVERY}. By default, local backup is deactivated. Local backup
     * currently only covers keyed state backends (including both the EmbeddedRocksDBStateBackend
     * and the HashMapStateBackend).
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_CHECKPOINTING)
    public static final ConfigOption<Boolean> LOCAL_BACKUP_ENABLED =
            ConfigOptions.key("execution.checkpointing.local-backup.enabled")
                    .booleanType()
                    .defaultValue(StateRecoveryOptions.LOCAL_RECOVERY.defaultValue())
                    .withFallbackKeys(StateRecoveryOptions.LOCAL_RECOVERY.key())
                    .withDeprecatedKeys(LOCAL_RECOVERY.key())
                    .withDescription(
                            "This option configures local backup for the state backend, "
                                    + "which indicates whether to make backup checkpoint on local disk.  "
                                    + "If not configured, fallback to "
                                    + StateRecoveryOptions.LOCAL_RECOVERY.key()
                                    + ". By default, local backup is deactivated. Local backup currently only "
                                    + "covers keyed state backends (including both the EmbeddedRocksDBStateBackend and the HashMapStateBackend).");

    // ------------------------------------------------------------------------
    //  Options related to file merging
    // ------------------------------------------------------------------------

    /**
     * Whether to enable merging multiple checkpoint files into one, which will greatly reduce the
     * number of small checkpoint files. See FLIP-306 for details.
     *
     * <p>Note: This is an experimental feature under evaluation, make sure you're aware of the
     * possible effects of enabling it.
     */
    @Experimental
    @Documentation.Section(value = Documentation.Sections.CHECKPOINT_FILE_MERGING, position = 1)
    public static final ConfigOption<Boolean> FILE_MERGING_ENABLED =
            ConfigOptions.key("execution.checkpointing.file-merging.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable merging multiple checkpoint files into one, which will greatly reduce"
                                    + " the number of small checkpoint files. This is an experimental feature under evaluation, "
                                    + "make sure you're aware of the possible effects of enabling it.");

    /**
     * Whether to allow merging data of multiple checkpoints into one physical file. If this option
     * is set to false, only merge files within checkpoint boundaries. Otherwise, it is possible for
     * the logical files of different checkpoints to share the same physical file.
     */
    @Experimental
    @Documentation.Section(value = Documentation.Sections.CHECKPOINT_FILE_MERGING, position = 2)
    public static final ConfigOption<Boolean> FILE_MERGING_ACROSS_BOUNDARY =
            ConfigOptions.key("execution.checkpointing.file-merging.across-checkpoint-boundary")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Only relevant if %s is enabled.",
                                            TextElement.code(FILE_MERGING_ENABLED.key()))
                                    .linebreak()
                                    .text(
                                            "Whether to allow merging data of multiple checkpoints into one physical file. "
                                                    + "If this option is set to false, "
                                                    + "only merge files within checkpoint boundaries. "
                                                    + "Otherwise, it is possible for the logical files of different "
                                                    + "checkpoints to share the same physical file.")
                                    .build());

    /** The max size of a physical file for merged checkpoints. */
    @Experimental
    @Documentation.Section(value = Documentation.Sections.CHECKPOINT_FILE_MERGING, position = 3)
    public static final ConfigOption<MemorySize> FILE_MERGING_MAX_FILE_SIZE =
            ConfigOptions.key("execution.checkpointing.file-merging.max-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("32MB"))
                    .withDescription("Max size of a physical file for merged checkpoints.");

    /**
     * Whether to use Blocking or Non-Blocking pool for merging physical files. A Non-Blocking pool
     * will always provide usable physical file without blocking. It may create many physical files
     * if poll file frequently. When poll a small file from a Blocking pool, it may be blocked until
     * the file is returned.
     */
    @Experimental
    @Documentation.Section(value = Documentation.Sections.CHECKPOINT_FILE_MERGING, position = 4)
    public static final ConfigOption<Boolean> FILE_MERGING_POOL_BLOCKING =
            ConfigOptions.key("execution.checkpointing.file-merging.pool-blocking")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to use Blocking or Non-Blocking pool for merging physical files. "
                                    + "A Non-Blocking pool will always provide usable physical file without blocking. It may create many physical files if poll file frequently. "
                                    + "When poll a small file from a Blocking pool, it may be blocked until the file is returned.");

    /**
     * The upper limit of the file pool size based on the number of subtasks within each TM (only
     * for merging private state at Task Manager level).
     *
     * <p>TODO: remove '@Documentation.ExcludeFromDocumentation' after the feature is implemented.
     */
    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Integer> FILE_MERGING_MAX_SUBTASKS_PER_FILE =
            ConfigOptions.key("execution.checkpointing.file-merging.max-subtasks-per-file")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The upper limit of the file pool size based on the number of subtasks within each TM"
                                    + "(only for merging private state at Task Manager level).");

    /**
     * Space amplification stands for the magnification of the occupied space compared to the amount
     * of valid data. The more space amplification is, the more waste of space will be. This configs
     * a space amplification above which a re-uploading for physical files will be triggered to
     * reclaim space.
     */
    @Experimental
    @Documentation.Section(value = Documentation.Sections.CHECKPOINT_FILE_MERGING, position = 6)
    public static final ConfigOption<Float> FILE_MERGING_MAX_SPACE_AMPLIFICATION =
            ConfigOptions.key("execution.checkpointing.file-merging.max-space-amplification")
                    .floatType()
                    .defaultValue(2f)
                    .withDescription(
                            "Space amplification stands for the magnification of the occupied space compared to the amount of valid data. "
                                    + "The more space amplification is, the more waste of space will be. This configs a space amplification "
                                    + "above which a re-uploading for physical files will be triggered to reclaim space. Any value below 1f "
                                    + "means disabling the space control.");

    public static final ConfigOption<CheckpointingMode> CHECKPOINTING_CONSISTENCY_MODE =
            ConfigOptions.key("execution.checkpointing.mode")
                    .enumType(CheckpointingMode.class)
                    .defaultValue(CheckpointingMode.EXACTLY_ONCE)
                    .withDescription("The checkpointing mode (exactly-once vs. at-least-once).");

    public static final ConfigOption<Duration> CHECKPOINTING_TIMEOUT =
            ConfigOptions.key("execution.checkpointing.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The maximum time that a checkpoint may take before being discarded.");

    public static final ConfigOption<Integer> MAX_CONCURRENT_CHECKPOINTS =
            ConfigOptions.key("execution.checkpointing.max-concurrent-checkpoints")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The maximum number of checkpoint attempts that may be in progress at the same time. If "
                                    + "this value is n, then no checkpoints will be triggered while n checkpoint attempts are currently in "
                                    + "flight. For the next checkpoint to be triggered, one checkpoint attempt would need to finish or "
                                    + "expire.");

    public static final ConfigOption<Duration> MIN_PAUSE_BETWEEN_CHECKPOINTS =
            ConfigOptions.key("execution.checkpointing.min-pause")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The minimal pause between checkpointing attempts. This setting defines how soon the"
                                                    + "checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger"
                                                    + "another checkpoint with respect to the maximum number of concurrent checkpoints"
                                                    + "(see %s).",
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If the maximum number of concurrent checkpoints is set to one, this setting makes effectively "
                                                    + "sure that a minimum amount of time passes where no checkpoint is in progress at all.")
                                    .build());

    public static final ConfigOption<Integer> TOLERABLE_FAILURE_NUMBER =
            ConfigOptions.key("execution.checkpointing.tolerable-failed-checkpoints")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The tolerable checkpoint consecutive failure number. If set to 0, that means "
                                    + "we do not tolerance any checkpoint failure. This only applies to the following failure reasons: IOException on the "
                                    + "Job Manager, failures in the async phase on the Task Managers and checkpoint expiration due to a timeout. Failures "
                                    + "originating from the sync phase on the Task Managers are always forcing failover of an affected task. Other types of "
                                    + "checkpoint failures (such as checkpoint being subsumed) are being ignored.");

    public static final ConfigOption<ExternalizedCheckpointRetention>
            EXTERNALIZED_CHECKPOINT_RETENTION =
                    ConfigOptions.key("execution.checkpointing.externalized-checkpoint-retention")
                            .enumType(ExternalizedCheckpointRetention.class)
                            .defaultValue(
                                    ExternalizedCheckpointRetention.NO_EXTERNALIZED_CHECKPOINTS)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "Externalized checkpoints write their meta data out to persistent storage and are not "
                                                            + "automatically cleaned up when the owning job fails or is suspended (terminating with job "
                                                            + "status %s or %s). In this case, you have to manually clean up the checkpoint state, both the "
                                                            + "meta data and actual program state.",
                                                    TextElement.code("JobStatus#FAILED"),
                                                    TextElement.code("JobStatus#SUSPENDED"))
                                            .linebreak()
                                            .linebreak()
                                            .text(
                                                    "The mode defines how an externalized checkpoint should be cleaned up on job cancellation. If "
                                                            + "you choose to retain externalized checkpoints on cancellation you have to handle checkpoint "
                                                            + "clean up manually when you cancel the job as well (terminating with job status %s).",
                                                    TextElement.code("JobStatus#CANCELED"))
                                            .linebreak()
                                            .linebreak()
                                            .text(
                                                    "The target directory for externalized checkpoints is configured via %s.",
                                                    TextElement.code(
                                                            CheckpointingOptions
                                                                    .CHECKPOINTS_DIRECTORY
                                                                    .key()))
                                            .build());

    public static final ConfigOption<Duration> CHECKPOINTING_INTERVAL_DURING_BACKLOG =
            ConfigOptions.key("execution.checkpointing.interval-during-backlog")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If it is not null and any source reports isProcessingBacklog=true, "
                                                    + "it is the interval in which checkpoints are periodically scheduled.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Checkpoint triggering may be delayed by the settings %s and %s.",
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()),
                                            TextElement.code(MIN_PAUSE_BETWEEN_CHECKPOINTS.key()))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Note: if it is not null, the value must either be 0, "
                                                    + "which means the checkpoint is disabled during backlog, "
                                                    + "or be larger than or equal to execution.checkpointing.interval.")
                                    .build());

    public static final ConfigOption<Duration> CHECKPOINTING_INTERVAL =
            ConfigOptions.key("execution.checkpointing.interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Gets the interval in which checkpoints are periodically scheduled.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "This setting defines the base interval. Checkpoint triggering may be delayed by the settings "
                                                    + "%s, %s and %s",
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()),
                                            TextElement.code(MIN_PAUSE_BETWEEN_CHECKPOINTS.key()),
                                            TextElement.code(
                                                    CHECKPOINTING_INTERVAL_DURING_BACKLOG.key()))
                                    .build());

    public static final ConfigOption<Boolean> ENABLE_UNALIGNED =
            ConfigOptions.key("execution.checkpointing.unaligned.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("execution.checkpointing.unaligned")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Unaligned checkpoints contain data stored in buffers as part of the checkpoint state, which "
                                                    + "allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration becomes "
                                                    + "independent of the current throughput as checkpoint barriers are effectively not embedded into "
                                                    + "the stream of data anymore.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Unaligned checkpoints can only be enabled if %s is %s and if %s is 1",
                                            TextElement.code(CHECKPOINTING_CONSISTENCY_MODE.key()),
                                            TextElement.code(
                                                    CheckpointingMode.EXACTLY_ONCE.toString()),
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()))
                                    .build());

    public static final ConfigOption<Duration> ALIGNED_CHECKPOINT_TIMEOUT =
            ConfigOptions.key("execution.checkpointing.aligned-checkpoint-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0L))
                    .withDeprecatedKeys("execution.checkpointing.alignment-timeout")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Only relevant if %s is enabled.",
                                            TextElement.code(ENABLE_UNALIGNED.key()))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If timeout is 0, checkpoints will always start unaligned.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If timeout has a positive value, checkpoints will start aligned. "
                                                    + "If during checkpointing, checkpoint start delay exceeds this timeout, alignment "
                                                    + "will timeout and checkpoint barrier will start working as unaligned checkpoint.")
                                    .build());

    public static final ConfigOption<Boolean> FORCE_UNALIGNED =
            ConfigOptions.key("execution.checkpointing.unaligned.forced")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Forces unaligned checkpoints, particularly allowing them for iterative jobs.")
                                    .build());

    @Experimental
    public static final ConfigOption<Boolean> ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS =
            ConfigOptions.key("execution.checkpointing.unaligned.interruptible-timers.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allows unaligned checkpoints to skip timers that are currently being fired."
                                    + " For this feature to be enabled, it must be also supported by the operator."
                                    + " Currently this is supported by all TableStreamOperators and CepOperator.");

    public static final ConfigOption<Boolean> ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH =
            ConfigOptions.key("execution.checkpointing.checkpoints-after-tasks-finish")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys(
                            "execution.checkpointing.checkpoints-after-tasks-finish.enabled")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Feature toggle for enabling checkpointing even if some of tasks"
                                                    + " have finished. Before you enable it, please take a look at %s ",
                                            link(
                                                    "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/dev/datastream/fault-tolerance/checkpointing/#checkpointing-with-parts-of-the-graph-finished",
                                                    "the important considerations"))
                                    .build());

    // TODO: deprecated
    // Currently, both two file merging mechanism can work simultaneously:
    //  1. If UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE=1 and
    // execution.checkpointing.file-merging.enabled: true, only the unified file merging mechanism
    // takes
    // effect.
    //  2. if UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE>1 and
    // execution.checkpointing.file-merging.enabled: false, only the current mechanism takes effect.
    //  3. if UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE>1 and
    // execution.checkpointing.file-merging.enabled: true, both two mechanism take effect.
    public static final ConfigOption<Integer> UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE =
            key("execution.checkpointing.unaligned.max-subtasks-per-channel-state-file")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Defines the maximum number of subtasks that share the same channel state file. "
                                    + "It can reduce the number of small files when enable unaligned checkpoint. "
                                    + "Each subtask will create a new channel state file when this is configured to 1.");
}
