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
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

/** A collection of all configuration options that relate to checkpoints and savepoints. */
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
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 2)
    public static final ConfigOption<String> CHECKPOINT_STORAGE =
            ConfigOptions.key("state.checkpoint-storage")
                    .stringType()
                    .noDefaultValue()
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
                                            "'state.checkpoint-storage' and 'state.checkpoints.dir' are usually combined to configure the checkpoint location."
                                                    + " By default,  the checkpoint meta data and actual program state will be stored in the JobManager's memory directly."
                                                    + " When 'state.checkpoint-storage' is set to 'jobmanager', if 'state.checkpoints.dir' is configured,"
                                                    + " the meta data of checkpoints will be persisted to the path specified by 'state.checkpoints.dir'."
                                                    + " Otherwise, the meta data will be stored in the JobManager's memory."
                                                    + " When 'state.checkpoint-storage' is set to 'filesystem', a valid path must be configured to 'state.checkpoints.dir',"
                                                    + " and the checkpoint meta data and actual program state will both be persisted to the path.")
                                    .build());

    /** The maximum number of completed checkpoints to retain. */
    @Documentation.Section(Documentation.Sections.COMMON_STATE_BACKENDS)
    public static final ConfigOption<Integer> MAX_RETAINED_CHECKPOINTS =
            ConfigOptions.key("state.checkpoints.num-retained")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The maximum number of completed checkpoints to retain.");

    /* Option whether to clean individual checkpoint's operatorstates in parallel. If enabled,
     * operator states are discarded in parallel using the ExecutorService passed to the cleaner.
     * This speeds up checkpoints cleaning, but adds load to the IO.
     */
    @Documentation.Section(Documentation.Sections.COMMON_STATE_BACKENDS)
    public static final ConfigOption<Boolean> CLEANER_PARALLEL_MODE =
            ConfigOptions.key("state.checkpoint.cleaner.parallel-mode")
                    .booleanType()
                    .defaultValue(true)
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
     * Option whether the state backend should create incremental checkpoints, if possible. For an
     * incremental checkpoint, only a diff from the previous checkpoint is stored, rather than the
     * complete checkpoint state.
     *
     * <p>Once enabled, the state size shown in web UI or fetched from rest API only represents the
     * delta checkpoint size instead of full checkpoint size.
     *
     * <p>Some state backends may not support incremental checkpoints and ignore this option.
     */
    @Documentation.Section(Documentation.Sections.COMMON_STATE_BACKENDS)
    public static final ConfigOption<Boolean> INCREMENTAL_CHECKPOINTS =
            ConfigOptions.key("state.backend.incremental")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Option whether the state backend should create incremental checkpoints, if possible. For"
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
    @Documentation.Section(Documentation.Sections.COMMON_STATE_BACKENDS)
    public static final ConfigOption<String> LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS =
            ConfigOptions.key("taskmanager.state.local.root-dirs")
                    .stringType()
                    .noDefaultValue()
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
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 3)
    public static final ConfigOption<String> SAVEPOINT_DIRECTORY =
            ConfigOptions.key("state.savepoints.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("savepoints.state.backend.fs.dir")
                    .withDescription(
                            "The default directory for savepoints. Used by the state backends that write savepoints to"
                                    + " file systems (HashMapStateBackend, EmbeddedRocksDBStateBackend).");

    /**
     * The default directory used for storing the data files and meta data of checkpoints in a Flink
     * supported filesystem. The storage path must be accessible from all participating
     * processes/nodes(i.e. all TaskManagers and JobManagers). If {@link #CHECKPOINT_STORAGE} is set
     * to 'jobmanager', only the meta data of checkpoints will be stored in this directory.
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 2)
    public static final ConfigOption<String> CHECKPOINTS_DIRECTORY =
            ConfigOptions.key("state.checkpoints.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("state.backend.fs.checkpointdir")
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
    @Documentation.Section(Documentation.Sections.EXPERT_STATE_BACKENDS)
    public static final ConfigOption<Boolean> CREATE_CHECKPOINT_SUB_DIR =
            ConfigOptions.key("state.checkpoints.create-subdir")
                    .booleanType()
                    .defaultValue(true)
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
    @Documentation.Section(Documentation.Sections.EXPERT_STATE_BACKENDS)
    public static final ConfigOption<MemorySize> FS_SMALL_FILE_THRESHOLD =
            ConfigOptions.key("state.storage.fs.memory-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("20kb"))
                    .withDescription(
                            "The minimum size of state data files. All state chunks smaller than that are stored"
                                    + " inline in the root checkpoint metadata file. The max memory threshold for this configuration is 1MB.")
                    .withDeprecatedKeys("state.backend.fs.memory-threshold");

    /**
     * The default size of the write buffer for the checkpoint streams that write to file systems.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_STATE_BACKENDS)
    public static final ConfigOption<Integer> FS_WRITE_BUFFER_SIZE =
            ConfigOptions.key("state.storage.fs.write-buffer-size")
                    .intType()
                    .defaultValue(4 * 1024)
                    .withDescription(
                            String.format(
                                    "The default size of the write buffer for the checkpoint streams that write to file systems. "
                                            + "The actual write buffer size is determined to be the maximum of the value of this option and option '%s'.",
                                    FS_SMALL_FILE_THRESHOLD.key()))
                    .withDeprecatedKeys("state.backend.fs.write-buffer-size");

    /**
     * This option configures local backup for the state backend, which indicates whether to make
     * backup checkpoint on local disk. If not configured, fallback to {@link
     * StateRecoveryOptions#LOCAL_RECOVERY}. By default, local backup is deactivated. Local backup
     * currently only covers keyed state backends (including both the EmbeddedRocksDBStateBackend
     * and the HashMapStateBackend).
     */
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
            ConfigOptions.key("state.checkpoints.file-merging.enabled")
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
            ConfigOptions.key("state.checkpoints.file-merging.across-checkpoint-boundary")
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
            ConfigOptions.key("state.checkpoints.file-merging.max-file-size")
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
            ConfigOptions.key("state.checkpoints.file-merging.pool-blocking")
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
            ConfigOptions.key("state.checkpoints.file-merging.max-subtasks-per-file")
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
     *
     * <p>TODO: remove '@Documentation.ExcludeFromDocumentation' after the feature is implemented.
     */
    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Float> FILE_MERGING_MAX_SPACE_AMPLIFICATION =
            ConfigOptions.key("state.checkpoints.file-merging.max-space-amplification")
                    .floatType()
                    .defaultValue(2f)
                    .withDescription(
                            "Space amplification stands for the magnification of the occupied space compared to the amount of valid data. "
                                    + "The more space amplification is, the more waste of space will be. This configs a space amplification "
                                    + "above which a re-uploading for physical files will be triggered to reclaim space.");
}
