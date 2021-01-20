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

import org.apache.flink.annotation.docs.Documentation;

/** A collection of all configuration options that relate to checkpoints and savepoints. */
public class CheckpointingOptions {

    // ------------------------------------------------------------------------
    //  general checkpoint and state backend options
    // ------------------------------------------------------------------------

    /** The state backend to be used to store and checkpoint state. */
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 1)
    public static final ConfigOption<String> STATE_BACKEND =
            ConfigOptions.key("state.backend")
                    .noDefaultValue()
                    .withDescription("The state backend to be used to store and checkpoint state.");

    /** The checkpoint storage used to checkpoint state. */
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 2)
    @Documentation.ExcludeFromDocumentation(
            "Hidden until FileSystemStorage and JobManagerStorage are implemented")
    public static final ConfigOption<String> CHECKPOINT_STORAGE =
            ConfigOptions.key("state.checkpoint-storage")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The state backend to be used to checkpoint state.");

    /** The maximum number of completed checkpoints to retain. */
    @Documentation.Section(Documentation.Sections.COMMON_STATE_BACKENDS)
    public static final ConfigOption<Integer> MAX_RETAINED_CHECKPOINTS =
            ConfigOptions.key("state.checkpoints.num-retained")
                    .defaultValue(1)
                    .withDescription("The maximum number of completed checkpoints to retain.");

    /**
     * Option whether the state backend should use an asynchronous snapshot method where possible
     * and configurable.
     *
     * <p>Some state backends may not support asynchronous snapshots, or only support asynchronous
     * snapshots, and ignore this option.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_STATE_BACKENDS)
    public static final ConfigOption<Boolean> ASYNC_SNAPSHOTS =
            ConfigOptions.key("state.backend.async")
                    .defaultValue(true)
                    .withDescription(
                            "Option whether the state backend should use an asynchronous snapshot method where"
                                    + " possible and configurable. Some state backends may not support asynchronous snapshots, or only support"
                                    + " asynchronous snapshots, and ignore this option.");

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
     * <p>Local recovery currently only covers keyed state backends. Currently, MemoryStateBackend
     * does not support local recovery and ignore this option.
     */
    @Documentation.Section(Documentation.Sections.COMMON_STATE_BACKENDS)
    public static final ConfigOption<Boolean> LOCAL_RECOVERY =
            ConfigOptions.key("state.backend.local-recovery")
                    .defaultValue(false)
                    .withDescription(
                            "This option configures local recovery for this state backend. By default, local recovery is "
                                    + "deactivated. Local recovery currently only covers keyed state backends. Currently, MemoryStateBackend does "
                                    + "not support local recovery and ignore this option.");

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
                    .noDefaultValue()
                    .withDescription(
                            "The config parameter defining the root directories for storing file-based state for local "
                                    + "recovery. Local recovery currently only covers keyed state backends. Currently, MemoryStateBackend does "
                                    + "not support local recovery and ignore this option");

    // ------------------------------------------------------------------------
    //  Options specific to the file-system-based state backends
    // ------------------------------------------------------------------------

    /**
     * The default directory for savepoints. Used by the state backends that write savepoints to
     * file systems (MemoryStateBackend, FsStateBackend, RocksDBStateBackend).
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 3)
    public static final ConfigOption<String> SAVEPOINT_DIRECTORY =
            ConfigOptions.key("state.savepoints.dir")
                    .noDefaultValue()
                    .withDeprecatedKeys("savepoints.state.backend.fs.dir")
                    .withDescription(
                            "The default directory for savepoints. Used by the state backends that write savepoints to"
                                    + " file systems (MemoryStateBackend, FsStateBackend, RocksDBStateBackend).");

    /**
     * The default directory used for storing the data files and meta data of checkpoints in a Flink
     * supported filesystem. The storage path must be accessible from all participating
     * processes/nodes(i.e. all TaskManagers and JobManagers).
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 2)
    public static final ConfigOption<String> CHECKPOINTS_DIRECTORY =
            ConfigOptions.key("state.checkpoints.dir")
                    .noDefaultValue()
                    .withDeprecatedKeys("state.backend.fs.checkpointdir")
                    .withDescription(
                            "The default directory used for storing the data files and meta data of checkpoints "
                                    + "in a Flink supported filesystem. The storage path must be accessible from all participating processes/nodes"
                                    + "(i.e. all TaskManagers and JobManagers).");

    /**
     * The minimum size of state data files. All state chunks smaller than that are stored inline in
     * the root checkpoint metadata file.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_STATE_BACKENDS)
    public static final ConfigOption<MemorySize> FS_SMALL_FILE_THRESHOLD =
            ConfigOptions.key("state.backend.fs.memory-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("20kb"))
                    .withDescription(
                            "The minimum size of state data files. All state chunks smaller than that are stored"
                                    + " inline in the root checkpoint metadata file. The max memory threshold for this configuration is 1MB.");

    /**
     * The default size of the write buffer for the checkpoint streams that write to file systems.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_STATE_BACKENDS)
    public static final ConfigOption<Integer> FS_WRITE_BUFFER_SIZE =
            ConfigOptions.key("state.backend.fs.write-buffer-size")
                    .defaultValue(4 * 1024)
                    .withDescription(
                            String.format(
                                    "The default size of the write buffer for the checkpoint streams that write to file systems. "
                                            + "The actual write buffer size is determined to be the maximum of the value of this option and option '%s'.",
                                    FS_SMALL_FILE_THRESHOLD.key()));
}
