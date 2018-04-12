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

/**
 * A collection of all configuration options that relate to checkpoints
 * and savepoints.
 */
public class CheckpointingOptions {

	// ------------------------------------------------------------------------
	//  general checkpoint and state backend options
	// ------------------------------------------------------------------------

	/** The state backend to be used to store and checkpoint state. */
	public static final ConfigOption<String> STATE_BACKEND = ConfigOptions
			.key("state.backend")
			.noDefaultValue()
			.withDescription("The state backend to be used to store and checkpoint state.");

	/** The maximum number of completed checkpoints to retain.*/
	public static final ConfigOption<Integer> MAX_RETAINED_CHECKPOINTS = ConfigOptions
			.key("state.checkpoints.num-retained")
			.defaultValue(1)
			.withDescription("The maximum number of completed checkpoints to retain.");

	/** Option whether the state backend should use an asynchronous snapshot method where
	 * possible and configurable.
	 *
	 * <p>Some state backends may not support asynchronous snapshots, or only support
	 * asynchronous snapshots, and ignore this option. */
	public static final ConfigOption<Boolean> ASYNC_SNAPSHOTS = ConfigOptions
			.key("state.backend.async")
			.defaultValue(true)
			.withDescription("Option whether the state backend should use an asynchronous snapshot method where" +
				" possible and configurable. Some state backends may not support asynchronous snapshots, or only support" +
				" asynchronous snapshots, and ignore this option.");

	/** Option whether the state backend should create incremental checkpoints,
	 * if possible. For an incremental checkpoint, only a diff from the previous
	 * checkpoint is stored, rather than the complete checkpoint state.
	 *
	 * <p>Some state backends may not support incremental checkpoints and ignore
	 * this option.*/
	public static final ConfigOption<Boolean> INCREMENTAL_CHECKPOINTS = ConfigOptions
			.key("state.backend.incremental")
			.defaultValue(false)
			.withDescription("Option whether the state backend should create incremental checkpoints, if possible. For" +
				" an incremental checkpoint, only a diff from the previous checkpoint is stored, rather than the" +
				" complete checkpoint state. Some state backends may not support incremental checkpoints and ignore" +
				" this option.");

	/**
	 * This option configures local recovery for this state backend.
	 */
	public static final ConfigOption<String> LOCAL_RECOVERY = ConfigOptions
		.key("state.backend.local-recovery")
		.defaultValue("DISABLED");

	/**
	 * The config parameter defining the root directories for storing file-based state for local recovery.
	 */
	public static final ConfigOption<String> LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS = ConfigOptions
		.key("taskmanager.state.local.root-dirs")
		.noDefaultValue();

	// ------------------------------------------------------------------------
	//  Options specific to the file-system-based state backends
	// ------------------------------------------------------------------------

	/** The default directory for savepoints. Used by the state backends that write
	 * savepoints to file systems (MemoryStateBackend, FsStateBackend, RocksDBStateBackend). */
	public static final ConfigOption<String> SAVEPOINT_DIRECTORY = ConfigOptions
			.key("state.savepoints.dir")
			.noDefaultValue()
			.withDeprecatedKeys("savepoints.state.backend.fs.dir")
			.withDescription("The default directory for savepoints. Used by the state backends that write savepoints to" +
				" file systems (MemoryStateBackend, FsStateBackend, RocksDBStateBackend).");

	/** The default directory used for checkpoints. Used by the state backends that write
	 * checkpoints to file systems (MemoryStateBackend, FsStateBackend, RocksDBStateBackend). */
	public static final ConfigOption<String> CHECKPOINTS_DIRECTORY = ConfigOptions
			.key("state.checkpoints.dir")
			.noDefaultValue()
			.withDeprecatedKeys("state.backend.fs.checkpointdir")
			.withDescription("The default directory used for checkpoints. Used by the state backends that write" +
				" checkpoints to file systems (MemoryStateBackend, FsStateBackend, RocksDBStateBackend).");

	/** The minimum size of state data files. All state chunks smaller than that
	 * are stored inline in the root checkpoint metadata file. */
	public static final ConfigOption<Integer> FS_SMALL_FILE_THRESHOLD = ConfigOptions
			.key("state.backend.fs.memory-threshold")
			.defaultValue(1024)
			.withDescription("The minimum size of state data files. All state chunks smaller than that are stored" +
				" inline in the root checkpoint metadata file.");

	// ------------------------------------------------------------------------
	//  Options specific to the RocksDB state backend
	// ------------------------------------------------------------------------

	/** The local directory (on the TaskManager) where RocksDB puts its files. */
	public static final ConfigOption<String> ROCKSDB_LOCAL_DIRECTORIES = ConfigOptions
			.key("state.backend.rocksdb.localdir")
			.noDefaultValue()
			.withDeprecatedKeys("state.backend.rocksdb.checkpointdir")
			.withDescription("The local directory (on the TaskManager) where RocksDB puts its files.");
}
