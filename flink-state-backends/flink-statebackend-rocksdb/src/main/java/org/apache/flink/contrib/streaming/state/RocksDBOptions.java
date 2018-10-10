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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType.HEAP;
import static org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType.ROCKSDB;

/**
 * Configuration options for the RocksDB backend.
 */
public class RocksDBOptions {

	/** The local directory (on the TaskManager) where RocksDB puts its files. */
	public static final ConfigOption<String> LOCAL_DIRECTORIES = ConfigOptions
		.key("state.backend.rocksdb.localdir")
		.noDefaultValue()
		.withDeprecatedKeys("state.backend.rocksdb.checkpointdir")
		.withDescription("The local directory (on the TaskManager) where RocksDB puts its files.");

	/**
	 * Choice of timer service implementation.
	 */
	public static final ConfigOption<String> TIMER_SERVICE_FACTORY = ConfigOptions
		.key("state.backend.rocksdb.timer-service.factory")
		.defaultValue(HEAP.name())
		.withDescription(String.format("This determines the factory for timer service state implementation. Options " +
			"are either %s (heap-based, default) or %s for an implementation based on RocksDB .",
			HEAP.name(), ROCKSDB.name()));

	public static final ConfigOption<Boolean> MONITOR_NUM_IMMUTABLE_MEM_TABLES = ConfigOptions
		.key(RocksDBProperty.NumImmutableMemTable.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the number of immutable memtables in RocksDB.");

	public static final ConfigOption<Boolean> MONITOR_MEM_TABLE_FLUSH_PENDING = ConfigOptions
		.key(RocksDBProperty.MemTableFlushPending.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the number of pending memtable flushes in RocksDB.");

	public static final ConfigOption<Boolean> MONITOR_COMPACTION_PENDING = ConfigOptions
		.key(RocksDBProperty.CompactionPending.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor pending compactions in RocksDB.");

	public static final ConfigOption<Boolean> MONITOR_BACKGROUND_ERRORS = ConfigOptions
		.key(RocksDBProperty.BackgroundErrors.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the number of background errors in RocksDB.");

	public static final ConfigOption<Boolean> MONITOR_CUR_SIZE_ACTIVE_MEM_TABLE = ConfigOptions
		.key(RocksDBProperty.CurSizeActiveMemTable.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the approximate size of the active memtable in bytes.");

	public static final ConfigOption<Boolean> MONITOR_CUR_SIZE_ALL_MEM_TABLE = ConfigOptions
		.key(RocksDBProperty.CurSizeAllMemTables.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the approximate size of the active and unflushed immutable memtables" +
			" in bytes.");

	public static final ConfigOption<Boolean> MONITOR_SIZE_ALL_MEM_TABLES = ConfigOptions
		.key(RocksDBProperty.SizeAllMemTables.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the approximate size of the active, unflushed immutable, " +
			"and pinned immutable memtables in bytes.");

	public static final ConfigOption<Boolean> MONITOR_NUM_ENTRIES_ACTIVE_MEM_TABLE = ConfigOptions
		.key(RocksDBProperty.NumEntriesActiveMemTable.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the total number of entries in the active memtable.");

	public static final ConfigOption<Boolean> MONITOR_NUM_ENTRIES_IMM_MEM_TABLES = ConfigOptions
		.key(RocksDBProperty.NumEntriesImmMemTables.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the total number of entries in the unflushed immutable memtables.");

	public static final ConfigOption<Boolean> MONITOR_NUM_DELETES_ACTIVE_MEM_TABLE = ConfigOptions
		.key(RocksDBProperty.NumDeletesActiveMemTable.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the total number of delete entries in the active memtable.");

	public static final ConfigOption<Boolean> MONITOR_NUM_DELETES_IMM_MEM_TABLE = ConfigOptions
		.key(RocksDBProperty.NumDeletesImmMemTables.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the total number of delete entries in the unflushed immutable memtables.");

	public static final ConfigOption<Boolean> ESTIMATE_NUM_KEYS = ConfigOptions
		.key(RocksDBProperty.EstimateNumKeys.getConfigKey())
		.defaultValue(false)
		.withDescription("Estimate the number of keys in RocksDB.");

	public static final ConfigOption<Boolean> ESTIMATE_TABLE_READERS_MEM = ConfigOptions
		.key(RocksDBProperty.EstimateTableReadersMem.getConfigKey())
		.defaultValue(false)
		.withDescription("Estimate the memory used for reading SST tables, excluding memory" +
			" used in block cache (e.g.,filter and index blocks) in bytes.");

	public static final ConfigOption<Boolean> MONITOR_NUM_SNAPSHOTS = ConfigOptions
		.key(RocksDBProperty.NumSnapshots.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the number of unreleased snapshots of the database.");

	public static final ConfigOption<Boolean> MONITOR_NUM_LIVE_VERSIONS = ConfigOptions
		.key(RocksDBProperty.NumLiveVersions.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor number of live versions. `Version`is an internal data structure. " +
			"See version_set.h for details. More live versions often mean more SST files are held " +
			"from being deleted, by iterators or unfinished compactions.");

	public static final ConfigOption<Boolean> ESTIMATE_LIVE_DATA_SIZE = ConfigOptions
		.key(RocksDBProperty.EstimateLiveDataSize.getConfigKey())
		.defaultValue(false)
		.withDescription("Estimate of the amount of live data in bytes.");

	public static final ConfigOption<Boolean> MONITOR_TOTAL_SST_FILES_SIZE = ConfigOptions
		.key(RocksDBProperty.TotalSstFilesSize.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the total size (bytes) of all SST files." +
			"<b>WARNING</b>: may slow down online queries if there are too many files.");

	public static final ConfigOption<Boolean> ESTIMATE_PENDING_COMPACTION_BYTES = ConfigOptions
		.key(RocksDBProperty.EstimatePendingCompactionBytes.getConfigKey())
		.defaultValue(false)
		.withDescription("Estimated total number of bytes compaction needs to rewrite to get all levels " +
			"down to under target size. Not valid for other compactions than level-based.");

	public static final ConfigOption<Boolean> MONITOR_NUM_RUNNING_COMPACTIONS = ConfigOptions
		.key(RocksDBProperty.NumRunningCompactions.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the number of currently running compactions.");

	public static final ConfigOption<Boolean> MONITOR_NUM_RUNNING_FLUSHES = ConfigOptions
		.key(RocksDBProperty.NumRunningFlushes.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the number of currently running flushes.");

	public static final ConfigOption<Boolean> MONITOR_ACTUAL_DELAYED_WRITE_RATE = ConfigOptions
		.key(RocksDBProperty.ActualDelayedWriteRate.getConfigKey())
		.defaultValue(false)
		.withDescription("Monitor the current actual delayed write rate. 0 means no delay.");
}
