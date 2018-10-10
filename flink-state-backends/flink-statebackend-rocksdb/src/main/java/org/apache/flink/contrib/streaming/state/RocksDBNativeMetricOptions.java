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

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Enable which RocksDB metrics to forward to Flink's metrics reporter.
 * All metrics report at the column family level and return unsigned long values.
 *
 * <p>Properties and doc comments are taken from RocksDB documentation. See
 * <a href="https://github.com/facebook/rocksdb/blob/64324e329eb0a9b4e77241a425a1615ff524c7f1/include/rocksdb/db.h#L429">
 * db.h</a> for more information.
 */
public class RocksDBNativeMetricOptions implements Serializable {

	private Set<String> properties;

	/**
	 * Creates a {@link RocksDBNativeMetricOptions} based on an
	 * external configuration.
	 */
	public static RocksDBNativeMetricOptions fromConfig(Configuration config) {
		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_IMMUTABLE_MEM_TABLES)) {
			options.enableNumImmutableMemTable();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_MEM_TABLE_FLUSH_PENDING)) {
			options.enableMemTableFlushPending();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_COMPACTION_PENDING)) {
			options.enableCompactionPending();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_BACKGROUND_ERRORS)) {
			options.enableBackgroundErrors();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_CUR_SIZE_ACTIVE_MEM_TABLE)) {
			options.enableCurSizeActiveMemTable();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_CUR_SIZE_ALL_MEM_TABLE)) {
			options.enableCurSizeAllMemTables();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_SIZE_ALL_MEM_TABLES)) {
			options.enableSizeAllMemTables();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_ENTRIES_ACTIVE_MEM_TABLE)) {
			options.enableNumEntriesActiveMemTable();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_ENTRIES_IMM_MEM_TABLES)) {
			options.enableNumEntriesImmMemTables();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_DELETES_ACTIVE_MEM_TABLE)) {
			options.enableNumDeletesActiveMemTable();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_DELETES_IMM_MEM_TABLE)) {
			options.enableNumDeletesImmMemTables();
		}

		if (config.getBoolean(RocksDBOptions.ESTIMATE_NUM_KEYS)) {
			options.enableEstimateNumKeys();
		}

		if (config.getBoolean(RocksDBOptions.ESTIMATE_TABLE_READERS_MEM)) {
			options.enableEstimateTableReadersMem();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_SNAPSHOTS)) {
			options.enableNumSnapshots();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_LIVE_VERSIONS)) {
			options.enableNumLiveVersions();
		}

		if (config.getBoolean(RocksDBOptions.ESTIMATE_LIVE_DATA_SIZE)) {
			options.enableEstimateLiveDataSize();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_TOTAL_SST_FILES_SIZE)) {
			options.enableTotalSstFilesSize();
		}

		if (config.getBoolean(RocksDBOptions.ESTIMATE_PENDING_COMPACTION_BYTES)) {
			options.enableEstimatePendingCompactionBytes();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_RUNNING_COMPACTIONS)) {
			options.enableNumRunningCompactions();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_NUM_RUNNING_FLUSHES)) {
			options.enableNumRunningFlushes();
		}

		if (config.getBoolean(RocksDBOptions.MONITOR_ACTUAL_DELAYED_WRITE_RATE)) {
			options.enableActualDelayedWriteRate();
		}

		return options;
	}

	public RocksDBNativeMetricOptions() {
		this.properties = new HashSet<>();
	}

	/**
	 * Returns number of immutable memtables that have not yet been flushed.
	 */
	public void enableNumImmutableMemTable() {
		this.properties.add(RocksDBProperty.NumImmutableMemTable.getRocksDBProperty());
	}

	/**
	 * Returns 1 if a memtable flush is pending; otherwise, returns 0.
	 */
	public void enableMemTableFlushPending() {
		this.properties.add(RocksDBProperty.MemTableFlushPending.getRocksDBProperty());
	}

	/**
	 * Returns 1 if at least one compaction is pending; otherwise, returns 0.
	 */
	public void enableCompactionPending() {
		this.properties.add(RocksDBProperty.CompactionPending.getRocksDBProperty());
	}

	/**
	 * Returns accumulated number of background errors.
	 */
	public void enableBackgroundErrors() {
		this.properties.add(RocksDBProperty.BackgroundErrors.getRocksDBProperty());
	}

	/**
	 * Returns approximate size of active memtable (bytes).
	 */
	public void enableCurSizeActiveMemTable() {
		this.properties.add(RocksDBProperty.CurSizeActiveMemTable.getRocksDBProperty());
	}

	/**
	 * Returns approximate size of active and unflushed immutable memtables (bytes).
	 */
	public void enableCurSizeAllMemTables() {
		this.properties.add(RocksDBProperty.CurSizeAllMemTables.getRocksDBProperty());
	}

	/**
	 * Returns approximate size of active, unflushed immutable, and pinned immutable memtables (bytes).
	 */
	public void enableSizeAllMemTables() {
		this.properties.add(RocksDBProperty.SizeAllMemTables.getRocksDBProperty());
	}

	/**
	 * Returns total number of entries in the active memtable.
	 */
	public void enableNumEntriesActiveMemTable() {
		this.properties.add(RocksDBProperty.NumEntriesActiveMemTable.getRocksDBProperty());
	}

	/**
	 * Returns total number of entries in the unflushed immutable memtables.
	 */
	public void enableNumEntriesImmMemTables() {
		this.properties.add(RocksDBProperty.NumEntriesImmMemTables.getRocksDBProperty());
	}

	/**
	 * Returns total number of delete entries in the active memtable.
	 */
	public void enableNumDeletesActiveMemTable() {
		this.properties.add(RocksDBProperty.NumDeletesActiveMemTable.getRocksDBProperty());
	}

	/**
	 * Returns total number of delete entries in the unflushed immutable memtables.
	 */
	public void enableNumDeletesImmMemTables() {
		this.properties.add(RocksDBProperty.NumDeletesImmMemTables.getRocksDBProperty());
	}

	/**
	 * Returns estimated number of total keys in the active and unflushed immutable memtables and storage.
	 */
	public void enableEstimateNumKeys() {
		this.properties.add(RocksDBProperty.EstimateNumKeys.getRocksDBProperty());
	}

	/**
	 * Returns estimated memory used for reading SST tables, excluding memory
	 * used in block cache (e.g.,filter and index blocks).
	 */
	public void enableEstimateTableReadersMem() {
		this.properties.add(RocksDBProperty.EstimateTableReadersMem.getRocksDBProperty());
	}

	/**
	 * Returns number of unreleased snapshots of the database.
	 */
	public void enableNumSnapshots() {
		this.properties.add(RocksDBProperty.NumSnapshots.getRocksDBProperty());
	}

	/**
	 * Returns number of live versions. `Version`
	 * is an internal data structure. See version_set.h for details. More
	 * live versions often mean more SST files are held from being deleted,
	 * by iterators or unfinished compactions.
	 */
	public void enableNumLiveVersions() {
		this.properties.add(RocksDBProperty.NumLiveVersions.getRocksDBProperty());
	}

	/**
	 * Returns an estimate of the amount of live data in bytes.
	 */
	public void enableEstimateLiveDataSize() {
		this.properties.add(RocksDBProperty.EstimateLiveDataSize.getRocksDBProperty());
	}

	/**
	 * Returns total size (bytes) of all SST files.
	 * <strong>WARNING</strong>: may slow down online queries if there are too many files.
	 */
	public void enableTotalSstFilesSize() {
		this.properties.add(RocksDBProperty.TotalSstFilesSize.getRocksDBProperty());
	}

	/**
	 * Returns estimated total number of bytes compaction needs to rewrite to get all levels down
	 * to under target size. Not valid for other compactions than level-based.
	 */
	public void enableEstimatePendingCompactionBytes() {
		this.properties.add(RocksDBProperty.EstimatePendingCompactionBytes.getRocksDBProperty());
	}

	/**
	 * Returns the number of currently running compactions.
	 */
	public void enableNumRunningCompactions() {
		this.properties.add(RocksDBProperty.NumRunningCompactions.getRocksDBProperty());
	}

	/**
	 * Returns the number of currently running flushes.
	 */
	public void enableNumRunningFlushes() {
		this.properties.add(RocksDBProperty.NumRunningFlushes.getRocksDBProperty());
	}

	/**
	 * Returns the current actual delayed write rate. 0 means no delay.
	 */
	public void enableActualDelayedWriteRate() {
		this.properties.add(RocksDBProperty.ActualDelayedWriteRate.getRocksDBProperty());
	}

	/**
	 * @return the enabled RocksDB metrics
	 */
	public Collection<String> getProperties() {
		return Collections.unmodifiableCollection(properties);
	}

	/**
	 * {{@link RocksDBNativeMetricMonitor}} is enabled is any property is set.
	 *
	 * @return true if {{RocksDBNativeMetricMonitor}} should be enabled, false otherwise.
	 */
	public boolean isEnabled() {
		return !properties.isEmpty();
	}
}
