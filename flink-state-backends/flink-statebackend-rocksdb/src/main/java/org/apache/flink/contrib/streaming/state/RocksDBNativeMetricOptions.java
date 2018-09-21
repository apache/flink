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
public class RocksDBNativeMetricOptions {

	private static final long TEN_SECONDS = 10 * 1000;

	private Set<String> properties;

	private long frequency = TEN_SECONDS;

	public RocksDBNativeMetricOptions() {
		this.properties = new HashSet<>();
	}

	/**
	 * Returns number of immutable memtables that have not yet been flushed.
	 */
	public void enableNumImmutableMemTable() {
		this.properties.add("rocksdb.num-immutable-mem-table");
	}

	/**
	 * Returns 1 if a memtable flush is pending; otherwise, returns 0.
	 */
	public void enableMemTableFlushPending() {
		this.properties.add("rocksdb.mem-table-flush-pending");
	}

	/**
	 * Returns 1 if at least one compaction is pending; otherwise, returns 0.
	 */
	public void enableCompactionPending() {
		this.properties.add("rocksdb.compaction-pending");
	}

	/**
	 * Returns accumulated number of background errors.
	 */
	public void enableBackgroundErrors() {
		this.properties.add("rocksdb.background-errors");
	}

	/**
	 * Returns approximate size of active memtable (bytes).
	 */
	public void enableCurSizeActiveMemTable() {
		this.properties.add("rocksdb.cur-size-active-mem-table");
	}

	/**
	 * Returns approximate size of active and unflushed immutable memtables (bytes).
	 */
	public void enableCurSizeAllMemTables() {
		this.properties.add("rocksdb.cur-size-all-mem-tables");
	}

	/**
	 * Returns approximate size of active, unflushed immutable, and pinned immutable memtables (bytes).
	 */
	public void enableSizeAllMemTables() {
		this.properties.add("rocksdb.size-all-mem-tables");
	}

	/**
	 * Returns total number of entries in the active memtable.
	 */
	public void enableNumEntriesActiveMemTable() {
		this.properties.add("rocksdb.num-entries-active-mem-table");
	}

	/**
	 * Returns total number of entries in the unflushed immutable memtables.
	 */
	public void enableNumEntriesImmMemTables() {
		this.properties.add("rocksdb.num-entries-imm-mem-tables");
	}

	/**
	 * Returns total number of delete entries in the active memtable.
	 */
	public void enableNumDeletesActiveMemTable() {
		this.properties.add("rocksdb.num-deletes-active-mem-table");
	}

	/**
	 * Returns total number of delete entries in the unflushed immutable memtables.
	 */
	public void enableNumDeletesImmMemTables() {
		this.properties.add("rocksdb.num-deletes-imm-mem-tables");
	}

	/**
	 * Returns estimated number of total keys in the active and unflushed immutable memtables and storage.
	 */
	public void enableEstimateNumKeys() {
		this.properties.add("rocksdb.estimate-num-keys");
	}

	/**
	 * Returns estimated memory used for reading SST tables, excluding memory
	 * used in block cache (e.g.,filter and index blocks).
	 */
	public void enableEstimateTableReadersMem() {
		this.properties.add("rocksdb.estimate-table-readers-mem");
	}

	/**
	 * Returns number of unreleased snapshots of the database.
	 */
	public void enableNumSnapshots() {
		this.properties.add("rocksdb.num-snapshots");
	}

	/**
	 * Returns number representing unix timestamp of oldest unreleased snapshot.
	 */
	public void enableOldestSnapshotTime() {
		this.properties.add("rocksdb.oldest-snapshot-time");
	}

	/**
	 * Returns number of live versions. `Version`
	 * is an internal data structure. See version_set.h for details. More
	 * live versions often mean more SST files are held from being deleted,
	 * by iterators or unfinished compactions.
	 */
	public void enableNumLiveVersions() {
		this.properties.add("rocksdb.num-live-versions");
	}

	/**
	 * Returns an estimate of the amount of live data in bytes.
	 */
	public void enableEstimateLiveDataSize() {
		this.properties.add("rocksdb.estimate-live-data-size");
	}

	/**
	 * Returns total size (bytes) of all SST files.
	 * <strong>WARNING</strong>: may slow down online queries if there are too many files.
	 */
	public void enableTotalSstFilesSize() {
		this.properties.add("rocksdb.total-sst-files-size");
	}

	/**
	 * Returns total size (bytes) of all SST files belong to the latest LSM tree.
	 */
	public void enableLiveSstFilesSize() {
		this.properties.add("rocksdb.live-sst-files-size");
	}

	/**
	 * Returns number of level to which L0 data will be compacted.
	 */
	public void enableBaseLevel() {
		this.properties.add("rocksdb.base-level");
	}

	/**
	 * Returns estimated total number of bytes compaction needs to rewrite to get all levels down
	 * to under target size. Not valid for other compactions than level-based.
	 */
	public void enableEstimatePendingCompactionBytes() {
		this.properties.add("rocksdb.estimate-pending-compaction-bytes");
	}

	/**
	 * Returns the number of currently running compactions.
	 */
	public void enableNumRunningCompactions() {
		this.properties.add("rocksdb.num-running-compactions");
	}

	/**
	 * Returns the number of currently running flushes.
	 */
	public void enableNumRunningFlushes() {
		this.properties.add("rocksdb.num-running-flushes");
	}

	/**
	 * Returns the current actual delayed write rate. 0 means no delay.
	 */
	public void enableActualDelayedWriteRate() {
		this.properties.add("rocksdb.actual-delayed-write-rate");
	}

	/**
	 * Returns 1 if write has been stopped.
	 */
	public void enableIsWriteStopped() {
		this.properties.add("rocksdb.is-write-stopped");
	}

	/**
	 * Returns block cache capacity (bytes).
	 */
	public void enableBlockCacheCapacity() {
		this.properties.add("rocksdb.block-cache-capacity");
	}

	/**
	 * Returns the memory size for the entries residing in block cache (bytes).
	 */
	public void enableBlockCacheUsage() {
		this.properties.add("rocksdb.block-cache-usage");
	}

	/**
	 * Returns the memory size for the entries being pinned.
	 */
	public void enableBlockCachePinnedUsage() {
		this.properties.add("rocksdb.block-cache-pinned-usage");
	}

	/**
	 * Set the refresh frequency from RocksDB metrics, default 10 seconds.
	 * @param frequency refresh frequency in milliesconds.
	 */
	public void setFrequency(long frequency) {
		this.frequency = frequency;
	}

	/**
	 * @return the metric refresh frequency in milliseconds.
	 */
	public long getFrequency() {
		return frequency;
	}

	/**
	 * @return the enabled RocksDB metrics
	 */
	public Collection<String> getProperties() {
		return Collections.unmodifiableCollection(properties);
	}

	/**
	 * {{RocksDBNativeMetricMonitor}} is enabled is any property is set.
	 *
	 * @return true if {{RocksDBNativeMetricMonitor}} should be enabled, false otherwise.
	 */
	public boolean isEnabled() {
		return !properties.isEmpty();
	}
}
