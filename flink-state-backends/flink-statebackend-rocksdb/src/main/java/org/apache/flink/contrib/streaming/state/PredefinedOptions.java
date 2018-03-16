/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;

/**
 * The {@code PredefinedOptions} are configuration settings for the {@link RocksDBStateBackend}.
 * The various pre-defined choices are configurations that have been empirically
 * determined to be beneficial for performance under different settings.
 *
 * <p>Some of these settings are based on experiments by the Flink community, some follow
 * guides from the RocksDB project.
 */
public enum PredefinedOptions {

	/**
	 * Default options for all settings, except that writes are not forced to the
	 * disk.
	 *
	 * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery,
	 * there is no need to sync data to stable storage.
	 */
	DEFAULT {

		@Override
		public DBOptions createDBOptions() {
			return new DBOptions()
					.setUseFsync(false);
		}

		@Override
		public ColumnFamilyOptions createColumnOptions() {
			return new ColumnFamilyOptions();
		}

	},

	/**
	 * Pre-defined options for regular spinning hard disks.
	 *
	 * <p>This constant configures RocksDB with some options that lead empirically
	 * to better performance when the machines executing the system use
	 * regular spinning hard disks.
	 *
	 * <p>The following options are set:
	 * <ul>
	 *     <li>setCompactionStyle(CompactionStyle.LEVEL)</li>
	 *     <li>setLevelCompactionDynamicLevelBytes(true)</li>
	 *     <li>setIncreaseParallelism(4)</li>
	 *     <li>setUseFsync(false)</li>
	 *     <li>setDisableDataSync(true)</li>
	 *     <li>setMaxOpenFiles(-1)</li>
	 * </ul>
	 *
	 * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery,
	 * there is no need to sync data to stable storage.
	 */
	SPINNING_DISK_OPTIMIZED {

		@Override
		public DBOptions createDBOptions() {

			return new DBOptions()
					.setIncreaseParallelism(4)
					.setUseFsync(false)
					.setMaxOpenFiles(-1);
		}

		@Override
		public ColumnFamilyOptions createColumnOptions() {
			return new ColumnFamilyOptions()
					.setCompactionStyle(CompactionStyle.LEVEL)
					.setLevelCompactionDynamicLevelBytes(true);
		}
	},

	/**
	 * Pre-defined options for better performance on regular spinning hard disks,
	 * at the cost of a higher memory consumption.
	 *
	 * <p><b>NOTE: These settings will cause RocksDB to consume a lot of memory for
	 * block caching and compactions. If you experience out-of-memory problems related to,
	 * RocksDB, consider switching back to {@link #SPINNING_DISK_OPTIMIZED}.</b></p>
	 *
	 * <p>The following options are set:
	 * <ul>
	 *     <li>setLevelCompactionDynamicLevelBytes(true)</li>
	 *     <li>setTargetFileSizeBase(256 MBytes)</li>
	 *     <li>setMaxBytesForLevelBase(1 GByte)</li>
	 *     <li>setWriteBufferSize(64 MBytes)</li>
	 *     <li>setIncreaseParallelism(4)</li>
	 *     <li>setMinWriteBufferNumberToMerge(3)</li>
	 *     <li>setMaxWriteBufferNumber(4)</li>
	 *     <li>setUseFsync(false)</li>
	 *     <li>setMaxOpenFiles(-1)</li>
	 *     <li>BlockBasedTableConfig.setBlockCacheSize(256 MBytes)</li>
	 *     <li>BlockBasedTableConfigsetBlockSize(128 KBytes)</li>
	 * </ul>
	 *
	 * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery,
	 * there is no need to sync data to stable storage.
	 */
	SPINNING_DISK_OPTIMIZED_HIGH_MEM {

		@Override
		public DBOptions createDBOptions() {

			return new DBOptions()
					.setIncreaseParallelism(4)
					.setUseFsync(false)
					.setMaxOpenFiles(-1);
		}

		@Override
		public ColumnFamilyOptions createColumnOptions() {

			final long blockCacheSize = 256 * 1024 * 1024;
			final long blockSize = 128 * 1024;
			final long targetFileSize = 256 * 1024 * 1024;
			final long writeBufferSize = 64 * 1024 * 1024;

			return new ColumnFamilyOptions()
					.setCompactionStyle(CompactionStyle.LEVEL)
					.setLevelCompactionDynamicLevelBytes(true)
					.setTargetFileSizeBase(targetFileSize)
					.setMaxBytesForLevelBase(4 * targetFileSize)
					.setWriteBufferSize(writeBufferSize)
					.setMinWriteBufferNumberToMerge(3)
					.setMaxWriteBufferNumber(4)
					.setTableFormatConfig(
							new BlockBasedTableConfig()
									.setBlockCacheSize(blockCacheSize)
									.setBlockSize(blockSize)
									.setFilter(new BloomFilter())
					);
		}
	},

	/**
	 * Pre-defined options for Flash SSDs.
	 *
	 * <p>This constant configures RocksDB with some options that lead empirically
	 * to better performance when the machines executing the system use SSDs.
	 *
	 * <p>The following options are set:
	 * <ul>
	 *     <li>setIncreaseParallelism(4)</li>
	 *     <li>setUseFsync(false)</li>
	 *     <li>setDisableDataSync(true)</li>
	 *     <li>setMaxOpenFiles(-1)</li>
	 * </ul>
	 *
	 * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery,
	 * there is no need to sync data to stable storage.
	 */
	FLASH_SSD_OPTIMIZED {

		@Override
		public DBOptions createDBOptions() {
			return new DBOptions()
					.setIncreaseParallelism(4)
					.setUseFsync(false)
					.setMaxOpenFiles(-1);
		}

		@Override
		public ColumnFamilyOptions createColumnOptions() {
			return new ColumnFamilyOptions();
		}
	};

	// ------------------------------------------------------------------------

	/**
	 * Creates the {@link DBOptions}for this pre-defined setting.
	 *
	 * @return The pre-defined options object.
	 */
	public abstract DBOptions createDBOptions();

	/**
	 * Creates the {@link org.rocksdb.ColumnFamilyOptions}for this pre-defined setting.
	 *
	 * @return The pre-defined options object.
	 */
	public abstract ColumnFamilyOptions createColumnOptions();

}
