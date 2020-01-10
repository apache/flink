/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;

import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBufferManager;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME;

/**
 * Utils for RocksDB Operations.
 */
public class RocksDBOperationUtils {

	private static final String MANAGED_MEMORY_RESOURCE_ID = "state-rocks-managed-memory";

	private static final String FIXED_SLOT_MEMORY_RESOURCE_ID = "state-rocks-fixed-slot-memory";

	public static RocksDB openDB(
		String path,
		List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
		List<ColumnFamilyHandle> stateColumnFamilyHandles,
		ColumnFamilyOptions columnFamilyOptions,
		DBOptions dbOptions) throws IOException {
		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

		// we add the required descriptor for the default CF in FIRST position, see
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
		columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

		RocksDB dbRef;

		try {
			dbRef = RocksDB.open(
				Preconditions.checkNotNull(dbOptions),
				Preconditions.checkNotNull(path),
				columnFamilyDescriptors,
				stateColumnFamilyHandles);
		} catch (RocksDBException e) {
			IOUtils.closeQuietly(columnFamilyOptions);
			columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));
			throw new IOException("Error while opening RocksDB instance.", e);
		}

		// requested + default CF
		Preconditions.checkState(1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
			"Not all requested column family handles have been created");
		return dbRef;
	}

	public static RocksIteratorWrapper getRocksIterator(RocksDB db) {
		return new RocksIteratorWrapper(db.newIterator());
	}

	public static RocksIteratorWrapper getRocksIterator(RocksDB db, ColumnFamilyHandle columnFamilyHandle) {
		return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle));
	}

	public static void registerKvStateInformation(
		Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
		RocksDBNativeMetricMonitor nativeMetricMonitor,
		String columnFamilyName,
		RocksDBKeyedStateBackend.RocksDbKvStateInfo registeredColumn) {

		kvStateInformation.put(columnFamilyName, registeredColumn);
		if (nativeMetricMonitor != null) {
			nativeMetricMonitor.registerColumnFamily(columnFamilyName, registeredColumn.columnFamilyHandle);
		}
	}

	/**
	 * Creates a state info from a new meta info to use with a k/v state.
	 *
	 * <p>Creates the column family for the state.
	 * Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
	 */
	public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
		RegisteredStateMetaInfoBase metaInfoBase,
		RocksDB db,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		@Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {

		ColumnFamilyDescriptor columnFamilyDescriptor = createColumnFamilyDescriptor(
			metaInfoBase, columnFamilyOptionsFactory, ttlCompactFiltersManager);
		return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(createColumnFamily(columnFamilyDescriptor, db), metaInfoBase);
	}

	/**
	 * Creates a column descriptor for sate column family.
	 *
	 * <p>Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
	 */
	public static ColumnFamilyDescriptor createColumnFamilyDescriptor(
		RegisteredStateMetaInfoBase metaInfoBase,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		@Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {

		ColumnFamilyOptions options = createColumnFamilyOptions(columnFamilyOptionsFactory, metaInfoBase.getName());
		if (ttlCompactFiltersManager != null) {
			ttlCompactFiltersManager.setAndRegisterCompactFilterIfStateTtl(metaInfoBase, options);
		}
		byte[] nameBytes = metaInfoBase.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
			"The chosen state name 'default' collides with the name of the default column family!");

		return new ColumnFamilyDescriptor(nameBytes, options);
	}

	public static ColumnFamilyOptions createColumnFamilyOptions(
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory, String stateName) {

		// ensure that we use the right merge operator, because other code relies on this
		return columnFamilyOptionsFactory.apply(stateName).setMergeOperatorName(MERGE_OPERATOR_NAME);
	}

	private static ColumnFamilyHandle createColumnFamily(ColumnFamilyDescriptor columnDescriptor, RocksDB db) {
		try {
			return db.createColumnFamily(columnDescriptor);
		} catch (RocksDBException e) {
			IOUtils.closeQuietly(columnDescriptor.getOptions());
			throw new FlinkRuntimeException("Error creating ColumnFamilyHandle.", e);
		}
	}

	public static void addColumnFamilyOptionsToCloseLater(
		List<ColumnFamilyOptions> columnFamilyOptions, ColumnFamilyHandle columnFamilyHandle) {
		try {
			if (columnFamilyHandle != null && columnFamilyHandle.getDescriptor() != null) {
				columnFamilyOptions.add(columnFamilyHandle.getDescriptor().getOptions());
			}
		} catch (RocksDBException e) {
			// ignore
		}
	}

	@Nullable
	public static OpaqueMemoryResource<RocksDBSharedResources> allocateSharedCachesIfConfigured(
			RocksDBMemoryConfiguration memoryConfig,
			MemoryManager memoryManager,
			Logger logger) throws IOException {

		if (!memoryConfig.isUsingFixedMemoryPerSlot() && !memoryConfig.isUsingManagedMemory()) {
			return null;
		}

		final double highPriorityPoolRatio = memoryConfig.getHighPriorityPoolRatio();
		final double writeBufferRatio = memoryConfig.getWriteBufferRatio();

		final LongFunctionWithException<RocksDBSharedResources, Exception> allocator = (size) ->
			allocateRocksDBSharedResources(size, writeBufferRatio, highPriorityPoolRatio);

		try {
			if (memoryConfig.isUsingFixedMemoryPerSlot()) {
				assert memoryConfig.getFixedMemoryPerSlot() != null;

				logger.info("Getting fixed-size shared cache for RocksDB.");
				return memoryManager.getExternalSharedMemoryResource(
						FIXED_SLOT_MEMORY_RESOURCE_ID, allocator, memoryConfig.getFixedMemoryPerSlot().getBytes());
			}
			else {
				logger.info("Getting managed memory shared cache for RocksDB.");
				return memoryManager.getSharedMemoryResourceForManagedMemory(MANAGED_MEMORY_RESOURCE_ID, allocator);
			}
		}
		catch (Exception e) {
			throw new IOException("Failed to acquire shared cache resource for RocksDB", e);
		}
	}

	@VisibleForTesting
	static RocksDBSharedResources allocateRocksDBSharedResources(long memorySize, double writeBufferRatio, double highPriorityPoolRatio) {
		long calculatedCacheCapacity = calculateActualCacheCapacity(memorySize, writeBufferRatio);
		final Cache cache = createCache(calculatedCacheCapacity, highPriorityPoolRatio);
		long writeBufferManagerCapacity = calculateWriteBufferManagerCapacity(memorySize, writeBufferRatio);
		final WriteBufferManager wbm = new WriteBufferManager(writeBufferManagerCapacity, cache);
		return new RocksDBSharedResources(cache, wbm);
	}

	/**
	 * Calculate the actual calculated memory size of cache, which would be shared among rocksDB instance(s).
	 * We introduce this method because:
	 * a) We cannot create a strict capacity limit cache util FLINK-15532 resolved.
	 * b) Regardless of the memory usage of blocks pinned by RocksDB iterators,
	 * which is difficult to calculate and only happened when we iterator entries in RocksDBMapState, the overuse of memory is mainly occupied by at most half of the write buffer usage.
	 * (see <a href="https://github.com/dataArtisans/frocksdb/blob/958f191d3f7276ae59b270f9db8390034d549ee0/include/rocksdb/write_buffer_manager.h#L51">the flush implementation of write buffer manager</a>).
	 * Thus, we have four equations below:
	 *   write_buffer_manager_memory = 1.5 * write_buffer_manager_capacity
	 *   write_buffer_manager_memory = total_memory_size * write_buffer_ratio
	 *   write_buffer_manager_memory + other_part = total_memory_size
	 *   write_buffer_manager_capacity + other_part = cache_capacity
	 * And we would deduce the formula:
	 *   cache_capacity =  (3 - write_buffer_ratio) * total_memory_size / 3
	 *   write_buffer_manager_capacity = 2 * total_memory_size * write_buffer_ratio / 3
	 *
	 * @param totalMemorySize  Total off-heap memory size reserved for RocksDB instance(s).
	 * @param writeBufferRatio The ratio of memory size which could be reserved for write buffer manager to control the memory usage.
	 * @return The actual calculated memory size.
	 */
	@VisibleForTesting
	static long calculateActualCacheCapacity(long totalMemorySize, double writeBufferRatio) {
		return (long) ((3 - writeBufferRatio) * totalMemorySize / 3);
	}

	@VisibleForTesting
	static long calculateWriteBufferManagerCapacity(long totalMemorySize, double writeBufferRatio) {
		return (long) (2 * totalMemorySize * writeBufferRatio / 3);
	}

	@VisibleForTesting
	static Cache createCache(long cacheCapacity, double highPriorityPoolRatio) {
		// TODO use strict capacity limit until FLINK-15532 resolved
		return new LRUCache(cacheCapacity, -1, false, highPriorityPoolRatio);
	}
}
