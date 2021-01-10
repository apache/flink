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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME;

/** Utils for RocksDB Operations. */
public class RocksDBOperationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBOperationUtils.class);

    private static final String MANAGED_MEMORY_RESOURCE_ID = "state-rocks-managed-memory";

    private static final String FIXED_SLOT_MEMORY_RESOURCE_ID = "state-rocks-fixed-slot-memory";

    public static RocksDB openDB(
            String path,
            List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
            List<ColumnFamilyHandle> stateColumnFamilyHandles,
            ColumnFamilyOptions columnFamilyOptions,
            DBOptions dbOptions)
            throws IOException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
        columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

        RocksDB dbRef;

        try {
            dbRef =
                    RocksDB.open(
                            Preconditions.checkNotNull(dbOptions),
                            Preconditions.checkNotNull(path),
                            columnFamilyDescriptors,
                            stateColumnFamilyHandles);
        } catch (RocksDBException e) {
            IOUtils.closeQuietly(columnFamilyOptions);
            columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));

            // improve error reporting on Windows
            throwExceptionIfPathLengthExceededOnWindows(path, e);

            throw new IOException("Error while opening RocksDB instance.", e);
        }

        // requested + default CF
        Preconditions.checkState(
                1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
                "Not all requested column family handles have been created");
        return dbRef;
    }

    public static RocksIteratorWrapper getRocksIterator(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, ReadOptions readOptions) {
        return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions));
    }

    /**
     * Create a total order read option to avoid user misuse, see FLINK-17800 for more details.
     *
     * <p>Note, remember to close the generated {@link ReadOptions} when dispose.
     */
    // TODO We would remove this method once we bump RocksDB version larger than 6.2.2.
    public static ReadOptions createTotalOrderSeekReadOptions() {
        return new ReadOptions().setTotalOrderSeek(true);
    }

    public static void registerKvStateInformation(
            Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            RocksDBNativeMetricMonitor nativeMetricMonitor,
            String columnFamilyName,
            RocksDBKeyedStateBackend.RocksDbKvStateInfo registeredColumn) {

        kvStateInformation.put(columnFamilyName, registeredColumn);
        if (nativeMetricMonitor != null) {
            nativeMetricMonitor.registerColumnFamily(
                    columnFamilyName, registeredColumn.columnFamilyHandle);
        }
    }

    /**
     * Creates a state info from a new meta info to use with a k/v state.
     *
     * <p>Creates the column family for the state. Sets TTL compaction filter if {@code
     * ttlCompactFiltersManager} is not {@code null}.
     */
    public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
            RegisteredStateMetaInfoBase metaInfoBase,
            RocksDB db,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity) {

        ColumnFamilyDescriptor columnFamilyDescriptor =
                createColumnFamilyDescriptor(
                        metaInfoBase,
                        columnFamilyOptionsFactory,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);
        return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(
                createColumnFamily(columnFamilyDescriptor, db), metaInfoBase);
    }

    /**
     * Creates a column descriptor for a state column family.
     *
     * <p>Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
     */
    public static ColumnFamilyDescriptor createColumnFamilyDescriptor(
            RegisteredStateMetaInfoBase metaInfoBase,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity) {

        ColumnFamilyOptions options =
                createColumnFamilyOptions(columnFamilyOptionsFactory, metaInfoBase.getName());
        if (ttlCompactFiltersManager != null) {
            ttlCompactFiltersManager.setAndRegisterCompactFilterIfStateTtl(metaInfoBase, options);
        }
        byte[] nameBytes = metaInfoBase.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
        Preconditions.checkState(
                !Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
                "The chosen state name 'default' collides with the name of the default column family!");

        if (writeBufferManagerCapacity != null) {
            // It'd be great to perform the check earlier, e.g. when creating write buffer manager.
            // Unfortunately the check needs write buffer size that was just calculated.
            sanityCheckArenaBlockSize(
                    options.writeBufferSize(),
                    options.arenaBlockSize(),
                    writeBufferManagerCapacity);
        }

        return new ColumnFamilyDescriptor(nameBytes, options);
    }

    /**
     * Logs a warning if the arena block size is too high causing RocksDB to flush constantly.
     * Essentially, the condition <a
     * href="https://github.com/dataArtisans/frocksdb/blob/49bc897d5d768026f1eb816d960c1f2383396ef4/include/rocksdb/write_buffer_manager.h#L47">
     * here</a> will always be true.
     *
     * @param writeBufferSize the size of write buffer (bytes)
     * @param arenaBlockSizeConfigured the manually configured arena block size, zero or less means
     *     not configured
     * @param writeBufferManagerCapacity the size of the write buffer manager (bytes)
     * @return true if sanity check passes, false otherwise
     */
    static boolean sanityCheckArenaBlockSize(
            long writeBufferSize, long arenaBlockSizeConfigured, long writeBufferManagerCapacity)
            throws IllegalStateException {

        long defaultArenaBlockSize =
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(writeBufferSize);
        long arenaBlockSize =
                arenaBlockSizeConfigured <= 0 ? defaultArenaBlockSize : arenaBlockSizeConfigured;
        long mutableLimit =
                RocksDBMemoryControllerUtils.calculateRocksDBMutableLimit(
                        writeBufferManagerCapacity);
        if (RocksDBMemoryControllerUtils.validateArenaBlockSize(arenaBlockSize, mutableLimit)) {
            return true;
        } else {
            LOG.warn(
                    "RocksDBStateBackend performance will be poor because of the current Flink memory configuration! "
                            + "RocksDB will flush memtable constantly, causing high IO and CPU. "
                            + "Typically the easiest fix is to increase task manager managed memory size. "
                            + "If running locally, see the parameter taskmanager.memory.managed.size. "
                            + "Details: arenaBlockSize {} > mutableLimit {} (writeBufferSize = {}, arenaBlockSizeConfigured = {},"
                            + " defaultArenaBlockSize = {}, writeBufferManagerCapacity = {})",
                    arenaBlockSize,
                    mutableLimit,
                    writeBufferSize,
                    arenaBlockSizeConfigured,
                    defaultArenaBlockSize,
                    writeBufferManagerCapacity);
            return false;
        }
    }

    public static ColumnFamilyOptions createColumnFamilyOptions(
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory, String stateName) {

        // ensure that we use the right merge operator, because other code relies on this
        return columnFamilyOptionsFactory
                .apply(stateName)
                .setMergeOperatorName(MERGE_OPERATOR_NAME);
    }

    private static ColumnFamilyHandle createColumnFamily(
            ColumnFamilyDescriptor columnDescriptor, RocksDB db) {
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
            double memoryFraction,
            Logger logger)
            throws IOException {

        if (!memoryConfig.isUsingFixedMemoryPerSlot() && !memoryConfig.isUsingManagedMemory()) {
            return null;
        }

        final double highPriorityPoolRatio = memoryConfig.getHighPriorityPoolRatio();
        final double writeBufferRatio = memoryConfig.getWriteBufferRatio();

        final LongFunctionWithException<RocksDBSharedResources, Exception> allocator =
                (size) ->
                        RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                                size, writeBufferRatio, highPriorityPoolRatio);

        try {
            if (memoryConfig.isUsingFixedMemoryPerSlot()) {
                assert memoryConfig.getFixedMemoryPerSlot() != null;

                logger.info("Getting fixed-size shared cache for RocksDB.");
                return memoryManager.getExternalSharedMemoryResource(
                        FIXED_SLOT_MEMORY_RESOURCE_ID,
                        allocator,
                        memoryConfig.getFixedMemoryPerSlot().getBytes());
            } else {
                logger.info("Getting managed memory shared cache for RocksDB.");
                return memoryManager.getSharedMemoryResourceForManagedMemory(
                        MANAGED_MEMORY_RESOURCE_ID, allocator, memoryFraction);
            }
        } catch (Exception e) {
            throw new IOException("Failed to acquire shared cache resource for RocksDB", e);
        }
    }

    private static void throwExceptionIfPathLengthExceededOnWindows(String path, Exception cause)
            throws IOException {
        // max directory path length on Windows is 247.
        // the maximum path length is 260, subtracting one file name length (12 chars) and one NULL
        // terminator.
        final int maxWinDirPathLen = 247;

        if (path.length() > maxWinDirPathLen && OperatingSystem.isWindows()) {
            throw new IOException(
                    String.format(
                            "The directory path length (%d) is longer than the directory path length limit for Windows (%d): %s",
                            path.length(), maxWinDirPathLen, path),
                    cause);
        }
    }
}
