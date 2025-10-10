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

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.rocksdb.RocksDBIncrementalCheckpointUtils;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.types.Either;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ExportImportFilesMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class for distributing state handle data during RocksDB incremental restore. This class
 * encapsulates the logic for processing a single state handle.
 */
public class DistributeStateHandlerHelper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributeStateHandlerHelper.class);

    private final IncrementalLocalKeyedStateHandle stateHandle;
    private final RestoredDBInstance restoredDbInstance;
    private final int keyGroupPrefixBytes;
    private final KeyGroupRange keyGroupRange;
    private final String operatorIdentifier;
    private final int index;

    /**
     * Creates a helper for processing a single state handle. The database instance is created in
     * the constructor to enable proper resource management and separation of concerns.
     *
     * @param stateHandle the state handle to process
     * @param columnFamilyOptionsFactory factory for creating column family options
     * @param dbOptions database options
     * @param ttlCompactFiltersManager TTL compact filters manager (can be null)
     * @param writeBufferManagerCapacity write buffer manager capacity (can be null)
     * @param keyGroupPrefixBytes number of key group prefix bytes for SST file range checking
     * @param keyGroupRange target key group range (for logging)
     * @param operatorIdentifier operator identifier (for logging)
     * @param index current processing index (for logging)
     * @throws Exception on any database opening error
     */
    public DistributeStateHandlerHelper(
            IncrementalLocalKeyedStateHandle stateHandle,
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            DBOptions dbOptions,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            Long writeBufferManagerCapacity,
            int keyGroupPrefixBytes,
            KeyGroupRange keyGroupRange,
            String operatorIdentifier,
            int index)
            throws Exception {
        this.stateHandle = stateHandle;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.keyGroupRange = keyGroupRange;
        this.operatorIdentifier = operatorIdentifier;
        this.index = index;

        final String logLineSuffix = createLogLineSuffix();

        LOG.debug("Opening temporary database : {}", logLineSuffix);

        // Open database using restored instance helper method
        this.restoredDbInstance =
                RestoredDBInstance.restoreTempDBInstanceFromLocalState(
                        stateHandle,
                        stateMetaInfoSnapshots,
                        columnFamilyOptionsFactory,
                        dbOptions,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);
    }

    /**
     * Distributes state handle data by checking SST file ranges and exporting column families.
     * Returns Left if successfully exported, Right if the handle was skipped.
     *
     * @param exportCfBasePath base path for export
     * @param exportedColumnFamiliesOut output parameter for exported column families
     * @return Either.Left containing key group range if successfully exported, Either.Right
     *     containing the skipped state handle otherwise
     * @throws Exception on any export error
     */
    public Either<KeyGroupRange, IncrementalLocalKeyedStateHandle> tryDistribute(
            Path exportCfBasePath,
            Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>>
                    exportedColumnFamiliesOut)
            throws Exception {

        final String logLineSuffix = createLogLineSuffix();

        List<ColumnFamilyHandle> tmpColumnFamilyHandles = restoredDbInstance.columnFamilyHandles;

        LOG.debug("Checking actual keys of sst files {}", logLineSuffix);

        // Check SST file range
        RocksDBIncrementalCheckpointUtils.RangeCheckResult rangeCheckResult =
                RocksDBIncrementalCheckpointUtils.checkSstDataAgainstKeyGroupRange(
                        restoredDbInstance.db, keyGroupPrefixBytes, stateHandle.getKeyGroupRange());

        LOG.info("{} {}", rangeCheckResult, logLineSuffix);

        if (rangeCheckResult.allInRange()) {
            LOG.debug("Start exporting {}", logLineSuffix);

            List<RegisteredStateMetaInfoBase> registeredStateMetaInfoBases =
                    restoredDbInstance.stateMetaInfoSnapshots.stream()
                            .map(RegisteredStateMetaInfoBase::fromMetaInfoSnapshot)
                            .collect(Collectors.toList());

            // Export all the Column Families and store the result in exportedColumnFamiliesOut
            RocksDBIncrementalCheckpointUtils.exportColumnFamilies(
                    restoredDbInstance.db,
                    tmpColumnFamilyHandles,
                    registeredStateMetaInfoBases,
                    exportCfBasePath,
                    exportedColumnFamiliesOut);

            LOG.debug("Done exporting {}", logLineSuffix);
            return Either.Left(stateHandle.getKeyGroupRange());
        } else {
            LOG.debug("Skipped export {}", logLineSuffix);
            return Either.Right(stateHandle);
        }
    }

    @Override
    public void close() throws Exception {
        restoredDbInstance.close();
    }

    /** Creates a consistent log line suffix for logging operations. */
    private String createLogLineSuffix() {
        return " for state handle at index "
                + index
                + " with proclaimed key-group range "
                + stateHandle.getKeyGroupRange().prettyPrintInterval()
                + " for backend with range "
                + keyGroupRange.prettyPrintInterval()
                + " in operator "
                + operatorIdentifier
                + ".";
    }
}
