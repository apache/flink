/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.rocksdb.RocksDBOperationUtils;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Restored DB instance containing all necessary handles and metadata. */
public class RestoredDBInstance implements AutoCloseable {

    @Nonnull public final RocksDB db;
    @Nonnull public final ColumnFamilyHandle defaultColumnFamilyHandle;
    @Nonnull public final List<ColumnFamilyHandle> columnFamilyHandles;
    @Nonnull public final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    @Nonnull public final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
    public final ReadOptions readOptions;
    public final IncrementalLocalKeyedStateHandle srcStateHandle;

    public RestoredDBInstance(
            @Nonnull RocksDB db,
            @Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
            @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            IncrementalLocalKeyedStateHandle srcStateHandle) {
        this.db = db;
        this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
        this.columnFamilyHandles = columnFamilyHandles;
        this.columnFamilyDescriptors = columnFamilyDescriptors;
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        this.readOptions = new ReadOptions();
        this.srcStateHandle = srcStateHandle;
    }

    @Override
    public void close() {
        List<ColumnFamilyOptions> columnFamilyOptions =
                new ArrayList<>(columnFamilyDescriptors.size() + 1);
        columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
        RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                columnFamilyOptions, defaultColumnFamilyHandle);
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeAllQuietly(columnFamilyHandles);
        IOUtils.closeQuietly(db);
        IOUtils.closeAllQuietly(columnFamilyOptions);
        IOUtils.closeQuietly(readOptions);
    }

    /**
     * Restores a RocksDB instance from local state for the given state handle.
     *
     * @param stateHandle the state handle to restore from
     * @param columnFamilyOptionsFactory factory for creating column family options
     * @param dbOptions database options
     * @param ttlCompactFiltersManager TTL compact filters manager (can be null)
     * @param writeBufferManagerCapacity write buffer manager capacity (can be null)
     * @return restored DB instance with all necessary handles and metadata
     * @throws Exception on any restore error
     */
    public static RestoredDBInstance restoreTempDBInstanceFromLocalState(
            IncrementalLocalKeyedStateHandle stateHandle,
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            DBOptions dbOptions,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            Long writeBufferManagerCapacity)
            throws Exception {

        Function<String, ColumnFamilyOptions> tempDBCfFactory =
                stateName ->
                        columnFamilyOptionsFactory.apply(stateName).setDisableAutoCompactions(true);

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                createColumnFamilyDescriptors(
                        stateMetaInfoSnapshots,
                        tempDBCfFactory,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity,
                        false);

        Path restoreSourcePath = stateHandle.getDirectoryStateHandle().getDirectory();

        List<ColumnFamilyHandle> columnFamilyHandles =
                new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

        RocksDB db =
                RocksDBOperationUtils.openDB(
                        restoreSourcePath.toString(),
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        RocksDBOperationUtils.createColumnFamilyOptions(tempDBCfFactory, "default"),
                        dbOptions);

        return new RestoredDBInstance(
                db,
                columnFamilyHandles,
                columnFamilyDescriptors,
                stateMetaInfoSnapshots,
                stateHandle);
    }

    /**
     * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state
     * metadata snapshot.
     */
    public static List<ColumnFamilyDescriptor> createColumnFamilyDescriptors(
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            Long writeBufferManagerCapacity,
            boolean registerTtlCompactFilter) {

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>(stateMetaInfoSnapshots.size());

        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
            RegisteredStateMetaInfoBase metaInfoBase =
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);

            ColumnFamilyDescriptor columnFamilyDescriptor =
                    RocksDBOperationUtils.createColumnFamilyDescriptor(
                            metaInfoBase,
                            columnFamilyOptionsFactory,
                            registerTtlCompactFilter ? ttlCompactFiltersManager : null,
                            writeBufferManagerCapacity);

            columnFamilyDescriptors.add(columnFamilyDescriptor);
        }
        return columnFamilyDescriptors;
    }
}
