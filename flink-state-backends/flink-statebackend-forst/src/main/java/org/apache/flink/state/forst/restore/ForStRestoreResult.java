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

package org.apache.flink.state.forst.restore;

import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.state.forst.ForStNativeMetricMonitor;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.RocksDB;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.SortedMap;
import java.util.UUID;

/** Entity holding result of ForSt instance restore. */
public class ForStRestoreResult {
    private final RocksDB db;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;
    @Nullable private final ForStNativeMetricMonitor nativeMetricMonitor;

    // fields only for incremental restore
    private final long lastCompletedCheckpointId;
    private final UUID backendUID;
    private final SortedMap<Long, Collection<HandleAndLocalPath>> restoredSstFiles;

    public ForStRestoreResult(
            RocksDB db,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            @Nullable ForStNativeMetricMonitor nativeMetricMonitor,
            long lastCompletedCheckpointId,
            UUID backendUID,
            SortedMap<Long, Collection<HandleAndLocalPath>> restoredSstFiles) {
        this.db = db;
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.nativeMetricMonitor = nativeMetricMonitor;
        this.lastCompletedCheckpointId = lastCompletedCheckpointId;
        this.backendUID = backendUID;
        this.restoredSstFiles = restoredSstFiles;
    }

    public RocksDB getDb() {
        return db;
    }

    public long getLastCompletedCheckpointId() {
        return lastCompletedCheckpointId;
    }

    public UUID getBackendUID() {
        return backendUID;
    }

    public SortedMap<Long, Collection<HandleAndLocalPath>> getRestoredSstFiles() {
        return restoredSstFiles;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    @Nullable
    public ForStNativeMetricMonitor getNativeMetricMonitor() {
        return nativeMetricMonitor;
    }
}
