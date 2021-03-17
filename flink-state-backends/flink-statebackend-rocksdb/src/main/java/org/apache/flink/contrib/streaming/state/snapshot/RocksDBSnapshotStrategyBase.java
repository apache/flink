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

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.LinkedHashMap;

/**
 * Abstract base class for {@link SnapshotStrategy} implementations for RocksDB state backend.
 *
 * @param <K> type of the backend keys.
 */
public abstract class RocksDBSnapshotStrategyBase<K, R extends SnapshotResources>
        implements CheckpointListener, SnapshotStrategy<KeyedStateHandle, R> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBSnapshotStrategyBase.class);

    @Nonnull private final String description;
    /** RocksDB instance from the backend. */
    @Nonnull protected RocksDB db;

    /** Resource guard for the RocksDB instance. */
    @Nonnull protected final ResourceGuard rocksDBResourceGuard;

    /** The key serializer of the backend. */
    @Nonnull protected final TypeSerializer<K> keySerializer;

    /** Key/Value state meta info from the backend. */
    @Nonnull protected final LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation;

    /** The key-group range for the task. */
    @Nonnull protected final KeyGroupRange keyGroupRange;

    /** Number of bytes in the key-group prefix. */
    @Nonnegative protected final int keyGroupPrefixBytes;

    /** The configuration for local recovery. */
    @Nonnull protected final LocalRecoveryConfig localRecoveryConfig;

    public RocksDBSnapshotStrategyBase(
            @Nonnull String description,
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig) {
        this.db = db;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        this.keySerializer = keySerializer;
        this.kvStateInformation = kvStateInformation;
        this.keyGroupRange = keyGroupRange;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.localRecoveryConfig = localRecoveryConfig;
        this.description = description;
    }

    @Nonnull
    public String getDescription() {
        return description;
    }
}
