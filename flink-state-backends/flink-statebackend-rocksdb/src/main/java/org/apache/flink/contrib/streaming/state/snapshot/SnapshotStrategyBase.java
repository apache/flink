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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.LinkedHashMap;

/**
 * Base class for {@link SnapshotStrategy} implementations on RocksDB.
 *
 * @param <K> type of the backend keys.
 */
public abstract class SnapshotStrategyBase<K> implements SnapshotStrategy<SnapshotResult<KeyedStateHandle>> {

	@Nonnull
	protected final RocksDB db;

	@Nonnull
	protected final ResourceGuard rocksDBResourceGuard;

	@Nonnull
	protected final TypeSerializer<K> keySerializer;

	@Nonnull
	protected final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> kvStateInformation;

	@Nonnull
	protected final KeyGroupRange keyGroupRange;

	@Nonnegative
	protected final int keyGroupPrefixBytes;

	@Nonnull
	protected final LocalRecoveryConfig localRecoveryConfig;

	@Nonnull
	protected final CloseableRegistry cancelStreamRegistry;

	public SnapshotStrategyBase(
		@Nonnull RocksDB db,
		@Nonnull ResourceGuard rocksDBResourceGuard,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> kvStateInformation,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnull LocalRecoveryConfig localRecoveryConfig,
		@Nonnull CloseableRegistry cancelStreamRegistry) {

		this.db = db;
		this.rocksDBResourceGuard = rocksDBResourceGuard;
		this.keySerializer = keySerializer;
		this.kvStateInformation = kvStateInformation;
		this.keyGroupRange = keyGroupRange;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.localRecoveryConfig = localRecoveryConfig;
		this.cancelStreamRegistry = cancelStreamRegistry;
	}
}
