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

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Base implementation of RocksDB restore operation.
 *
 * @param <K> The data type that the serializer serializes.
 */
public abstract class AbstractRocksDBRestoreOperation<K> implements RocksDBRestoreOperation, AutoCloseable {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final KeyGroupRange keyGroupRange;
	protected final int keyGroupPrefixBytes;
	protected final int numberOfTransferringThreads;
	protected final CloseableRegistry cancelStreamRegistry;
	protected final ClassLoader userCodeClassLoader;
	protected final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;
	protected final DBOptions dbOptions;
	protected final Map<String, RocksDbKvStateInfo> kvStateInformation;
	protected final File instanceBasePath;
	protected final File instanceRocksDBPath;
	protected final String dbPath;
	protected List<ColumnFamilyHandle> columnFamilyHandles;
	protected List<ColumnFamilyDescriptor> columnFamilyDescriptors;
	protected final StateSerializerProvider<K> keySerializerProvider;
	protected final RocksDBNativeMetricOptions nativeMetricOptions;
	protected final MetricGroup metricGroup;
	protected final Collection<KeyedStateHandle> restoreStateHandles;
	// Current places to set compact filter into column family options:
	// - Incremental restore
	//   - restore with rescaling
	//     - init from a certain sst: #createAndRegisterColumnFamilyDescriptors when prepare files, before db open
	//     - data ingestion after db open: #getOrRegisterStateColumnFamilyHandle before creating column family
	//   - restore without rescaling: #createAndRegisterColumnFamilyDescriptors when prepare files, before db open
	// - Full restore
	//   - data ingestion after db open: #getOrRegisterStateColumnFamilyHandle before creating column family
	protected final RocksDbTtlCompactFiltersManager ttlCompactFiltersManager;

	protected RocksDB db;
	protected ColumnFamilyHandle defaultColumnFamilyHandle;
	protected RocksDBNativeMetricMonitor nativeMetricMonitor;
	protected boolean isKeySerializerCompatibilityChecked;
	protected final Long writeBufferManagerCapacity;

	protected AbstractRocksDBRestoreOperation(
		KeyGroupRange keyGroupRange,
		int keyGroupPrefixBytes,
		int numberOfTransferringThreads,
		CloseableRegistry cancelStreamRegistry,
		ClassLoader userCodeClassLoader,
		Map<String, RocksDbKvStateInfo> kvStateInformation,
		StateSerializerProvider<K> keySerializerProvider,
		File instanceBasePath,
		File instanceRocksDBPath,
		DBOptions dbOptions,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		RocksDBNativeMetricOptions nativeMetricOptions,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		@Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
		Long writeBufferManagerCapacity) {
		this.keyGroupRange = keyGroupRange;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.numberOfTransferringThreads = numberOfTransferringThreads;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.userCodeClassLoader = userCodeClassLoader;
		this.kvStateInformation = kvStateInformation;
		this.keySerializerProvider = keySerializerProvider;
		this.instanceBasePath = instanceBasePath;
		this.instanceRocksDBPath = instanceRocksDBPath;
		this.dbPath = instanceRocksDBPath.getAbsolutePath();
		this.dbOptions = dbOptions;
		this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
		this.nativeMetricOptions = nativeMetricOptions;
		this.metricGroup = metricGroup;
		this.restoreStateHandles = stateHandles;
		this.ttlCompactFiltersManager = ttlCompactFiltersManager;
		this.columnFamilyHandles = new ArrayList<>(1);
		this.columnFamilyDescriptors = Collections.emptyList();
		this.writeBufferManagerCapacity = writeBufferManagerCapacity;
	}

	void openDB() throws IOException {
		db = RocksDBOperationUtils.openDB(
			dbPath,
			columnFamilyDescriptors,
			columnFamilyHandles,
			RocksDBOperationUtils.createColumnFamilyOptions(columnFamilyOptionsFactory, "default"),
			dbOptions);
		// remove the default column family which is located at the first index
		defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
		// init native metrics monitor if configured
		nativeMetricMonitor = nativeMetricOptions.isEnabled() ?
			new RocksDBNativeMetricMonitor(nativeMetricOptions, metricGroup, db) : null;
	}

	public RocksDB getDb() {
		return this.db;
	}

	RocksDbKvStateInfo getOrRegisterStateColumnFamilyHandle(
		ColumnFamilyHandle columnFamilyHandle,
		StateMetaInfoSnapshot stateMetaInfoSnapshot) {

		RocksDbKvStateInfo registeredStateMetaInfoEntry =
			kvStateInformation.get(stateMetaInfoSnapshot.getName());

		if (null == registeredStateMetaInfoEntry) {
			// create a meta info for the state on restore;
			// this allows us to retain the state in future snapshots even if it wasn't accessed
			RegisteredStateMetaInfoBase stateMetaInfo =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
			if (columnFamilyHandle == null) {
				registeredStateMetaInfoEntry = RocksDBOperationUtils.createStateInfo(
					stateMetaInfo, db, columnFamilyOptionsFactory, ttlCompactFiltersManager, writeBufferManagerCapacity);
			} else {
				registeredStateMetaInfoEntry = new RocksDbKvStateInfo(columnFamilyHandle, stateMetaInfo);
			}

			RocksDBOperationUtils.registerKvStateInformation(
				kvStateInformation,
				nativeMetricMonitor,
				stateMetaInfoSnapshot.getName(),
				registeredStateMetaInfoEntry);
		} else {
			// TODO with eager state registration in place, check here for serializer migration strategies
		}

		return registeredStateMetaInfoEntry;
	}

	KeyedBackendSerializationProxy<K> readMetaData(DataInputView dataInputView)
		throws IOException, StateMigrationException {
		// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
		// deserialization of state happens lazily during runtime; we depend on the fact
		// that the new serializer for states could be compatible, and therefore the restore can continue
		// without old serializers required to be present.
		KeyedBackendSerializationProxy<K> serializationProxy =
			new KeyedBackendSerializationProxy<>(userCodeClassLoader);
		serializationProxy.read(dataInputView);
		if (!isKeySerializerCompatibilityChecked) {
			// fetch current serializer now because if it is incompatible, we can't access
			// it anymore to improve the error message
			TypeSerializer<K> currentSerializer =
				keySerializerProvider.currentSchemaSerializer();
			// check for key serializer compatibility; this also reconfigures the
			// key serializer to be compatible, if it is required and is possible
			TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
				keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(serializationProxy.getKeySerializerSnapshot());
			if (keySerializerSchemaCompat.isCompatibleAfterMigration() || keySerializerSchemaCompat.isIncompatible()) {
				throw new StateMigrationException("The new key serializer (" + currentSerializer + ") must be compatible with the previous key serializer (" + keySerializerProvider.previousSchemaSerializer() + ").");
			}

			isKeySerializerCompatibilityChecked = true;
		}

		return serializationProxy;
	}

	/**
	 * Necessary clean up iff restore operation failed.
	 */
	@Override
	public void close() {
		IOUtils.closeQuietly(defaultColumnFamilyHandle);
		IOUtils.closeQuietly(nativeMetricMonitor);
		IOUtils.closeQuietly(db);
		// Making sure the already created column family options will be closed
		columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));
	}

	@Override
	public abstract RocksDBRestoreResult restore() throws Exception;
}
