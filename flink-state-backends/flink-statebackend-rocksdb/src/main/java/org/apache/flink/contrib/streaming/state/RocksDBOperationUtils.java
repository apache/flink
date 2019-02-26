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
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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

	public static ColumnFamilyDescriptor createColumnFamilyDescriptor(String stateName, ColumnFamilyOptions columnOptions) {
		byte[] nameBytes = stateName.getBytes(ConfigConstants.DEFAULT_CHARSET);
		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
			"The chosen state name 'default' collides with the name of the default column family!");

		return new ColumnFamilyDescriptor(nameBytes, columnOptions);
	}

	public static ColumnFamilyHandle createColumnFamily(ColumnFamilyDescriptor columnDescriptor, RocksDB db) {
		try {
			return db.createColumnFamily(columnDescriptor);
		} catch (RocksDBException e) {
			IOUtils.closeQuietly(columnDescriptor.getOptions());
			throw new FlinkRuntimeException("Error creating ColumnFamilyHandle.", e);
		}
	}

	public static ColumnFamilyHandle createColumnFamily(
		String stateName,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		RocksDB db) {
		ColumnFamilyOptions options = createColumnFamilyOptions(columnFamilyOptionsFactory, stateName);
		return createColumnFamily(createColumnFamilyDescriptor(stateName, options), db);
	}

	public static RocksIteratorWrapper getRocksIterator(RocksDB db) {
		return new RocksIteratorWrapper(db.newIterator());
	}

	public static RocksIteratorWrapper getRocksIterator(
		RocksDB db,
		ColumnFamilyHandle columnFamilyHandle) {
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
	 */
	public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
		RegisteredStateMetaInfoBase metaInfoBase,
		RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
		TtlTimeProvider ttlTimeProvider,
		RocksDB db,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory) {
		ColumnFamilyOptions options = createColumnFamilyOptions(columnFamilyOptionsFactory, metaInfoBase.getName());
		ttlCompactFiltersManager.setAndRegisterCompactFilterIfStateTtl(ttlTimeProvider, metaInfoBase, options);
		ColumnFamilyDescriptor columnFamilyDescriptor = createColumnFamilyDescriptor(metaInfoBase.getName(), options);
		return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(createColumnFamily(columnFamilyDescriptor, db), metaInfoBase);
	}

	public static ColumnFamilyOptions createColumnFamilyOptions(
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		String stateName) {
		// ensure that we use the right merge operator, because other code relies on this
		return columnFamilyOptionsFactory.apply(stateName).setMergeOperatorName(MERGE_OPERATOR_NAME);
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
}
