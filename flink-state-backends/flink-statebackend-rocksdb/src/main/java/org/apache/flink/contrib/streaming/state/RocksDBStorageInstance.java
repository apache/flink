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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.BatchPutWrapper;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StorageInstance;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.util.Map;

/**
 * An implementation of {@link StorageInstance} which used {@code RocksDB} to store data.
 */
public class RocksDBStorageInstance implements StorageInstance, AutoCloseable {
	static final String SST_FILE_SUFFIX = ".sst";

	/**
	 * Our RocksDB database, this is used to store state.
	 * The different k/v states that we have will have their own RocksDB instance and columnFamilyHandle.
	 */
	private final RocksDB db;

	private final ColumnFamilyHandle columnFamilyHandle;

	/** The write options to use in the states. We disable write ahead logging. */
	private final WriteOptions writeOptions;

	/**
	 * Create a RocksDBStorageInstance with the given {@link RocksDB} and {@link ColumnFamilyHandle} instance.
	 * @param db The RocksDB instance the storage used.
	 * @param columnFamilyHandle The ColumnFamilyHandle the storage used.
	 */
	public RocksDBStorageInstance(
		final RocksDB db,
		final ColumnFamilyHandle columnFamilyHandle,
		WriteOptions writeOptions) {
		this.db = Preconditions.checkNotNull(db);
		this.columnFamilyHandle = Preconditions.checkNotNull(columnFamilyHandle);
		this.writeOptions = Preconditions.checkNotNull(writeOptions);
	}

	public ColumnFamilyHandle getColumnFamilyHandle() {
		return this.columnFamilyHandle;
	}

	RocksDB getDb() {
		return db;
	}

	byte[] get(byte[] keyBytes) {
		try {
			return db.get(columnFamilyHandle, keyBytes);
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	void put(byte[] keyBytes, byte[] valueBytes) {
		try {
			db.put(columnFamilyHandle, writeOptions, keyBytes, valueBytes);
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	void multiPut(Map<byte[], byte[]> keyValueBytesMap) {
		try (RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(db, writeOptions)) {
			for (Map.Entry<byte[], byte[]> entry : keyValueBytesMap.entrySet()) {
				writeBatchWrapper.put(columnFamilyHandle, entry.getKey(), entry.getValue());
			}
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	void delete(byte[] keyBytes) {
		try {
			db.delete(columnFamilyHandle, writeOptions, keyBytes);
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	void merge(byte[] keyBytes, byte[] partialValueBytes) {
		try {
			db.merge(columnFamilyHandle, writeOptions, keyBytes, partialValueBytes);
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	RocksIterator iterator() {
		return db.newIterator(columnFamilyHandle);
	}

	void snapshot(String localCheckpointPath) throws RocksDBException {
		Checkpoint checkpoint = Checkpoint.create(db);
		checkpoint.createCheckpoint(localCheckpointPath);
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(columnFamilyHandle);
	}

	@Override
	public BatchPutWrapper getBatchPutWrapper() {
		return new RocksDBBatchPutWrapper(
					new RocksDBWriteBatchWrapper(db, writeOptions),
					columnFamilyHandle);
	}
}
