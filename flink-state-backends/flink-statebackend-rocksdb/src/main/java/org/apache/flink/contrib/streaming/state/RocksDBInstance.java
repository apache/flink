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

import org.apache.flink.runtime.state.StateAccessException;
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
 * A DB instance wrapper of {@link RocksDB}.
 */
public class RocksDBInstance implements AutoCloseable {

	/**
	 * Our RocksDB database, this is used to store state.
	 * The different k/v states that we have don't each have their own RocksDB instance.
	 */
	private RocksDB db;

	/** The write options to use in the states. We disable write ahead logging. */
	private final WriteOptions writeOptions;

	private final ColumnFamilyHandle columnFamilyHandle;

	/**
	 * Creates a rocksDB instance with given options, ttlSeconds and the instance path for rocksDB.
	 */
	RocksDBInstance(RocksDB db, ColumnFamilyHandle handle) {
		Preconditions.checkNotNull(db);
		Preconditions.checkNotNull(handle);

		this.writeOptions = new WriteOptions().setDisableWAL(true);
		this.db = db;
		this.columnFamilyHandle = handle;
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(columnFamilyHandle);
		IOUtils.closeQuietly(writeOptions);
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
			db.delete(writeOptions, keyBytes);
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	void merge(byte[] keyBytes, byte[] partialValueBytes) {
		try {
			db.merge(writeOptions, keyBytes, partialValueBytes);
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	RocksIterator iterator() {
		return db.newIterator();
	}

	void snapshot(String localCheckpointPath) throws RocksDBException {
		Checkpoint checkpoint = Checkpoint.create(db);
		checkpoint.createCheckpoint(localCheckpointPath);
	}

	RocksDB getDb() {
		return db;
	}

	WriteOptions getWriteOptions() {
		return writeOptions;
	}

	public ColumnFamilyHandle getColumnFamilyHandle() {
		return columnFamilyHandle;
	}

//--------------------------------------------------------------------------

	/**
	 * Check whether the given bytes is prefixed with prefiBytes.
	 *
	 * @param bytes The given bytes to compare.
	 * @param prefixBytes The target prefix bytes.
	 */
	public static boolean isPrefixWith(byte[] bytes, byte[] prefixBytes) {
		Preconditions.checkArgument(bytes != null);
		Preconditions.checkArgument(prefixBytes != null);

		if (bytes.length < prefixBytes.length) {
			return false;
		}

		for (int i = 0; i < prefixBytes.length; ++i) {
			if (bytes[i] != prefixBytes[i]) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Compares two given bytes array for order. Returns a negative integer,
	 * zero, or a positive integer as the first bytes array is less than, equal
	 * to, or greater than the second one.
	 *
	 * @param leftBytes The first bytes array to compare.
	 * @param rightBytes The second bytes array to compare.
	 */
	static int compare(byte[] leftBytes, byte[] rightBytes) {
		Preconditions.checkArgument(leftBytes != null);
		Preconditions.checkArgument(rightBytes != null);

		int commonLength = Math.min(leftBytes.length, rightBytes.length);
		for (int i = 0; i < commonLength; ++i) {
			int leftByte = leftBytes[i] & 0xFF;
			int rightByte = rightBytes[i] & 0xFF;

			if (leftByte > rightByte) {
				return 1;
			} else if (leftByte < rightByte) {
				return -1;
			}
		}

		return (leftBytes.length - rightBytes.length);
	}
}

