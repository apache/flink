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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Implementation of {@link BatchPutWrapper} for {@link RocksDBStateStorage}.
 */
class RocksDBBatchPutWrapper implements BatchPutWrapper<byte[], byte[]> {
	/** The write batch wrapper of RocksDB. */
	private final RocksDBWriteBatchWrapper batchWrapper;

	/** The columnFamilyHandle where the record will inserted into. */
	private final ColumnFamilyHandle columnFamilyHandle;

	RocksDBBatchPutWrapper(
		RocksDBWriteBatchWrapper writeBatchWrapper,
		ColumnFamilyHandle handle) {
		this.batchWrapper = writeBatchWrapper;
		this.columnFamilyHandle = handle;
	}

	@Override
	public void put(byte[] key, byte[] value) {
		try {
			batchWrapper.put(columnFamilyHandle, key, value);
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void close() {
		try {
			batchWrapper.close();
		} catch (RocksDBException e) {
			throw new StateAccessException(e);
		}
	}
}

