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

import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

/**
 * A helper class for RocksDB Pair.
 */
public class RocksDBPair implements Pair<byte[], byte[]> {

	/** The rocksDB storage of current pair. */
	private final RocksDBStorageInstance storageInstance;

	/** Current db key of the pair.*/
	private final byte[] key;

	/** Current db value of the pair.*/
	private byte[] value;

	/** Is the current pair is deleted.*/
	private boolean deleted;

	RocksDBPair(RocksDBStorageInstance storageInstance, byte[] key, byte[] value) {
		this.storageInstance = Preconditions.checkNotNull(storageInstance);
		this.key = Preconditions.checkNotNull(key);
		this.value = Preconditions.checkNotNull(value);
		this.deleted = false;
	}

	@Override
	public byte[] getKey() {
		return key;
	}

	@Override
	public byte[] getValue() {
		return value;
	}

	void remove() {
		Preconditions.checkState(!deleted);

		deleted = true;
		value = null;
		storageInstance.delete(key);
	}

	boolean isDeleted() {
		return deleted;
	}

	@Override
	public byte[] setValue(byte[] value) {
		if (deleted) {
			throw new IllegalStateException("Set value on deleted key.");
		}

		byte[] oldValue = this.value;
		storageInstance.put(key, value);
		this.value = value;
		return oldValue;
	}
}

