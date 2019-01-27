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

import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * A helper class returns an iterator with the given prefix.
 */
public class RocksDBStoragePrefixIterator implements StorageIterator<byte[], byte[]> {
	private final RocksDBStorageInstance dbStorageInstance;
	/**
	 * The prefixKye will be null if iterator from the first element from the storage.
	 */
	@Nullable
	private final byte[] prefixKey;
	private final AbstractRocksDbStorageIterator<RocksDBPair> prefixIterator;

	public RocksDBStoragePrefixIterator(RocksDBStorageInstance dbStorageInstance, byte[] prefixKey) {
		this.dbStorageInstance = dbStorageInstance;
		this.prefixKey = prefixKey;

		this.prefixIterator = new AbstractRocksDbStorageIterator<RocksDBPair>(dbStorageInstance) {
			@Override
			byte[] getStartDBKey() {
				return prefixKey;
			}

			@Override
			boolean isEndDBKey(byte[] dbKey) {
				return prefixKey != null && !isPrefixWith(dbKey);
			}

			@Override
			public RocksDBPair next() {
				return getNextPair();
			}
		};
	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void remove() {
		prefixIterator.remove();
	}

	@Override
	public boolean hasNext() {
		return prefixIterator.hasNext();
	}

	@Override
	public Pair<byte[], byte[]> next() {
		return prefixIterator.next();
	}

	private boolean isPrefixWith(byte[] bytes) {
		Preconditions.checkArgument(bytes != null);

		if (bytes.length < prefixKey.length) {
			return false;
		}

		for (int i = 0; i < prefixKey.length; ++i) {
			if (bytes[i] != prefixKey[i]) {
				return false;
			}
		}

		return true;
	}
}
