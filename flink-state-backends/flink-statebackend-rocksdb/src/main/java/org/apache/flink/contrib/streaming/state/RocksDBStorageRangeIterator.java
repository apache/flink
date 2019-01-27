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

/**
 * An helper class returns an iterator which key locates in the given key range.
 */
public class RocksDBStorageRangeIterator implements StorageIterator<byte[], byte[]> {
	private final RocksDBStorageInstance dbStorageInstance;
	private final AbstractRocksDbStorageIterator<RocksDBPair> rangeIterator;

	public RocksDBStorageRangeIterator(
		RocksDBStorageInstance dbStorageInstance,
		final byte[] keyPrefixStart,
		final byte[] keyPrefixEnd) {
		Preconditions.checkState(compare(keyPrefixStart, keyPrefixEnd) <= 0);
		this.dbStorageInstance = dbStorageInstance;
		this.rangeIterator = new AbstractRocksDbStorageIterator<RocksDBPair>(dbStorageInstance) {
			@Override
			byte[] getStartDBKey() {
				return keyPrefixStart;
			}

			@Override
			boolean isEndDBKey(byte[] dbKey) {
				return compare(dbKey, keyPrefixEnd) >= 0;
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
	public boolean hasNext() {
		return rangeIterator.hasNext();
	}

	@Override
	public Pair<byte[], byte[]> next() {
		return rangeIterator.next();
	}

	private int compare(byte[] leftBytes, byte[] rightBytes) {
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

