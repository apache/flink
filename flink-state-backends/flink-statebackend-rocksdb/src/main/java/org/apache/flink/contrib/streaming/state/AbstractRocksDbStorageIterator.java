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

import org.apache.flink.util.Preconditions;

import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A helper class for iterating over entries backed in rocksDB.
 */
abstract class AbstractRocksDbStorageIterator<T> implements Iterator<T> {
	private static final int CACHE_SIZE_LIMIT = 128;

	/**
	 *  The cached rocksDB entries for this iterator to improve the read performance, the cache size is 128.
	 *
	 *  <p>When this {@link AbstractRocksDbStorageIterator} is created, it would load 128 elements into this {@code cachePairs}.
	 */
	private final List<RocksDBPair> cachePairs;

	/** The rocksDB storage where entries are located. */
	private RocksDBStorageInstance dbStorageInstance;

	/** The entry pointing to the current position which is last returned by calling {@link #getNextPair()}. */
	private RocksDBPair currentPair;

	private boolean expired;

	private int cacheIndex;

	AbstractRocksDbStorageIterator(RocksDBStorageInstance dbStorageInstance) {
		this.dbStorageInstance = dbStorageInstance;
		this.cachePairs = new ArrayList<>();
		this.expired = false;
		this.cacheIndex = 0;
	}

	/**
	 * Get the start-key bytes, which used to seek for iterator.
	 */
	abstract byte[] getStartDBKey();

	/**
	 * Check whether the key-bytes is the end for this {@link AbstractRocksDbStorageIterator}.
	 *
	 * @param dbKey The key-bytes to check.
	 */
	abstract boolean isEndDBKey(byte[] dbKey);

	@Override
	public boolean hasNext() {
		loadCachePairs();
		return (cacheIndex < cachePairs.size());
	}

	@Override
	public void remove() {
		if (currentPair == null || currentPair.isDeleted()) {
			throw new IllegalStateException("The remove operation must be called after a valid next operation.");
		}

		currentPair.remove();
	}

	final RocksDBPair getNextPair() {
		loadCachePairs();

		if (cacheIndex == cachePairs.size()) {
			Preconditions.checkState(expired);
			throw new NoSuchElementException();
		}

		this.currentPair = cachePairs.get(cacheIndex);
		cacheIndex++;

		return currentPair;
	}

	/**
	 * Load rocksDB entries into {@link AbstractRocksDbStorageIterator#cachePairs} using {@link RocksIterator}.
	 */
	private void loadCachePairs() {
		Preconditions.checkState(cacheIndex <= cachePairs.size());

		// Load cache entries only when cache is empty and there still exist unread entries
		if (cacheIndex < cachePairs.size() || expired) {
			return;
		}

		try (RocksIterator iterator = dbStorageInstance.iterator()) {
			/*
			 * The iteration starts from the prefix bytes at the first loading. After #nextEntry() is called,
			 * the currentPair points to the last returned entry, and at that time, we will start
			 * the iterating from currentPair if reloading cache is needed.
			 */
			byte[] startRocksKey = (currentPair == null ? getStartDBKey() : currentPair.getKey());

			cachePairs.clear();
			cacheIndex = 0;

			if (startRocksKey == null) {
				iterator.seekToFirst();
			} else {
				iterator.seek(startRocksKey);
			}

			/*
			 * If the last returned entry is not deleted, it will be the first
			 * entry in the iterating. Skip it to avoid redundant access in such
			 * cases.
			 */
			if (currentPair != null && !currentPair.isDeleted()) {
				iterator.next();
			}

			while (true) {
				if (!iterator.isValid() || isEndDBKey(iterator.key())) {
					expired = true;
					break;
				}

				if (cachePairs.size() >= CACHE_SIZE_LIMIT) {
					break;
				}

				RocksDBPair entry = new RocksDBPair(dbStorageInstance, iterator.key(), iterator.value());
				cachePairs.add(entry);

				iterator.next();
			}
		}
	}
}

