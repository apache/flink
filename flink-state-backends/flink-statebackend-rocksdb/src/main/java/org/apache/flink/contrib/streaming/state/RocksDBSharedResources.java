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

import org.rocksdb.Cache;
import org.rocksdb.WriteBufferManager;

/**
 * The set of resources that can be shared by all RocksDB instances in a slot.
 * Sharing these resources helps RocksDB a predictable resource footprint.
 */
final class RocksDBSharedResources implements AutoCloseable {

	private final Cache cache;

	private final WriteBufferManager writeBufferManager;
	private final long writeBufferManagerCapacity;

	RocksDBSharedResources(Cache cache, WriteBufferManager writeBufferManager, long writeBufferManagerCapacity) {
		this.cache = cache;
		this.writeBufferManager = writeBufferManager;
		this.writeBufferManagerCapacity = writeBufferManagerCapacity;
	}

	public Cache getCache() {
		return cache;
	}

	public WriteBufferManager getWriteBufferManager() {
		return writeBufferManager;
	}

	public long getWriteBufferManagerCapacity() {
		return writeBufferManagerCapacity;
	}

	@Override
	public void close() {
		writeBufferManager.close();
		cache.close();
	}
}
