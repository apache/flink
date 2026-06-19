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

package org.apache.flink.state.rocksdb.iterator;

import org.apache.flink.state.rocksdb.RocksIteratorWrapper;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nonnull;

/**
 * Wraps a RocksDB iterator to cache it's current key and assigns an id for the key/value state to
 * the iterator. Used by {@link RocksStatesPerKeyGroupMergeIterator}.
 */
class RocksSingleStateIterator implements SingleStateIterator {

    /**
     * @param iterator underlying {@link RocksIteratorWrapper}
     * @param kvStateId Id of the K/V state to which this iterator belongs.
     */
    RocksSingleStateIterator(@Nonnull RocksIteratorWrapper iterator, int kvStateId) {
        this.iterator = iterator;
        this.currentKey = iterator.key();
        this.kvStateId = kvStateId;
    }

    @Nonnull private final RocksIteratorWrapper iterator;
    private byte[] currentKey;
    private final int kvStateId;

    @Override
    public void next() {
        iterator.next();
        if (iterator.isValid()) {
            currentKey = iterator.key();
        }
    }

    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return iterator.value();
    }

    @Override
    public int getKvStateId() {
        return kvStateId;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(iterator);
    }
}
