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

package org.apache.flink.state.forst;

import org.apache.flink.core.state.InternalStateFuture;

import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;
import org.forstdb.RocksIterator;

import java.io.IOException;

import static org.apache.flink.state.forst.ForStDBIterRequest.startWithKeyPrefix;

/**
 * The Map#isEmpty() and Map#contains() request for ForStDB.
 *
 * @param <K> The type of key in map check request.
 * @param <N> The type of namespace in map check request.
 * @param <V> The type of value in map check request.
 */
public class ForStDBMapCheckRequest<K, N, V> extends ForStDBGetRequest<K, N, V, Boolean> {

    private static final byte[] VALID_PLACEHOLDER = new byte[0];

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    /** Check if the map is empty. */
    private final boolean checkEmpty;

    public ForStDBMapCheckRequest(
            ContextKey<K, N> key,
            ForStInnerTable<K, N, V> table,
            InternalStateFuture<Boolean> future,
            boolean checkEmpty) {
        super(key, table, future);
        this.keyGroupPrefixBytes = ((ForStMapState) table).getKeyGroupPrefixBytes();
        this.checkEmpty = checkEmpty;
    }

    @Override
    public void process(RocksDB db) throws RocksDBException, IOException {
        byte[] key = buildSerializedKey();
        if (checkEmpty) {
            try (RocksIterator iter = db.newIterator(getColumnFamilyHandle())) {
                iter.seek(key);
                if (iter.isValid() && startWithKeyPrefix(key, iter.key(), keyGroupPrefixBytes)) {
                    completeStateFuture(VALID_PLACEHOLDER);
                } else {
                    completeStateFuture(null);
                }
            }
        } else {
            byte[] value = db.get(getColumnFamilyHandle(), key);
            completeStateFuture(value);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void completeStateFuture(byte[] bytesValue) {
        if (checkEmpty) {
            future.complete(bytesValue == null);
            return;
        }
        future.complete(bytesValue != null);
    }
}
