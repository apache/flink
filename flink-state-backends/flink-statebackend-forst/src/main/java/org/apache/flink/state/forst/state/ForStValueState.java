/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.v2.InternalSyncValueState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.List;

/**
 * {@link InternalSyncValueState} implementation that stores state in ForStDB.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class ForStValueState<K, V> extends AbstractForStState<K, V>
        implements InternalSyncValueState<K, V> {

    private static final int PER_RECORD_ESTIMATE_BYTES = 100;

    public ForStValueState(
            RocksDB forstDB,
            WriteOptions writeOptions,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer,
            int maxParallelism) {
        super(forstDB, writeOptions, columnFamily, keySerializer, valueSerializer, maxParallelism);
    }

    @Override
    public void put(K key, V value) throws IOException {
        try {
            db.put(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyAndKeyGroup(key),
                    serializeValue(value));
        } catch (RocksDBException e) {
            throw new IOException("Error while adding data to ForStDB", e);
        }
    }

    @Override
    public V get(K key) throws IOException {
        try {
            byte[] valueBytes = db.get(columnFamily, serializeCurrentKeyAndKeyGroup(key));
            if (valueBytes == null) {
                return null;
            }
            return deserializeValue(valueBytes);
        } catch (RocksDBException e) {
            throw new IOException("Error while retrieving data from ForStDB.", e);
        }
    }

    @Override
    public void writeBatch(List<Tuple2<K, V>> batch) throws IOException {
        try (WriteBatch writeBatch = new WriteBatch(batch.size() * PER_RECORD_ESTIMATE_BYTES)) {
            for (Tuple2<K, V> keyValue : batch) {
                writeBatch.put(
                        columnFamily,
                        serializeCurrentKeyAndKeyGroup(keyValue.f0),
                        serializeValue(keyValue.f1));
            }
            db.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            throw new IOException("Error while adding data to ForStDB", e);
        }
    }

    @Override
    public List<V> multiGet(List<K> keys) throws IOException {
        throw new UnsupportedOperationException();
    }
}
