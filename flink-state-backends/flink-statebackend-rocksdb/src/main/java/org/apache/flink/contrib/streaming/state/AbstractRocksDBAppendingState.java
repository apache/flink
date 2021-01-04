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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;

abstract class AbstractRocksDBAppendingState<K, N, IN, SV, OUT>
        extends AbstractRocksDBState<K, N, SV>
        implements InternalAppendingState<K, N, IN, SV, OUT> {

    /**
     * Creates a new RocksDB backend appending state.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    protected AbstractRocksDBAppendingState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> valueSerializer,
            SV defaultValue,
            RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
    }

    @Override
    public SV getInternal() {
        return getInternal(getKeyBytes());
    }

    SV getInternal(byte[] key) {
        try {
            byte[] valueBytes = backend.db.get(columnFamily, key);
            if (valueBytes == null) {
                return null;
            }
            dataInputView.setBuffer(valueBytes);
            return valueSerializer.deserialize(dataInputView);
        } catch (IOException | RocksDBException e) {
            throw new FlinkRuntimeException("Error while retrieving data from RocksDB", e);
        }
    }

    @Override
    public void updateInternal(SV valueToStore) {
        updateInternal(getKeyBytes(), valueToStore);
    }

    void updateInternal(byte[] key, SV valueToStore) {
        try {
            // write the new value to RocksDB
            backend.db.put(columnFamily, writeOptions, key, getValueBytes(valueToStore));
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException("Error while adding value to RocksDB", e);
        }
    }
}
