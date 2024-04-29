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

import org.apache.flink.runtime.asyncprocessing.StateRequest;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

/**
 * The concept of an abstracted table oriented towards ForStDB, and each ForStInnerTable can be
 * mapped to a ForSt internal State.
 *
 * <p>The mapping between ForStInnerTable and ForStDB's columnFamily can be one-to-one or
 * many-to-one.
 *
 * @param <K> The key type of the table.
 * @param <V> The value type of the table.
 */
public interface ForStInnerTable<K, V> {

    /** Get the columnFamily handle corresponding to table. */
    ColumnFamilyHandle getColumnFamilyHandle();

    /**
     * Serialize the given key to bytes.
     *
     * @param key the key to be serialized.
     * @return the key bytes
     * @throws IOException Thrown if the serialization encountered an I/O related error.
     */
    byte[] serializeKey(K key) throws IOException;

    /**
     * Serialize the given value to the outputView.
     *
     * @param value the value to be serialized.
     * @return the value bytes
     * @throws IOException Thrown if the serialization encountered an I/O related error.
     */
    byte[] serializeValue(V value) throws IOException;

    /**
     * Deserialize the given bytes value to POJO value.
     *
     * @param value the value bytes to be deserialized.
     * @return the deserialized POJO value
     * @throws IOException Thrown if the deserialization encountered an I/O related error.
     */
    V deserializeValue(byte[] value) throws IOException;

    /**
     * Build a {@link ForStDBGetRequest} that belong to this {@code ForStInnerTable} with the given
     * stateRequest.
     *
     * @param stateRequest The given stateRequest.
     * @return The corresponding ForSt GetRequest.
     */
    ForStDBGetRequest<K, V> buildDBGetRequest(StateRequest<?, ?, ?> stateRequest);

    /**
     * Build a {@link ForStDBPutRequest} that belong to {@code ForStInnerTable} with the given
     * stateRequest.
     *
     * @param stateRequest The given stateRequest.
     * @return The corresponding ForSt PutRequest.
     */
    ForStDBPutRequest<K, V> buildDBPutRequest(StateRequest<?, ?, ?> stateRequest);
}
