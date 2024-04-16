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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * {@link InternalSyncState} interface for partitioned value state.
 *
 * <p>In addition to providing an ordinary single record access interface, the {@code
 * InternalSyncValueState} also provides batch access interfaces for performance optimization.
 *
 * @param <K> The type of key the state is associated to
 * @param <V> The type of the value in the state.
 */
@Internal
public interface InternalSyncValueState<K, V> extends InternalSyncState<K> {

    /**
     * Put the <key, value> pair into the state.
     *
     * @param key The key of the kv pair
     * @param value The new value of the kv pair.
     * @throws IOException Thrown when the performed I/O operation fails.
     */
    void put(K key, V value) throws IOException;

    /**
     * Returns the value corresponding to the given key.
     *
     * @param key The key to lookup.
     * @return The value associated with the given key, or null, if no value is found for the key.
     * @throws IOException Thrown when the performed I/O operation fails.
     */
    V get(K key) throws IOException;

    /**
     * Write a batch of key-value pairs into state.
     *
     * @param batch The key-value data to be written.
     * @throws IOException Thrown when the performed I/O operation fails.
     */
    void writeBatch(List<Tuple2<K, V>> batch) throws IOException;

    /**
     * Get the values corresponding to a batch of keys from the state. This interface should work
     * with {@link InternalSyncState#isSupportMultiGet()}, which indicates whether the state
     * supports the multiGet API.
     *
     * @param keys A batch of keys to be queried
     * @return The corresponding values.
     * @throws IOException Thrown when the performed I/O operation fails.
     */
    List<V> multiGet(List<K> keys) throws IOException;
}
