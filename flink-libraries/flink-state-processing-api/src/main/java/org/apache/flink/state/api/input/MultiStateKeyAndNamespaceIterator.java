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

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.state.api.input.operator.StateReaderOperator;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Reads all (key, namespace) pairs in a state backend across multiple namespaced (e.g.
 * window-scoped) states, as the deduplicated union of the pairs stored per given state name: a
 * namespace (e.g. a window) may have data in one state but not another, yet should still surface as
 * a single row.
 *
 * <p>The key-group is always reported as {@link StateReaderOperator#UNKNOWN_KEY_GROUP} since the
 * backend's per-namespace lookup API does not expose it.
 */
@Internal
public final class MultiStateKeyAndNamespaceIterator<K>
        implements CloseableIterator<Tuple3<K, Object, Integer>> {

    private final Iterator<Tuple3<K, Object, Integer>> iterator;

    private final CloseableRegistry registry;

    public MultiStateKeyAndNamespaceIterator(
            List<String> stateNames, KeyedStateBackend<K> backend) {
        Preconditions.checkNotNull(stateNames, "The state names must not be null");
        Preconditions.checkNotNull(backend, "The keyed state backend must not be null");

        registry = new CloseableRegistry();
        Stream<Tuple2<K, Object>> merged =
                stateNames.stream()
                        .map(name -> keysAndNamespaces(backend, name))
                        .reduce(Stream::concat)
                        .orElseGet(Stream::empty);

        iterator =
                merged.distinct()
                        .map(
                                t ->
                                        Tuple3.<K, Object, Integer>of(
                                                t.f0, t.f1, StateReaderOperator.UNKNOWN_KEY_GROUP))
                        .iterator();
    }

    private Stream<Tuple2<K, Object>> keysAndNamespaces(KeyedStateBackend<K> backend, String name) {
        Stream<Tuple2<K, Object>> stream = backend.getKeysAndNamespaces(name);
        try {
            registry.registerCloseable(stream::close);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read keys from configured StateBackend", e);
        }
        return stream;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Tuple3<K, Object, Integer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return iterator.next();
    }

    @Override
    public void close() throws Exception {
        registry.close();
    }
}
