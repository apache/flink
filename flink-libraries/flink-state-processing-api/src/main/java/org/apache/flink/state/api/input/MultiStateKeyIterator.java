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
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An iterator for reading all keys in a state backend across multiple partitioned states.
 *
 * @param <K> Type of the key by which state is keyed.
 */
@Internal
public final class MultiStateKeyIterator<K> implements CloseableIterator<K> {
    private final List<? extends StateDescriptor<?, ?>> descriptors;

    private final Iterator<K> iterator;

    private final CloseableRegistry registry;

    public MultiStateKeyIterator(
            List<? extends StateDescriptor<?, ?>> descriptors, KeyedStateBackend<K> backend) {

        this.descriptors = Preconditions.checkNotNull(descriptors);
        Preconditions.checkNotNull(backend);
        registry = new CloseableRegistry();
        Stream<K> stream =
                backend.getKeys(
                        this.descriptors.stream()
                                .map(StateDescriptor::getName)
                                .collect(Collectors.toList()),
                        VoidNamespace.INSTANCE);
        try {
            registry.registerCloseable(stream::close);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read keys from configured StateBackend", e);
        }
        iterator = stream.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public K next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        } else {
            return iterator.next();
        }
    }

    @Override
    public void close() throws Exception {
        registry.close();
    }
}
