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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * An iterator for reading all keys in a state backend across multiple partitioned states.
 *
 * <p>To read unique keys across all partitioned states callers must invoke {@link
 * MultiStateKeyIterator#remove}.
 *
 * @param <K> Type of the key by which state is keyed.
 */
@Internal
public final class MultiStateKeyIterator<K> implements CloseableIterator<K> {
    private final List<? extends StateDescriptor<?, ?>> descriptors;

    private final KeyedStateBackend<K> backend;

    /** Avoids using Stream#flatMap due to a known flaw, see FLINK-26585 for more details. */
    private final Iterator<? extends StateDescriptor<?, ?>> outerIter;

    private Iterator<K> innerIter;

    private final CloseableRegistry registry;

    private K currentKey;

    public MultiStateKeyIterator(
            List<? extends StateDescriptor<?, ?>> descriptors, KeyedStateBackend<K> backend) {

        this.descriptors = Preconditions.checkNotNull(descriptors);
        this.backend = Preconditions.checkNotNull(backend);

        outerIter = this.descriptors.iterator();
        innerIter = null;

        this.registry = new CloseableRegistry();
    }

    @Override
    public boolean hasNext() {
        while (innerIter == null || !innerIter.hasNext()) {
            if (!outerIter.hasNext()) {
                return false;
            }

            StateDescriptor<?, ?> descriptor = outerIter.next();
            Stream<K> stream = backend.getKeys(descriptor.getName(), VoidNamespace.INSTANCE);
            innerIter = stream.iterator();
            try {
                registry.registerCloseable(stream::close);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read keys from configured StateBackend", e);
            }
        }
        return true;
    }

    @Override
    public K next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        } else {
            currentKey = this.innerIter.next();
            return currentKey;
        }
    }

    /** Removes the current key from <b>ALL</b> known states in the state backend. */
    @Override
    public void remove() {
        if (currentKey == null) {
            return;
        }

        for (StateDescriptor<?, ?> descriptor : descriptors) {
            try {
                State state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                descriptor);

                state.clear();
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to drop partitioned state from state backend", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        registry.close();
    }
}
