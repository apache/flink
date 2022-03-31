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

package org.apache.flink.state.hashmap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.state.hashmap.RemovalLog.StateEntryRemoval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Writes out {@link IncrementalCopyOnWriteStateMapSnapshot}.
 *
 * <p>Entry iterator size is not known in advance so it "encodes" {@link Iterator#hasNext()} by
 * writing a boolean before each entry.
 *
 * @see IncrementalKeyGroupReader
 */
class IncrementalKeyGroupWriter<K, N, S> {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalKeyGroupWriter.class);

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;

    IncrementalKeyGroupWriter(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer) {
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.stateSerializer = stateSerializer;
    }

    void write(
            DataOutputView dov,
            Iterator<StateEntry<K, N, S>> entries,
            Set<StateEntryRemoval<K, N>> removals)
            throws IOException {
        writeEntries(dov, removals, entries);
        writeRemoved(dov, removals);
    }

    private void writeEntries(
            DataOutputView dov,
            Set<StateEntryRemoval<K, N>> removals,
            Iterator<StateEntry<K, N, S>> entries)
            throws IOException {
        int numEntries = 0;
        while (entries.hasNext()) {
            dov.writeBoolean(true);
            StateEntry<K, N, S> entry = entries.next();
            namespaceSerializer.serialize(entry.getNamespace(), dov);
            keySerializer.serialize(entry.getKey(), dov);
            stateSerializer.serialize(entry.getState(), dov);
            removals.remove(StateEntryRemoval.of(entry.getKey(), entry.getNamespace()));
            numEntries++;
        }
        dov.writeBoolean(false);
        LOG.trace("written {} entries", numEntries);
    }

    private void writeRemoved(DataOutputView dov, Set<StateEntryRemoval<K, N>> removals)
            throws IOException {
        dov.writeInt(removals.size());
        for (StateEntryRemoval<K, N> entry : removals) {
            keySerializer.serialize(entry.getKey(), dov);
            namespaceSerializer.serialize(entry.getNamespace(), dov);
        }
        LOG.trace("written {} removals", removals.size());
    }
}
