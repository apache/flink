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

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;

import java.util.Iterator;
import java.util.NoSuchElementException;

class IncrementalCopyOnWriteStateMapEntryIterator<K, N, S>
        implements Iterator<StateEntry<K, N, S>> {
    private final CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData;
    private final int minVersion;
    private CopyOnWriteStateMap.StateMapEntry<K, N, S> nextEntry;
    private int nextBucket = 0;
    private final StateSnapshotTransformer<S> transformer;

    IncrementalCopyOnWriteStateMapEntryIterator(
            CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData,
            int minVersion,
            StateSnapshotTransformer<S> stateSnapshotTransformer) {
        this.snapshotData = snapshotData;
        this.minVersion = minVersion;
        this.transformer = stateSnapshotTransformer;
    }

    @Override
    public boolean hasNext() {
        advanceGlobally();
        return nextEntry != null;
    }

    @Override
    public CopyOnWriteStateMap.StateMapEntry<K, N, S> next() {
        advanceGlobally();
        if (nextEntry == null) {
            throw new NoSuchElementException();
        }
        CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = nextEntry;
        nextEntry = nextEntry.next();
        return entry;
    }

    private void advanceGlobally() {
        advanceInChain();
        while (nextEntry == null && nextBucket < snapshotData.length) {
            nextEntry = snapshotData[nextBucket];
            advanceInChain();
            nextBucket++;
        }
    }

    private void advanceInChain() {
        while (nextEntry != null
                && (wasSnapshotted()
                        || filterOrTransformNextEntry() == null)) { // todo: check entryVersion too?
            nextEntry = nextEntry == null ? null : nextEntry.next(); // can be null after filtering
        }
    }

    private boolean wasSnapshotted() {
        return nextEntry.getStateVersion() < minVersion && nextEntry.getEntryVersion() < minVersion;
    }

    private CopyOnWriteStateMap.StateMapEntry<K, N, S> filterOrTransformNextEntry() {
        if (transformer != null && nextEntry != null) {
            S newValue = transformer.filterOrTransform(nextEntry.getState());
            if (newValue == null) {
                nextEntry = null;
            } else if (newValue != nextEntry.getState()) {
                nextEntry =
                        new CopyOnWriteStateMap.StateMapEntry<>(
                                nextEntry, nextEntry.getEntryVersion());
            }
        }
        return nextEntry;
    }
}
