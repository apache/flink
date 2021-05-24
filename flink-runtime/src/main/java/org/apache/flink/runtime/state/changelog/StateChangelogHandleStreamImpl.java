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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** {@link StateChangelogHandle} implementation based on {@link StreamStateHandle}. */
@Internal
public final class StateChangelogHandleStreamImpl
        implements StateChangelogHandle<StateChangelogHandleStreamImpl.StateChangeStreamReader> {
    private static final long serialVersionUID = -8070326169926626355L;

    private final KeyGroupRange keyGroupRange;
    /** NOTE: order is important as it reflects the order of changes. */
    private final List<Tuple2<StreamStateHandle, Long>> handlesAndOffsets;

    private transient SharedStateRegistry stateRegistry;

    public StateChangelogHandleStreamImpl(
            List<Tuple2<StreamStateHandle, Long>> handlesAndOffsets, KeyGroupRange keyGroupRange) {
        this.handlesAndOffsets = handlesAndOffsets;
        this.keyGroupRange = keyGroupRange;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry) {
        this.stateRegistry = stateRegistry;
        handlesAndOffsets.forEach(
                handleAndOffset ->
                        stateRegistry.registerReference(
                                getKey(handleAndOffset.f0), handleAndOffset.f0));
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        KeyGroupRange offsets = keyGroupRange.getIntersection(keyGroupRange);
        if (offsets.getNumberOfKeyGroups() == 0) {
            return null;
        }
        return new StateChangelogHandleStreamImpl(handlesAndOffsets, offsets);
    }

    @Override
    public CloseableIterator<StateChange> getChanges(StateChangeStreamReader reader) {
        return new CloseableIterator<StateChange>() {
            private final Iterator<Tuple2<StreamStateHandle, Long>> handleIterator =
                    handlesAndOffsets.iterator();

            private CloseableIterator<StateChange> current = CloseableIterator.empty();

            @Override
            public boolean hasNext() {
                advance();
                return current.hasNext();
            }

            @Override
            public StateChange next() {
                advance();
                return current.next();
            }

            private void advance() {
                while (!current.hasNext() && handleIterator.hasNext()) {
                    Tuple2<StreamStateHandle, Long> tuple2 = handleIterator.next();
                    try {
                        current = reader.read(tuple2.f0, tuple2.f1);
                    } catch (IOException e) {
                        ExceptionUtils.rethrow(e);
                    }
                }
            }

            @Override
            public void close() throws Exception {
                current.close();
            }
        };
    }

    @Override
    public void discardState() {
        handlesAndOffsets.forEach(
                handleAndOffset -> stateRegistry.unregisterReference(getKey(handleAndOffset.f0)));
    }

    @Override
    public long getStateSize() {
        return 0;
    }

    private static SharedStateRegistryKey getKey(StreamStateHandle stateHandle) {
        // StateHandle key used in SharedStateRegistry should only be based on the file name
        // and not on backend UUID or keygroup (multiple handles can refer to the same file and
        // making keys unique will effectively disable sharing)
        if (stateHandle instanceof FileStateHandle) {
            return new SharedStateRegistryKey(
                    ((FileStateHandle) stateHandle).getFilePath().toString());
        } else if (stateHandle instanceof ByteStreamStateHandle) {
            return new SharedStateRegistryKey(
                    ((ByteStreamStateHandle) stateHandle).getHandleName());
        } else {
            return new SharedStateRegistryKey(
                    Integer.toString(System.identityHashCode(stateHandle)));
        }
    }

    public interface StateChangeStreamReader {
        CloseableIterator<StateChange> read(StreamStateHandle handle, long offset)
                throws IOException;
    }
}
