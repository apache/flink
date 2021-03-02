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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamImpl;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Comparator.comparing;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Serialization format for state changes. */
@Internal
public class StateChangeFormat implements StateChangelogHandleStreamImpl.StateChangeStreamReader {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFormat.class);
    private static final int VERSION = 1;

    Map<StateChangeSet, Long> write(FSDataOutputStream os, Collection<StateChangeSet> changeSets)
            throws IOException {
        // todo in MVP or later: use bucketing instead of sorting (O(n) instead of O(nlogn))
        List<StateChangeSet> sorted = new ArrayList<>(changeSets);
        sorted.sort(
                comparing(StateChangeSet::getLogId)
                        .thenComparing(StateChangeSet::getSequenceNumber));
        DataOutputViewStreamWrapper dataOutput = new DataOutputViewStreamWrapper(os);
        Map<StateChangeSet, Long> pendingResults = new HashMap<>();
        dataOutput.writeInt(VERSION);
        for (StateChangeSet changeSet : sorted) {
            pendingResults.put(changeSet, os.getPos());
            writeChangeSet(changeSet, dataOutput);
        }
        return pendingResults;
    }

    private void writeChangeSet(StateChangeSet changeSet, DataOutputViewStreamWrapper output)
            throws IOException {
        output.writeInt(changeSet.getChanges().size());
        for (StateChange stateChange : changeSet.getChanges()) {
            output.writeInt(stateChange.getKeyGroup()); // todo in MVP: write once
            output.writeInt(stateChange.getChange().length);
            output.write(stateChange.getChange());
        }
    }

    @Override
    public CloseableIterator<StateChange> read(StreamStateHandle handle, long offset)
            throws IOException {
        FSDataInputStream stream = handle.openInputStream();
        DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(stream);
        checkArgument(input.readInt() == VERSION);
        if (stream.getPos() != offset) {
            stream.seek(offset);
        }
        return new CloseableIterator<StateChange>() {
            int numLeft = input.readInt();

            @Override
            public boolean hasNext() {
                return numLeft > 0;
            }

            @Override
            public StateChange next() {
                if (numLeft == 0) {
                    throw new NoSuchElementException();
                }
                numLeft--;
                try {
                    return readChange();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private StateChange readChange() throws IOException {
                int keyGroup = input.readInt();
                int size = input.readInt();
                byte[] bytes = new byte[size];
                checkState(size == input.read(bytes));
                return new StateChange(keyGroup, bytes);
            }

            @Override
            public void close() throws Exception {
                stream.close();
            }
        };
    }
}
