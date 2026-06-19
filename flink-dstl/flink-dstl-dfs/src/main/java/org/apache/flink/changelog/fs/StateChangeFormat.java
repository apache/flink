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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static org.apache.flink.runtime.state.changelog.StateChange.META_KEY_GROUP;

/** Serialization format for state changes. */
@Internal
public class StateChangeFormat {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFormat.class);

    Map<StateChangeSet, Tuple2<Long, Long>> write(
            OutputStreamWithPos os, Collection<StateChangeSet> changeSets) throws IOException {
        List<StateChangeSet> sorted = new ArrayList<>(changeSets);
        // using sorting instead of bucketing for simplicity
        sorted.sort(
                comparing(StateChangeSet::getLogId)
                        .thenComparing(StateChangeSet::getSequenceNumber));
        DataOutputViewStreamWrapper dataOutput = new DataOutputViewStreamWrapper(os);
        Map<StateChangeSet, Tuple2<Long, Long>> pendingResults = new HashMap<>();
        for (StateChangeSet changeSet : sorted) {
            long pos = os.getPos();
            pendingResults.put(changeSet, Tuple2.of(pos, pos));
            writeChangeSet(dataOutput, changeSet.getChanges());
        }
        return pendingResults;
    }

    private void writeChangeSet(DataOutputViewStreamWrapper output, List<StateChange> changes)
            throws IOException {
        // write in groups to output kg id only once
        Map<Integer, List<StateChange>> byKeyGroup =
                changes.stream().collect(Collectors.groupingBy(StateChange::getKeyGroup));
        // write the number of key groups
        output.writeInt(byKeyGroup.size());
        // output metadata first (see StateChange.META_KEY_GROUP)
        List<StateChange> meta = byKeyGroup.remove(META_KEY_GROUP);
        if (meta != null) {
            writeChangeSetOfKG(output, META_KEY_GROUP, meta);
        }
        // output changeSets
        for (Map.Entry<Integer, List<StateChange>> entry : byKeyGroup.entrySet()) {
            writeChangeSetOfKG(output, entry.getKey(), entry.getValue());
        }
    }

    private void writeChangeSetOfKG(
            DataOutputViewStreamWrapper output, int keyGroup, List<StateChange> stateChanges)
            throws IOException {
        output.writeInt(stateChanges.size());
        output.writeInt(keyGroup);
        for (StateChange stateChange : stateChanges) {
            output.writeInt(stateChange.getChange().length);
            output.write(stateChange.getChange());
        }
    }

    CloseableIterator<StateChange> read(DataInputStream input) throws IOException {

        return new CloseableIterator<StateChange>() {
            int numUnreadGroups = input.readInt();
            int numLeftInGroup = numUnreadGroups-- == 0 ? 0 : input.readInt();
            int keyGroup = numLeftInGroup == 0 ? 0 : input.readInt();

            @Override
            public boolean hasNext() {
                advance();
                return numLeftInGroup > 0;
            }

            private void advance() {
                if (numLeftInGroup == 0 && numUnreadGroups > 0) {
                    numUnreadGroups--;
                    try {
                        numLeftInGroup = input.readInt();
                        keyGroup = input.readInt();
                    } catch (IOException e) {
                        ExceptionUtils.rethrow(e);
                    }
                }
            }

            @Override
            public StateChange next() {
                advance();
                if (numLeftInGroup == 0) {
                    throw new NoSuchElementException();
                }
                numLeftInGroup--;
                try {
                    return readChange();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private StateChange readChange() throws IOException {
                int size = input.readInt();
                byte[] bytes = new byte[size];
                IOUtils.readFully(input, bytes, 0, size);
                return keyGroup == META_KEY_GROUP
                        ? StateChange.ofMetadataChange(bytes)
                        : StateChange.ofDataChange(keyGroup, bytes);
            }

            @Override
            public void close() throws Exception {
                LOG.trace("close {}", input);
                input.close();
            }
        };
    }
}
