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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamHandleReader;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

/** Serialization format for state changes. */
@Internal
public class StateChangeFormat
        implements StateChangelogHandleStreamHandleReader.StateChangeIterator {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFormat.class);

    Map<StateChangeSet, Long> write(OutputStreamWithPos os, Collection<StateChangeSet> changeSets)
            throws IOException {
        List<StateChangeSet> sorted = new ArrayList<>(changeSets);
        // using sorting instead of bucketing for simplicity
        sorted.sort(
                comparing(StateChangeSet::getLogId)
                        .thenComparing(StateChangeSet::getSequenceNumber));
        DataOutputViewStreamWrapper dataOutput = new DataOutputViewStreamWrapper(os);
        Map<StateChangeSet, Long> pendingResults = new HashMap<>();
        for (StateChangeSet changeSet : sorted) {
            pendingResults.put(changeSet, os.getPos());
            writeChangeSet(dataOutput, changeSet.getChanges());
        }
        return pendingResults;
    }

    private void writeChangeSet(DataOutputViewStreamWrapper output, List<StateChange> changes)
            throws IOException {
        // write in groups to output kg id only once
        Map<Integer, List<StateChange>> byKeyGroup =
                changes.stream().collect(Collectors.groupingBy(StateChange::getKeyGroup));
        // sort groups to output metadata first (see StateChangeLoggerImpl.COMMON_KEY_GROUP)
        Map<Integer, List<StateChange>> sorted = new TreeMap<>(byKeyGroup);
        output.writeInt(sorted.size());
        for (Map.Entry<Integer, List<StateChange>> entry : sorted.entrySet()) {
            output.writeInt(entry.getValue().size());
            output.writeInt(entry.getKey());
            for (StateChange stateChange : entry.getValue()) {
                output.writeInt(stateChange.getChange().length);
                output.write(stateChange.getChange());
            }
        }
    }

    @Override
    public CloseableIterator<StateChange> read(StreamStateHandle handle, long offset)
            throws IOException {
        FSDataInputStream stream = handle.openInputStream();
        DataInputViewStreamWrapper input = wrap(stream);
        if (stream.getPos() != offset) {
            LOG.debug("seek from {} to {}", stream.getPos(), offset);
            input.skipBytesToRead((int) offset);
        }
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
                return new StateChange(keyGroup, bytes);
            }

            @Override
            public void close() throws Exception {
                LOG.trace("close {}", stream);
                stream.close();
            }
        };
    }

    private DataInputViewStreamWrapper wrap(InputStream stream) throws IOException {
        stream = new BufferedInputStream(stream);
        boolean compressed = stream.read() == 1;
        return new DataInputViewStreamWrapper(
                compressed
                        ? SnappyStreamCompressionDecorator.INSTANCE.decorateWithCompression(stream)
                        : stream);
    }
}
