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

package org.apache.flink.runtime.state.changelog.inmemory;

import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toMap;

@NotThreadSafe
class InMemoryStateChangelogWriter implements StateChangelogWriter<InMemoryStateChangelogHandle> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateChangelogWriter.class);

    private final Map<Integer, NavigableMap<SequenceNumber, byte[]>> changesByKeyGroup =
            new HashMap<>();
    private long sqn = 0L;
    private boolean closed;

    @Override
    public void append(int keyGroup, byte[] value) {
        Preconditions.checkState(!closed, "LogWriter is closed");
        LOG.trace("append, keyGroup={}, {} bytes", keyGroup, value.length);
        changesByKeyGroup
                .computeIfAbsent(keyGroup, unused -> new TreeMap<>())
                .put(SequenceNumber.of(++sqn), value);
    }

    @Override
    public SequenceNumber lastAppendedSequenceNumber() {
        return SequenceNumber.of(sqn);
    }

    @Override
    public CompletableFuture<InMemoryStateChangelogHandle> persist(SequenceNumber from) {
        LOG.debug("Persist after {}", from);
        Preconditions.checkNotNull(from);
        return completedFuture(new InMemoryStateChangelogHandle(collectChanges(from)));
    }

    private Map<Integer, List<byte[]>> collectChanges(SequenceNumber after) {
        return changesByKeyGroup.entrySet().stream()
                .collect(
                        toMap(
                                Map.Entry::getKey,
                                kv ->
                                        new ArrayList<>(
                                                kv.getValue().tailMap(after, true).values())));
    }

    @Override
    public void close() {
        Preconditions.checkState(!closed);
        closed = true;
    }

    @Override
    public void truncate(SequenceNumber before) {
        changesByKeyGroup.forEach((k, v) -> {});
    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to) {
        throw new UnsupportedOperationException();
    }
}
