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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A mock {@link SplitEnumerator} for unit tests. */
public class MockSplitEnumerator
        implements SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> {
    private final List<MockSourceSplit> splits;
    private final SplitEnumeratorContext<MockSourceSplit> context;

    public MockSplitEnumerator(
            List<MockSourceSplit> splits, SplitEnumeratorContext<MockSourceSplit> context) {
        this.splits = splits;
        this.context = context;
    }

    @Override
    public void start() {}

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {
        this.splits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        if (context.registeredReaders().size() == context.currentParallelism()) {
            int numReaders = context.registeredReaders().size();
            Map<Integer, List<MockSourceSplit>> assignment = new HashMap<>();
            for (int i = 0; i < splits.size(); i++) {
                assignment
                        .computeIfAbsent(i % numReaders, t -> new ArrayList<>())
                        .add(splits.get(i));
            }
            context.assignSplits(new SplitsAssignment<>(assignment));
            splits.clear();
            for (int i = 0; i < numReaders; i++) {
                context.signalNoMoreSplits(i);
            }
        }
    }

    @Override
    public List<MockSourceSplit> snapshotState() {
        return splits;
    }

    @Override
    public void close() throws IOException {}
}
