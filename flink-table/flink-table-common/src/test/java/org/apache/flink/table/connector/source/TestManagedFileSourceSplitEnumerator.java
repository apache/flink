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

package org.apache.flink.table.connector.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/** Managed {@link SplitEnumerator} for testing. */
public class TestManagedFileSourceSplitEnumerator
        implements SplitEnumerator<TestManagedIterableSourceSplit, Void> {

    private final SplitEnumeratorContext<TestManagedIterableSourceSplit> context;
    private final Queue<TestManagedIterableSourceSplit> remainingSplits;

    public TestManagedFileSourceSplitEnumerator(
            SplitEnumeratorContext<TestManagedIterableSourceSplit> context,
            List<TestManagedIterableSourceSplit> splits) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>(splits);
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        TestManagedIterableSourceSplit split = remainingSplits.poll();
        if (split == null) {
            context.signalNoMoreSplits(subtaskId);
        } else {
            context.assignSplit(split, subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<TestManagedIterableSourceSplit> splits, int subtaskId) {
        splits.forEach(this.remainingSplits::offer);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {}
}
