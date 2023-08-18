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

package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SplitEnumerator} for iterator sources. Simply takes the pre-split set of splits and
 * assigns it first-come-first-serve.
 *
 * @param <SplitT> The type of the splits used by the source.
 */
@Public
public class IteratorSourceEnumerator<SplitT extends IteratorSourceSplit<?, ?>>
        implements SplitEnumerator<SplitT, Collection<SplitT>> {

    private final SplitEnumeratorContext<SplitT> context;
    private final Queue<SplitT> remainingSplits;

    public IteratorSourceEnumerator(
            SplitEnumeratorContext<SplitT> context, Collection<SplitT> splits) {
        this.context = checkNotNull(context);
        this.remainingSplits = new ArrayDeque<>(splits);
        this.context.metricGroup().setUnassignedSplitsGauge(() -> (long) remainingSplits.size());
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() {}

    @Override
    public void close() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final SplitT nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }

    @Override
    public void addReader(int subtaskId) {
        // we don't assign any splits here, because this registration happens after fist startup and
        // after each reader restart/recovery we only want to assign splits once, initially, which
        // we get by reacting to the readers explicit split request
    }
}
