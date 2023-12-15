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

package org.apache.flink.connector.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.source.TerminatingLogic;
import org.apache.flink.connector.source.ValuesSource;
import org.apache.flink.connector.source.split.ValuesSourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * A {@link SplitEnumerator} for {@link ValuesSource}. Simply takes the pre-split set of splits and
 * assigns it first-come-first-serve.
 */
public class ValuesSourceEnumerator implements SplitEnumerator<ValuesSourceSplit, NoOpEnumState> {

    private final SplitEnumeratorContext<ValuesSourceSplit> context;
    private final Queue<ValuesSourceSplit> remainingSplits;
    private final TerminatingLogic terminatingLogic;

    public ValuesSourceEnumerator(
            SplitEnumeratorContext<ValuesSourceSplit> context,
            List<ValuesSourceSplit> remainingSplits,
            TerminatingLogic terminatingLogic) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>(remainingSplits);
        this.terminatingLogic = terminatingLogic;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final ValuesSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else if (terminatingLogic == TerminatingLogic.INFINITE) {
            context.assignSplit(new ValuesSourceSplit(-1, TerminatingLogic.INFINITE), subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<ValuesSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // we don't assign any splits here, because this registration happens after fist startup and
        // after each reader restart/recovery we only want to assign splits once, initially, which
        // we get by reacting to the readers explicit split request
    }

    @Override
    public NoOpEnumState snapshotState(long checkpointId) throws Exception {
        return new NoOpEnumState();
    }

    @Override
    public void close() throws IOException {}
}
