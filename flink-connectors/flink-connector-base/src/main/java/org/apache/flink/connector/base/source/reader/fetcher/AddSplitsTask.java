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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import java.util.List;
import java.util.Map;

/** The task to add splits. */
class AddSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {

    private final SplitReader<?, SplitT> splitReader;
    private final List<SplitT> splitsToAdd;
    private final Map<String, SplitT> assignedSplits;

    AddSplitsTask(
            SplitReader<?, SplitT> splitReader,
            List<SplitT> splitsToAdd,
            Map<String, SplitT> assignedSplits) {
        this.splitReader = splitReader;
        this.splitsToAdd = splitsToAdd;
        this.assignedSplits = assignedSplits;
    }

    @Override
    public boolean run() {
        for (SplitT s : splitsToAdd) {
            assignedSplits.put(s.splitId(), s);
        }
        splitReader.handleSplitsChanges(new SplitsAddition<>(splitsToAdd));
        return true;
    }

    @Override
    public void wakeUp() {
        // Do nothing.
    }

    @Override
    public String toString() {
        return String.format("AddSplitsTask: [%s]", splitsToAdd);
    }
}
