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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** The task to finish reading some splits. */
@Internal
public class RemoveSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveSplitsTask.class);

    private final SplitReader<?, SplitT> splitReader;
    private final List<SplitT> removedSplits;
    private final Map<String, SplitT> assignedSplits;
    private final Consumer<Collection<String>> splitFinishedCallback;

    RemoveSplitsTask(
            SplitReader<?, SplitT> splitReader,
            List<SplitT> removedSplits,
            Map<String, SplitT> assignedSplits,
            Consumer<Collection<String>> splitFinishedCallback) {
        this.splitReader = splitReader;
        this.removedSplits = removedSplits;
        this.assignedSplits = assignedSplits;
        this.splitFinishedCallback = splitFinishedCallback;
    }

    @Override
    public boolean run() {
        for (SplitT s : removedSplits) {
            assignedSplits.remove(s.splitId());
        }
        splitReader.handleSplitsChanges(new SplitsRemoval<>(removedSplits));

        List<String> splitIds =
                removedSplits.stream().map(SourceSplit::splitId).collect(Collectors.toList());
        splitFinishedCallback.accept(splitIds);
        LOG.info("RecordEvaluator triggers splits {} to finish reading.", splitIds);
        return true;
    }

    @Override
    public void wakeUp() {
        // Do nothing.
    }

    @Override
    public String toString() {
        return String.format("RemoveSplitsTask: [%s]", removedSplits);
    }
}
