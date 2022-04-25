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

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Changes the paused splits of a n{@link SplitReader}. The task is used by default in {@link
 * SplitFetcherManager} and assumes that a {@link SplitFetcher} has multiple splits. For {@code
 * SplitFetchers} with single splits, it's instead recommended to subclass {@link
 * SplitFetcherManager} and pause the whole {@code SplitFetcher}.
 *
 * @param <SplitT> the type of the split
 */
@Internal
class PauseOrResumeSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {

    private final SplitReader<?, SplitT> splitReader;
    private final Collection<SplitT> splitsToPause;
    private final Collection<SplitT> splitsToResume;

    PauseOrResumeSplitsTask(
            SplitReader<?, SplitT> splitReader,
            Collection<SplitT> splitsToPause,
            Collection<SplitT> splitsToResume) {
        this.splitReader = checkNotNull(splitReader);
        this.splitsToPause = checkNotNull(splitsToPause);
        this.splitsToResume = checkNotNull(splitsToResume);
    }

    @Override
    public boolean run() throws IOException {
        splitReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
        return true;
    }

    @Override
    public void wakeUp() {}

    @Override
    public String toString() {
        return "AlignmentTask{"
                + "splitsToResume="
                + splitsToResume
                + ", splitsToPause="
                + splitsToPause
                + '}';
    }
}
