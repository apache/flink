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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Wraps the actual {@link SourceReader} and rate limits its data emission. */
@Experimental
public class RateLimitedSourceReader<E, SplitT extends SourceSplit>
        implements SourceReader<E, SplitT> {

    private final SourceReader<E, SplitT> sourceReader;
    private final RateLimiter rateLimiter;
    private CompletableFuture<Void> availabilityFuture = null;

    /**
     * Instantiates a new rate-limited source reader.
     *
     * @param sourceReader The actual source reader.
     * @param rateLimiter The rate limiter.
     */
    public RateLimitedSourceReader(SourceReader<E, SplitT> sourceReader, RateLimiter rateLimiter) {
        checkNotNull(sourceReader);
        checkNotNull(rateLimiter);
        this.sourceReader = sourceReader;
        this.rateLimiter = rateLimiter;
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() {
        sourceReader.start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<E> output) throws Exception {
        if (availabilityFuture == null) {
            // force isAvailable() to be called first to evaluate rate-limiting
            return InputStatus.NOTHING_AVAILABLE;
        }
        // reset future because the next record may hit the rate limit
        availabilityFuture = null;
        final InputStatus inputStatus = sourceReader.pollNext(output);
        if (inputStatus == InputStatus.MORE_AVAILABLE) {
            // force another go through isAvailable() to evaluate rate-limiting
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            return inputStatus;
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (availabilityFuture == null) {
            availabilityFuture =
                    rateLimiter
                            .acquire()
                            .toCompletableFuture()
                            .thenCombine(sourceReader.isAvailable(), (l, r) -> null);
        }
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<SplitT> splits) {
        sourceReader.addSplits(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        sourceReader.notifyNoMoreSplits();
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        return sourceReader.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        sourceReader.close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        rateLimiter.notifyCheckpointComplete(checkpointId);
    }
}
