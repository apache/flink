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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RateLimitedSourceReader}. */
class RateLimitedSourceReaderTest {

    @Test
    void testUpdateTargetRateForwardsToWrappedDynamicRateLimiter() throws Exception {
        RecordingDynamicRateLimiter rateLimiter = new RecordingDynamicRateLimiter();
        RateLimitedSourceReader<Integer, MockSourceSplit> reader =
                new RateLimitedSourceReader<>(new MockSourceReader(), rateLimiter);

        reader.updateTargetRate(42.0);

        assertThat(rateLimiter.lastUpdatedRate).isEqualTo(42.0);
    }

    @Test
    void testUpdateTargetRateIsNoOpForAStaticRateLimiter() throws Exception {
        RateLimitedSourceReader<Integer, MockSourceSplit> reader =
                new RateLimitedSourceReader<>(new MockSourceReader(), new NoOpRateLimiter<>());

        // Must not throw even though the wrapped limiter cannot be dynamically updated.
        reader.updateTargetRate(42.0);
    }

    @Test
    void testNotifyCheckpointCompleteForwardsToRateLimiter() throws Exception {
        RecordingDynamicRateLimiter rateLimiter = new RecordingDynamicRateLimiter();
        RateLimitedSourceReader<Integer, MockSourceSplit> reader =
                new RateLimitedSourceReader<>(new MockSourceReader(), rateLimiter);

        reader.notifyCheckpointComplete(123L);

        assertThat(rateLimiter.lastCompletedCheckpointId).isEqualTo(123L);
    }

    @Test
    void testNotifyCheckpointCompleteForwardsToWrappedReader() throws Exception {
        MockSourceReader wrapped = new MockSourceReader();
        RateLimitedSourceReader<Integer, MockSourceSplit> reader =
                new RateLimitedSourceReader<>(wrapped, new NoOpRateLimiter<>());

        reader.notifyCheckpointComplete(123L);

        assertThat(wrapped.getCompletedCheckpoints()).containsExactly(123L);
    }

    @Test
    void testPauseOrResumeSplitsForwardsToWrappedReader() {
        MockSourceReader wrapped = new MockSourceReader();
        RateLimitedSourceReader<Integer, MockSourceSplit> reader =
                new RateLimitedSourceReader<>(wrapped, new NoOpRateLimiter<>());

        reader.pauseOrResumeSplits(Collections.singleton("split-1"), Collections.emptySet());

        assertThat(wrapped.getPausedSplits()).containsExactly("split-1");
    }

    @Test
    void testHandleSourceEventsForwardsToWrappedReader() {
        MockSourceReader wrapped = new MockSourceReader();
        RateLimitedSourceReader<Integer, MockSourceSplit> reader =
                new RateLimitedSourceReader<>(wrapped, new NoOpRateLimiter<>());
        final SourceEvent event = new SourceEvent() {};

        reader.handleSourceEvents(event);

        assertThat(wrapped.getReceivedSourceEvents()).containsExactly(event);
    }

    private static class RecordingDynamicRateLimiter
            implements DynamicRateLimiter<MockSourceSplit> {
        private double lastUpdatedRate = Double.NaN;
        private long lastCompletedCheckpointId = -1L;

        @Override
        public void updateRate(double recordsPerSecond) {
            this.lastUpdatedRate = recordsPerSecond;
        }

        @Override
        public CompletionStage<Void> acquire(int numberOfEvents) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            this.lastCompletedCheckpointId = checkpointId;
        }
    }
}
