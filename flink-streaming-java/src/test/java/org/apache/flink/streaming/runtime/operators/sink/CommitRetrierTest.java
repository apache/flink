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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class CommitRetrierTest {
    @Test
    void testRetry() throws Exception {
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        CommitterHandlerWithRetries committerHandler = new CommitterHandlerWithRetries();
        CommitRetrier retryer = new CommitRetrier(processingTimeService, committerHandler);
        assertThat(committerHandler.needsRetry(), equalTo(false));

        committerHandler.addRetries(2);
        assertThat(committerHandler.needsRetry(), equalTo(true));
        assertThat(committerHandler.getPendingRetries(), equalTo(2));

        assertThat(retryer.retry(0), equalTo(true));
        assertThat(committerHandler.getPendingRetries(), equalTo(2));

        assertThat(retryer.retry(1), equalTo(true));
        assertThat(committerHandler.getPendingRetries(), equalTo(1));

        assertThat(retryer.retry(1), equalTo(false));
        assertThat(committerHandler.getPendingRetries(), equalTo(0));

        assertThat(committerHandler.needsRetry(), equalTo(false));
    }

    @Test
    void testInfiniteRetry() throws Exception {
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        CommitterHandlerWithRetries committerHandler = new CommitterHandlerWithRetries();
        CommitRetrier retryer = new CommitRetrier(processingTimeService, committerHandler);
        assertThat(committerHandler.needsRetry(), equalTo(false));

        committerHandler.addRetries(2);
        assertThat(committerHandler.needsRetry(), equalTo(true));
        assertThat(committerHandler.getPendingRetries(), equalTo(2));

        retryer.retryIndefinitely();
        assertThat(committerHandler.getPendingRetries(), equalTo(0));
        assertThat(committerHandler.needsRetry(), equalTo(false));
    }

    @Test
    void testTimedRetry() throws Exception {
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        Clock manualClock = new ManualClock();
        processingTimeService.setCurrentTime(manualClock.absoluteTimeMillis());

        CommitterHandlerWithRetries committerHandler = new CommitterHandlerWithRetries();
        CommitRetrier retryer =
                new CommitRetrier(processingTimeService, committerHandler, manualClock);
        assertThat(committerHandler.needsRetry(), equalTo(false));

        committerHandler.addRetries(2);
        retryer.retryWithDelay();

        assertThat(committerHandler.needsRetry(), equalTo(true));
        assertThat(committerHandler.getPendingRetries(), equalTo(2));

        processingTimeService.advance(CommitRetrier.RETRY_DELAY);
        assertThat(committerHandler.getPendingRetries(), equalTo(1));

        processingTimeService.advance(CommitRetrier.RETRY_DELAY);
        assertThat(committerHandler.getPendingRetries(), equalTo(0));
        assertThat(committerHandler.needsRetry(), equalTo(false));

        processingTimeService.advance(CommitRetrier.RETRY_DELAY);
        assertThat(committerHandler.getPendingRetries(), equalTo(0));
        assertThat(committerHandler.needsRetry(), equalTo(false));
    }

    private static class CommitterHandlerWithRetries extends ForwardCommittingHandler<String> {
        private AtomicInteger retriesNeeded = new AtomicInteger(0);

        void addRetries(int retries) {
            retriesNeeded.addAndGet(retries);
        }

        int getPendingRetries() {
            return retriesNeeded.get();
        }

        @Override
        public boolean needsRetry() {
            return getPendingRetries() > 0;
        }

        @Override
        public void retry() throws IOException, InterruptedException {
            retriesNeeded.decrementAndGet();
        }
    }
}
