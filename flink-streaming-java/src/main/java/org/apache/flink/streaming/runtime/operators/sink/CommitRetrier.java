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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Retries the committables of a {@link CommitterHandler} until all committables are eventually
 * committed.
 */
class CommitRetrier<CommT> {
    @VisibleForTesting static final int RETRY_DELAY = 1000;
    private final ProcessingTimeService processingTimeService;
    private final CommitterHandler<CommT> committerHandler;
    private final ThrowingConsumer<? super Collection<CommittableWrapper<CommT>>, IOException>
            committableConsumer;
    private final Clock clock;

    public CommitRetrier(
            ProcessingTimeService processingTimeService,
            CommitterHandler<CommT> committerHandler,
            ThrowingConsumer<? super Collection<CommittableWrapper<CommT>>, IOException>
                    committableConsumer) {
        this(
                processingTimeService,
                committerHandler,
                committableConsumer,
                SystemClock.getInstance());
    }

    @VisibleForTesting
    public CommitRetrier(
            ProcessingTimeService processingTimeService,
            CommitterHandler<CommT> committerHandler,
            ThrowingConsumer<? super Collection<CommittableWrapper<CommT>>, IOException>
                    committableConsumer,
            Clock clock) {
        this.processingTimeService = checkNotNull(processingTimeService);
        this.committerHandler = checkNotNull(committerHandler);
        this.committableConsumer = checkNotNull(committableConsumer);
        this.clock = clock;
    }

    public void retryWithDelay() {
        retryAt(clock.absoluteTimeMillis() + RETRY_DELAY);
    }

    private void retryAt(long timestamp) {
        if (committerHandler.needsRetry()) {
            processingTimeService.registerTimer(
                    timestamp,
                    ts -> {
                        if (retry(1)) {
                            retryAt(ts + RETRY_DELAY);
                        }
                    });
        }
    }

    public void retryIndefinitely() throws IOException, InterruptedException {
        retry(Long.MAX_VALUE);
    }

    @VisibleForTesting
    boolean retry(long tries) throws IOException, InterruptedException {
        for (long i = 0; i < tries; i++) {
            if (!committerHandler.needsRetry()) {
                return false;
            }
            committableConsumer.accept(committerHandler.retry());
        }
        return committerHandler.needsRetry();
    }
}
