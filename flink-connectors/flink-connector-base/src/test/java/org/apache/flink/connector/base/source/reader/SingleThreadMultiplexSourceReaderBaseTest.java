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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.mocks.MockSourceReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.MdcUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the job attribution that {@link SingleThreadMultiplexSourceReaderBase} gives the fetcher
 * threads it creates.
 */
class SingleThreadMultiplexSourceReaderBaseTest {

    @Test
    void testFetcherThreadNameIdentifiesJob() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();

        assertThat(fetcherThreadNameOf(context))
                .endsWith(MdcUtils.jobThreadNameSuffix(context.getJobInfo()));
    }

    /**
     * A context that leaves {@link SourceReaderContext#getJobInfo()} at its throwing default must
     * still yield a working reader, only one without a job suffix: attribution is diagnostic only.
     */
    @Test
    void testContextWithoutJobInfoYieldsUnattributedFetcherThread() throws Exception {
        final SourceReaderContext context =
                new TestingReaderContext() {
                    @Override
                    public JobInfo getJobInfo() {
                        throw new UnsupportedOperationException();
                    }
                };

        assertThat(fetcherThreadNameOf(context))
                .as("an unattributable reader must not gain a job suffix")
                .doesNotContain(" (job: ");
    }

    /**
     * Builds a reader over the given context and assigns it a split, so that the fetcher thread
     * starts and can report its own name.
     */
    private static String fetcherThreadNameOf(SourceReaderContext context) throws Exception {
        final CompletableFuture<String> fetcherThreadName = new CompletableFuture<>();
        try (MockSourceReader reader =
                new MockSourceReader(
                        () -> new ThreadNameReportingSplitReader(fetcherThreadName),
                        new Configuration(),
                        context)) {
            reader.start();
            reader.addSplits(Collections.singletonList(new MockSourceSplit(0, 0, 1)));
            assertThat(fetcherThreadName)
                    .as("The fetcher thread should have started fetching.")
                    .succeedsWithin(Duration.ofSeconds(60));
            return fetcherThreadName.get();
        }
    }

    /** Reports the thread it is driven on, which is the fetcher thread under test. */
    private static final class ThreadNameReportingSplitReader
            implements SplitReader<int[], MockSourceSplit> {

        private final CompletableFuture<String> threadName;
        private final OneShotLatch fetchBlocker = new OneShotLatch();

        private ThreadNameReportingSplitReader(CompletableFuture<String> threadName) {
            this.threadName = threadName;
        }

        @Override
        public RecordsWithSplitIds<int[]> fetch() {
            threadName.complete(Thread.currentThread().getName());
            // Stay inside fetch() until woken up, so the fetcher does not spin on empty fetches.
            try {
                fetchBlocker.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
        }

        @Override
        public void handleSplitsChanges(SplitsChange<MockSourceSplit> splitsChanges) {}

        @Override
        public void wakeUp() {
            fetchBlocker.trigger();
        }

        @Override
        public void close() {
            fetchBlocker.trigger();
        }
    }
}
