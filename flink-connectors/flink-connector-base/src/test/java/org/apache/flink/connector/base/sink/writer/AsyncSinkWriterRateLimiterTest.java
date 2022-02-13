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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.AsyncSinkReleaseAndBlockWriterImpl;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.AsyncSinkReleaseAndBlockWriterImplBuilder;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.AsyncSinkWriterImpl;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.AsyncSinkWriterImplBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for throttling of {@link AsyncSinkWriter} based on {@link RateLimitingStrategy}.
 */
public class AsyncSinkWriterRateLimiterTest {

    private TestSinkInitContext sinkInitContext;

    @Before
    public void before() {
        sinkInitContext = new TestSinkInitContext();
    }

    @Test
    public void testThatWriterThrottlesOnRateLimitingStrategy() throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(10);

        // latches needed for first request
        CountDownLatch blockedThreadLatch = new CountDownLatch(1);
        CountDownLatch delayedThreadLatch = new CountDownLatch(1);

        AsyncSinkReleaseAndBlockWriterImpl writer =
                new AsyncSinkReleaseAndBlockWriterImplBuilder()
                        .blockedThreadLatch(blockedThreadLatch)
                        .delayedThreadLatch(delayedThreadLatch)
                        .blockForLimitedTime(false)
                        .context(sinkInitContext)
                        .maxBatchSize(2)
                        .maxInFlightRequests(4)
                        .rateLimitingStrategy(new FixedRateLimitingStrategy(2))
                        .build();

        sendBatchAndAssert(es, writer, Arrays.asList(1, 2), sink -> (sink.getBufferSize() == 0));
        delayedThreadLatch.await();

        // assert throttling is done ny ratelimiting strategy not inflight request count
        sendBatchAndAssert(es, writer, Arrays.asList(3, 4), sink -> (sink.getBufferSize() == 2));

        blockedThreadLatch.countDown();
        es.shutdown();
        assertTrue(es.awaitTermination(1000, TimeUnit.MILLISECONDS), "Stuck at termination");
    }

    @Test
    public void testThatWriterUpdatesRateLimitingStrategy() throws Exception {
        RateLimitingStrategy rateLimitingStrategy = new AIMDRateLimitingStrategy(1, 0.5, 4, 4);
        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();

        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImplBuilder(new ArrayList<>())
                        .context(sinkInitContext)
                        .maxBatchSizeInBytes(10_000)
                        .maxTimeInBufferMS(100)
                        .maxRecordSizeInBytes(10_000)
                        .simulateFailures(true)
                        .rateLimitingStrategy(rateLimitingStrategy)
                        .build();

        assertThat(rateLimitingStrategy.getRateLimit()).isEqualTo(4);

        // sending first batch
        tpts.setCurrentTime(0L);
        sink.write("1");
        sink.write("2");
        sink.write("225");
        tpts.setCurrentTime(100L);

        // expect 1 to fail
        sink.write("3");
        sink.write("4");

        // buffer should be [225, 3, 4]
        tpts.setCurrentTime(199L);

        // failed request should update strategy 4 -> 2, new request passes updates strategy 2->3
        tpts.setCurrentTime(200L);

        assertThat(rateLimitingStrategy.getRateLimit()).isEqualTo(3);
    }

    private void sendBatchAndAssert(
            ExecutorService es,
            AsyncSinkWriterImpl writer,
            List<Integer> batch,
            Predicate<AsyncSinkWriterImpl> toAssert) {
        es.submit(
                () -> {
                    batch.forEach(
                            i -> {
                                try {
                                    writer.write(i.toString());
                                } catch (IOException | InterruptedException e) {
                                    fail("test failed");
                                }
                            });
                    assertTrue(toAssert.test(writer));
                });
    }
}
