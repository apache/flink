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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.collect.utils.TestJobClient;
import org.apache.flink.streaming.api.operators.collect.utils.TestSimpleCountCoordinationRequestHandler;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.IncrementalDelayRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Tests for {@link CollectResultIterator}. */
public class CollectResultFetcherRetryITCase extends TestLogger {
    private final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;
    private static final OperatorID TEST_OPERATOR_ID = new OperatorID();

    private static final JobID TEST_JOB_ID = new JobID();
    private static final String ACCUMULATOR_NAME = "accumulatorName";

    private final AtomicLong totalRetryInterval = new AtomicLong(0L);

    private final Consumer<RetryStrategy> retryAction =
            retryStrategy -> {
                long delay = retryStrategy.getRetryDelay().toMillis();
                try {
                    TimeUnit.MILLISECONDS.sleep(delay);
                    totalRetryInterval.getAndAdd(delay);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };

    @Test
    public void testFetcherWithIncrementalDelayRetryStrategy() throws Exception {
        long initDelay = 100L;
        long increment = 100L;
        long maxDelay = 1000L;
        totalRetryInterval.set(0L);

        TestSimpleCountCoordinationRequestHandler handler =
                new TestSimpleCountCoordinationRequestHandler();
        Tuple2<CollectResultFetcher<Integer>, JobClient> tuple2 =
                createFetcherAndJobClient(
                        new UncheckpointedCollectResultBuffer<>(serializer, true),
                        handler,
                        () ->
                                new IncrementalDelayRetryStrategy(
                                        Integer.MAX_VALUE,
                                        Duration.ofMillis(initDelay),
                                        Duration.ofMillis(increment),
                                        Duration.ofMillis(maxDelay)),
                        retryAction);

        CollectResultFetcher<Integer> resultFetcher = tuple2.f0;

        Thread t =
                new Thread(
                        () -> {
                            try {
                                resultFetcher.next();
                            } catch (Exception ignored) {

                            }
                        });

        t.start();

        Random random = new Random();
        TimeUnit.SECONDS.sleep(random.nextInt(5));
        handler.close();

        long expect = 0L;
        long currentDelay = initDelay;

        for (int i = 1; i <= handler.getRequestCount(); i++) {
            if (i > 1) {
                currentDelay = Math.min(currentDelay + increment, maxDelay);
            }

            expect += currentDelay;
        }

        Assert.assertEquals(expect, totalRetryInterval.get());
    }

    @Test
    public void testFetcherWithExponentialBackoffDelayRetryStrategy() throws Exception {
        long initDelay = 100L;
        long maxDelay = 1000L;
        totalRetryInterval.set(0L);

        TestSimpleCountCoordinationRequestHandler handler =
                new TestSimpleCountCoordinationRequestHandler();
        Tuple2<CollectResultFetcher<Integer>, JobClient> tuple2 =
                createFetcherAndJobClient(
                        new UncheckpointedCollectResultBuffer<>(serializer, true),
                        handler,
                        () ->
                                new ExponentialBackoffRetryStrategy(
                                        Integer.MAX_VALUE,
                                        Duration.ofMillis(initDelay),
                                        Duration.ofMillis(maxDelay)),
                        retryAction);

        CollectResultFetcher<Integer> resultFetcher = tuple2.f0;

        Thread t =
                new Thread(
                        () -> {
                            try {
                                resultFetcher.next();
                            } catch (Exception ignored) {

                            }
                        });

        t.start();

        Random random = new Random();
        TimeUnit.SECONDS.sleep(random.nextInt(5));
        handler.close();

        long expect = 0L;
        long currentDelay = initDelay;

        for (int i = 1; i <= handler.getRequestCount(); i++) {
            if (i > 1) {
                currentDelay = Math.min(currentDelay * 2, maxDelay);
            }

            expect += currentDelay;
        }

        Assert.assertEquals(expect, totalRetryInterval.get());
    }

    @Test
    public void testFetcherWithFixedDelayRetryStrategy() throws Exception {
        long initDelay = 100L;
        totalRetryInterval.set(0L);

        TestSimpleCountCoordinationRequestHandler handler =
                new TestSimpleCountCoordinationRequestHandler();
        Tuple2<CollectResultFetcher<Integer>, JobClient> tuple2 =
                createFetcherAndJobClient(
                        new UncheckpointedCollectResultBuffer<>(serializer, true),
                        handler,
                        () ->
                                new FixedRetryStrategy(
                                        Integer.MAX_VALUE, Duration.ofMillis(initDelay)),
                        retryAction);

        CollectResultFetcher<Integer> resultFetcher = tuple2.f0;

        Thread t =
                new Thread(
                        () -> {
                            try {
                                resultFetcher.next();
                            } catch (Exception ignored) {

                            }
                        });

        t.start();

        Random random = new Random();
        TimeUnit.SECONDS.sleep(random.nextInt(5));
        handler.close();

        long expect = 0L;
        for (int i = 1; i <= handler.getRequestCount(); i++) {
            expect += initDelay;
        }

        Assert.assertEquals(expect, totalRetryInterval.get());
    }

    private Tuple2<CollectResultFetcher<Integer>, JobClient> createFetcherAndJobClient(
            AbstractCollectResultBuffer<Integer> buffer,
            TestSimpleCountCoordinationRequestHandler handler,
            Supplier<RetryStrategy> retryStrategySupplier,
            Consumer<RetryStrategy> retryAction) {
        CollectResultFetcher<Integer> resultFetcher =
                new CollectResultFetcher<>(
                        buffer,
                        CompletableFuture.completedFuture(TEST_OPERATOR_ID),
                        ACCUMULATOR_NAME,
                        retryStrategySupplier,
                        retryAction,
                        AkkaOptions.ASK_TIMEOUT_DURATION.defaultValue().toMillis());

        TestJobClient.JobInfoProvider infoProvider =
                new TestJobClient.JobInfoProvider() {

                    @Override
                    public boolean isJobFinished() {
                        return handler.isClosed();
                    }

                    @Override
                    public Map<String, OptionalFailure<Object>> getAccumulatorResults() {
                        return new HashMap<>();
                    }
                };

        TestJobClient jobClient =
                new TestJobClient(TEST_JOB_ID, TEST_OPERATOR_ID, handler, infoProvider);
        resultFetcher.setJobClient(jobClient);

        return Tuple2.of(resultFetcher, jobClient);
    }
}
