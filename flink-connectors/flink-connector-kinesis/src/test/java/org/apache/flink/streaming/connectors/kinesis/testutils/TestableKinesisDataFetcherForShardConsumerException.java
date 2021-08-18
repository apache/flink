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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension of the {@link KinesisDataFetcher} for testing what happens when the thread is
 * interrupted during {@link #awaitTermination()}.
 */
public class TestableKinesisDataFetcherForShardConsumerException<T>
        extends TestableKinesisDataFetcher<T> {
    public volatile boolean wasInterrupted = false;

    private OneShotLatch awaitTerminationWaiter = new OneShotLatch();

    public TestableKinesisDataFetcherForShardConsumerException(
            final List<String> fakeStreams,
            final SourceFunction.SourceContext<T> sourceContext,
            final Properties fakeConfiguration,
            final KinesisDeserializationSchema<T> deserializationSchema,
            final int fakeTotalCountOfSubtasks,
            final int fakeIndexOfThisSubtask,
            final AtomicReference<Throwable> thrownErrorUnderTest,
            final LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest,
            final HashMap<String, String> subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
            final KinesisProxyInterface fakeKinesis,
            final RecordPublisherFactory recordPublisherFactory) {
        super(
                fakeStreams,
                sourceContext,
                fakeConfiguration,
                deserializationSchema,
                fakeTotalCountOfSubtasks,
                fakeIndexOfThisSubtask,
                thrownErrorUnderTest,
                subscribedShardsStateUnderTest,
                subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
                fakeKinesis);
    }

    /** Block until awaitTermination() has been called on this class. */
    public void waitUntilAwaitTermination(long timeout, TimeUnit timeUnit)
            throws InterruptedException, TimeoutException {
        awaitTerminationWaiter.await(timeout, timeUnit);
    }

    @Override
    protected ExecutorService createShardConsumersThreadPool(final String subtaskName) {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("KinesisShardConsumers-%d").build();
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        awaitTerminationWaiter.trigger();
        try {
            // Force this method to only exit by thread getting interrupted.
            while (true) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            wasInterrupted = true;
            throw e;
        }
    }
}
