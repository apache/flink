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
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Extension of the {@link KinesisDataFetcher} for testing. */
public class TestableKinesisDataFetcher<T> extends KinesisDataFetcher<T> {

    private final OneShotLatch runWaiter;
    private final Semaphore discoveryWaiter = new Semaphore(0);
    private final OneShotLatch shutdownWaiter;

    public TestableKinesisDataFetcher(
            List<String> fakeStreams,
            SourceFunction.SourceContext<T> sourceContext,
            Properties fakeConfiguration,
            KinesisDeserializationSchema<T> deserializationSchema,
            int fakeTotalCountOfSubtasks,
            int fakeIndexOfThisSubtask,
            AtomicReference<Throwable> thrownErrorUnderTest,
            LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest,
            HashMap<String, String> subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
            KinesisProxyInterface fakeKinesis) {

        this(
                fakeStreams,
                sourceContext,
                fakeConfiguration,
                deserializationSchema,
                fakeTotalCountOfSubtasks,
                fakeIndexOfThisSubtask,
                thrownErrorUnderTest,
                subscribedShardsStateUnderTest,
                subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
                fakeKinesis,
                null);
    }

    public TestableKinesisDataFetcher(
            List<String> fakeStreams,
            SourceFunction.SourceContext<T> sourceContext,
            Properties fakeConfiguration,
            KinesisDeserializationSchema<T> deserializationSchema,
            int fakeTotalCountOfSubtasks,
            int fakeIndexOfThisSubtask,
            AtomicReference<Throwable> thrownErrorUnderTest,
            LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest,
            HashMap<String, String> subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
            KinesisProxyInterface fakeKinesis,
            KinesisProxyV2Interface fakeKinesisV2) {
        super(
                fakeStreams,
                sourceContext,
                sourceContext.getCheckpointLock(),
                TestUtils.getMockedRuntimeContext(fakeTotalCountOfSubtasks, fakeIndexOfThisSubtask),
                fakeConfiguration,
                deserializationSchema,
                DEFAULT_SHARD_ASSIGNER,
                null,
                null,
                thrownErrorUnderTest,
                subscribedShardsStateUnderTest,
                subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
                properties -> fakeKinesis,
                properties -> fakeKinesisV2);

        this.runWaiter = new OneShotLatch();
        this.shutdownWaiter = new OneShotLatch();
    }

    @Override
    public void runFetcher() throws Exception {
        runWaiter.trigger();
        super.runFetcher();
    }

    public void waitUntilRun() throws Exception {
        runWaiter.await();
    }

    public void waitUntilShutdown(long timeout, TimeUnit timeUnit) throws Exception {
        shutdownWaiter.await(timeout, timeUnit);
    }

    @Override
    protected ExecutorService createShardConsumersThreadPool(String subtaskName) {
        // this is just a dummy fetcher, so no need to create a thread pool for shard consumers
        return new TestExecutorService();
    }

    @Override
    public void shutdownFetcher() {
        super.shutdownFetcher();
        shutdownWaiter.trigger();
    }

    @Override
    public List<StreamShardHandle> discoverNewShardsToSubscribe() throws InterruptedException {
        List<StreamShardHandle> newShards = super.discoverNewShardsToSubscribe();
        discoveryWaiter.release();
        return newShards;
    }

    public void waitUntilInitialDiscovery() throws InterruptedException {
        discoveryWaiter.acquire();
    }

    public void waitUntilDiscovery(int number) throws InterruptedException {
        discoveryWaiter.acquire(number);
    }

    private static class TestExecutorService implements ExecutorService {
        boolean terminated = false;

        @Override
        public void execute(Runnable command) {}

        @Override
        public void shutdown() {
            terminated = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            terminated = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return terminated;
        }

        @Override
        public boolean isTerminated() {
            return terminated;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return terminated;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return null;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return null;
        }

        @Override
        public Future<?> submit(Runnable task) {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
            return null;
        }

        @Override
        public <T> T invokeAny(
                Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            return null;
        }
    }
}
