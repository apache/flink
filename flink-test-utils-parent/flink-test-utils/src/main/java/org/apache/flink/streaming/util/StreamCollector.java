/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.junit.rules.ExternalResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple utility for collecting all the elements in a {@link DataStream}.
 *
 * <pre>{@code
 * public class DataStreamTest {
 *
 * 		{@literal @}Rule
 * 		public StreamCollector collector = new StreamCollector();
 *
 * 		public void test() throws Exception {
 * 		 	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * 		 	DataStream<Integer> stream = env.fromElements(1, 2, 3);
 *
 * 		 	CompletableFuture<Collection<Integer>> results = collector.collect(stream);
 * 		 	Assert.assertThat(results.get(), hasItems(1, 2, 3));
 * 		}
 * }
 * }</pre>
 *
 * <p><b>Note:</b> The stream collector assumes: 1) The stream is bounded. 2) All elements will fit
 * in memory. 3) All tasks run within the same JVM.
 */
@SuppressWarnings("rawtypes")
public class StreamCollector extends ExternalResource {

    private static final AtomicLong counter = new AtomicLong();

    private static final Map<Long, CountDownLatch> latches = new ConcurrentHashMap<>();

    private static final Map<Long, Queue> resultQueues = new ConcurrentHashMap<>();

    private List<Long> ids;

    @Override
    protected void before() {
        ids = new ArrayList<>();
    }

    /**
     * @return A future that contains all the elements of the DataStream which completes when all
     *     elements have been processed.
     */
    public <IN> CompletableFuture<Collection<IN>> collect(DataStream<IN> stream) {
        final long id = counter.getAndIncrement();
        ids.add(id);

        int parallelism = stream.getParallelism();
        if (parallelism == ExecutionConfig.PARALLELISM_DEFAULT) {
            parallelism = stream.getExecutionEnvironment().getParallelism();
        }

        CountDownLatch latch = new CountDownLatch(parallelism);
        latches.put(id, latch);

        Queue<IN> results = new ConcurrentLinkedDeque<>();
        resultQueues.put(id, results);

        stream.addSink(new CollectingSink<>(id));

        return CompletableFuture.runAsync(
                        () -> {
                            try {
                                latch.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException("Failed to collect results");
                            }
                        })
                .thenApply(ignore -> results);
    }

    @Override
    protected void after() {
        for (Long id : ids) {
            latches.remove(id);
            resultQueues.remove(id);
        }
    }

    private static class CollectingSink<IN> extends RichSinkFunction<IN> {

        private final long id;

        private transient CountDownLatch latch;

        private transient Queue<IN> results;

        private CollectingSink(long id) {
            this.id = id;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void open(Configuration parameters) throws Exception {
            latch = StreamCollector.latches.get(id);
            results = (Queue<IN>) StreamCollector.resultQueues.get(id);
        }

        @Override
        public void invoke(IN value, Context context) throws Exception {
            results.add(value);
        }

        @Override
        public void close() throws Exception {
            latch.countDown();
        }
    }
}
