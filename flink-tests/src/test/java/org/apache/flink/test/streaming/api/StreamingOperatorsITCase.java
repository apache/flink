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

package org.apache.flink.test.streaming.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Integration tests for streaming operators. */
public class StreamingOperatorsITCase extends AbstractTestBase {

    /**
     * Tests the basic functionality of the AsyncWaitOperator: Processing a limited stream of
     * elements by doubling their value. This is tested in for the ordered and unordered mode.
     */
    @Test
    public void testAsyncWaitOperator() throws Exception {
        final int numElements = 5;
        final long timeout = 1000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, NonSerializable>> input =
                env.addSource(new NonSerializableTupleSource(numElements));

        AsyncFunction<Tuple2<Integer, NonSerializable>, Integer> function =
                new RichAsyncFunction<Tuple2<Integer, NonSerializable>, Integer>() {
                    private static final long serialVersionUID = 7000343199829487985L;

                    transient ExecutorService executorService;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        executorService = Executors.newFixedThreadPool(numElements);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        executorService.shutdownNow();
                    }

                    @Override
                    public void asyncInvoke(
                            final Tuple2<Integer, NonSerializable> input,
                            final ResultFuture<Integer> resultFuture)
                            throws Exception {
                        executorService.submit(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        resultFuture.complete(
                                                Collections.singletonList(input.f0 + input.f0));
                                    }
                                });
                    }
                };

        DataStream<Integer> orderedResult =
                AsyncDataStream.orderedWait(input, function, timeout, TimeUnit.MILLISECONDS, 2)
                        .setParallelism(1);

        // save result from ordered process
        final MemorySinkFunction sinkFunction1 = new MemorySinkFunction(0);
        final List<Integer> actualResult1 = new ArrayList<>(numElements);
        MemorySinkFunction.registerCollection(0, actualResult1);

        orderedResult.addSink(sinkFunction1).setParallelism(1);

        DataStream<Integer> unorderedResult =
                AsyncDataStream.unorderedWait(input, function, timeout, TimeUnit.MILLISECONDS, 2);

        // save result from unordered process
        final MemorySinkFunction sinkFunction2 = new MemorySinkFunction(1);
        final List<Integer> actualResult2 = new ArrayList<>(numElements);
        MemorySinkFunction.registerCollection(1, actualResult2);

        unorderedResult.addSink(sinkFunction2);

        Collection<Integer> expected = new ArrayList<>(10);

        for (int i = 0; i < numElements; i++) {
            expected.add(i + i);
        }

        env.execute();

        Assert.assertEquals(expected, actualResult1);

        Collections.sort(actualResult2);
        Assert.assertEquals(expected, actualResult2);

        MemorySinkFunction.clear();
    }

    private static class NonSerializable {
        // This makes the type non-serializable
        private final Object obj = new Object();

        private final int value;

        public NonSerializable(int value) {
            this.value = value;
        }
    }

    private static class NonSerializableTupleSource
            implements SourceFunction<Tuple2<Integer, NonSerializable>> {
        private static final long serialVersionUID = 3949171986015451520L;
        private final int numElements;

        public NonSerializableTupleSource(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, NonSerializable>> ctx) throws Exception {
            for (int i = 0; i < numElements; i++) {
                ctx.collect(new Tuple2<>(i, new NonSerializable(i)));
            }
        }

        @Override
        public void cancel() {}
    }

    private static class TupleSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = -8110466235852024821L;
        private final int numElements;
        private final int numKeys;

        public TupleSource(int numElements, int numKeys) {
            this.numElements = numElements;
            this.numKeys = numKeys;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            for (int i = 0; i < numElements; i++) {
                // keys '1' and '2' hash to different buckets
                Tuple2<Integer, Integer> result =
                        new Tuple2<>(1 + (MathUtils.murmurHash(i) % numKeys), i);
                ctx.collect(result);
            }
        }

        @Override
        public void cancel() {}
    }

    private static class MemorySinkFunction implements SinkFunction<Integer> {
        private static Map<Integer, Collection<Integer>> collections = new ConcurrentHashMap<>();

        private static final long serialVersionUID = -8815570195074103860L;

        private final int key;

        public MemorySinkFunction(int key) {
            this.key = key;
        }

        @Override
        public void invoke(Integer value) throws Exception {
            Collection<Integer> collection = collections.get(key);

            synchronized (collection) {
                collection.add(value);
            }
        }

        public static void registerCollection(int key, Collection<Integer> collection) {
            collections.put(key, collection);
        }

        public static void clear() {
            collections.clear();
        }
    }

    @Test
    public void testOperatorChainWithObjectReuseAndNoOutputOperators() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        DataStream<Integer> input = env.fromElements(1, 2, 3);
        input.flatMap(
                new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(value << 1);
                    }
                });
        env.execute();
    }
}
