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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
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
public class StreamingOperatorsITCase extends AbstractTestBaseJUnit4 {

    /**
     * Tests the basic functionality of the AsyncWaitOperator: Processing a limited stream of
     * elements by doubling their value. This is tested in for the ordered and unordered mode.
     */
    @Test
    public void testAsyncWaitOperator() throws Exception {
        final int numElements = 5;
        final long timeout = 1000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create type information for Tuple2<Integer, NonSerializable>
        TypeInformation<Tuple2<Integer, NonSerializable>> tupleTypeInfo =
                new TupleTypeInfo<>(Types.INT, TypeInformation.of(NonSerializable.class));

        // Create generator function for NonSerializable tuples
        GeneratorFunction<Long, Tuple2<Integer, NonSerializable>> generateNonSerializableTuple =
                index -> new Tuple2<>(index.intValue(), new NonSerializable(index.intValue()));

        // Create DataGeneratorSource with the generator function
        DataGeneratorSource<Tuple2<Integer, NonSerializable>> source =
                new DataGeneratorSource<>(generateNonSerializableTuple, numElements, tupleTypeInfo);

        // Create data stream using Source V2 API
        DataStream<Tuple2<Integer, NonSerializable>> input =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "NonSerializable Source")
                        .setParallelism(1);

        AsyncFunction<Tuple2<Integer, NonSerializable>, Integer> function =
                new RichAsyncFunction<Tuple2<Integer, NonSerializable>, Integer>() {
                    private static final long serialVersionUID = 7000343199829487985L;

                    transient ExecutorService executorService;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        super.open(openContext);
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

        orderedResult.sinkTo(sinkFunction1).setParallelism(1);

        DataStream<Integer> unorderedResult =
                AsyncDataStream.unorderedWait(input, function, timeout, TimeUnit.MILLISECONDS, 2);

        // save result from unordered process
        final MemorySinkFunction sinkFunction2 = new MemorySinkFunction(1);
        final List<Integer> actualResult2 = new ArrayList<>(numElements);
        MemorySinkFunction.registerCollection(1, actualResult2);

        unorderedResult.sinkTo(sinkFunction2);

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

    private static class MemorySinkFunction implements Sink<Integer> {

        private final int key;
        private static final Map<Integer, Collection<Integer>> collections =
                new ConcurrentHashMap<>();

        public MemorySinkFunction(int key) {
            this.key = key;
        }

        public static void registerCollection(int key, Collection<Integer> collection) {
            collections.put(key, collection);
        }

        public static void clear() {
            collections.clear();
        }

        @Override
        public SinkWriter<Integer> createWriter(WriterInitContext context) throws IOException {
            return new MemorySinkWriter(key);
        }

        private static class MemorySinkWriter implements SinkWriter<Integer>, Serializable {
            private final int key;
            private final Collection<Integer> collection;

            public MemorySinkWriter(int key) {
                this.key = key;
                this.collection = collections.get(key);
            }

            @Override
            public void write(Integer element, Context context) {
                synchronized (collection) {
                    collection.add(element);
                }
            }

            @Override
            public void flush(boolean endOfInput) {}

            @Override
            public void close() {}
        }
    }

    @Test
    public void testOperatorChainWithObjectReuseAndNoOutputOperators() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        DataStream<Integer> input = env.fromData(1, 2, 3);
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
