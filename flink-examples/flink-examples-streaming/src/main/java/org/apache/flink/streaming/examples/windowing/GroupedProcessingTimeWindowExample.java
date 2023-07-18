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

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/** An example of grouped stream windowing into sliding time windows. */
public class GroupedProcessingTimeWindowExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final long numElementsPerParallel = 20000000;
        final long numKeys = 10000;

        GeneratorFunction<Long, Tuple2<Long, Long>> generatorFunction =
                new DataGeneratorFunction(numElementsPerParallel, numKeys);

        DataGeneratorSource<Tuple2<Long, Long>> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        numElementsPerParallel * env.getParallelism(),
                        Types.TUPLE(Types.LONG, Types.LONG));

        DataStream<Tuple2<Long, Long>> stream =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

        stream.keyBy(value -> value.f0)
                .window(
                        SlidingProcessingTimeWindows.of(
                                Time.milliseconds(2500), Time.milliseconds(500)))
                .reduce(new SummingReducer())

                // alternative: use a apply function which does not pre-aggregate
                //			.keyBy(new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>())
                //			.window(SlidingProcessingTimeWindows.of(Time.milliseconds(2500),
                // Time.milliseconds(500)))
                //			.apply(new SummingWindowFunction())

                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    private static class FirstFieldKeyExtractor<Type extends Tuple, Key>
            implements KeySelector<Type, Key> {

        @Override
        @SuppressWarnings("unchecked")
        public Key getKey(Type value) {
            return (Key) value.getField(0);
        }
    }

    private static class SummingWindowFunction
            implements WindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, Window> {

        @Override
        public void apply(
                Long key,
                Window window,
                Iterable<Tuple2<Long, Long>> values,
                Collector<Tuple2<Long, Long>> out) {
            long sum = 0L;
            for (Tuple2<Long, Long> value : values) {
                sum += value.f1;
            }

            out.collect(new Tuple2<>(key, sum));
        }
    }

    private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }

    /**
     * This class represents a data generator function that generates a stream of tuples. Each tuple
     * contains a key and a value. The function measures and prints the time it takes to generate
     * numElements. The key space is limited to numKeys. The value is always 1.
     */
    private static class DataGeneratorFunction
            implements GeneratorFunction<Long, Tuple2<Long, Long>> {

        private final long numElements;
        private final long numKeys;
        private long startTime;

        public DataGeneratorFunction(long numElements, long numKeys) {
            this.numElements = numElements;
            this.numKeys = numKeys;
        }

        @Override
        public Tuple2<Long, Long> map(Long value) throws Exception {
            if ((value % numElements) == 0) {
                startTime = System.currentTimeMillis();
            }
            if ((value % numElements + 1) == numElements) {
                final long endTime = System.currentTimeMillis();
                System.out.println(
                        Thread.currentThread()
                                + ": Took "
                                + (endTime - startTime)
                                + " msecs for "
                                + numElements
                                + " values");
            }
            return new Tuple2<>(value % numKeys, 1L);
        }
    }
}
