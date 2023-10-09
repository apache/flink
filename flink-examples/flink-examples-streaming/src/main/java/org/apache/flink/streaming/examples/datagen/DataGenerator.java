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

package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/** An example for generating data with a {@link DataGeneratorSource}. */
public class DataGenerator {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        GeneratorFunction<Long, MyType> generatorFunction =
                index -> new MyType("Number: " + index, Arrays.asList(1, 2, 3));

        //        GeneratorFunction<Long, String> generatorFunction1 =
        //                new GeneratorFunction<Long, String>() {
        //                    @Override
        //                    public String map(Long value) throws Exception {
        //                        return "bla";
        //                    }
        //                };
        //
        //        TypeInformation<String> typeInfo =
        //                TypeExtractor.createTypeInfo(
        //                        generatorFunction,
        //                        GeneratorFunction.class,
        //                        generatorFunction.getClass(),
        //                        1);

        //        DataGeneratorSource<String> generatorSource =
        //                new DataGeneratorSource<>(
        //                        generatorFunction,
        //                        Long.MAX_VALUE,
        //                        RateLimiterStrategy.perSecond(4),
        //                        Types.STRING);

        DataGeneratorSource<MyType> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction, Long.MAX_VALUE, RateLimiterStrategy.perSecond(4));

        DataStreamSource<MyType> streamSource =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");
        streamSource.print();

        env.execute("Data Generator Source Example");
    }

    private static class MyType {
        String x;
        List<Integer> y;

        public MyType(String x, List<Integer> y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public String toString() {
            return x + " -> " + y;
        }
    }
}
