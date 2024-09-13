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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/**
 * Test class used by the {@link org.apache.flink.test.classloading.ClassLoaderITCase}.
 *
 * <p>This class is used to test FLINK-3633
 */
public class UserCodeType {
    private static class CustomType {
        private final int value;

        public CustomType(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "CustomType(" + value + ")";
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> input = env.fromData(1, 2, 3, 4, 5);

        DataStream<CustomType> customTypes =
                input.map(
                                new MapFunction<Integer, CustomType>() {
                                    private static final long serialVersionUID =
                                            -5878758010124912128L;

                                    @Override
                                    public CustomType map(Integer integer) throws Exception {
                                        return new CustomType(integer);
                                    }
                                })
                        .rebalance();

        DataStream<Integer> result =
                customTypes.map(
                        new MapFunction<CustomType, Integer>() {
                            private static final long serialVersionUID = -7950126399899584991L;

                            @Override
                            public Integer map(CustomType value) throws Exception {
                                return value.value;
                            }
                        });

        result.sinkTo(new DiscardingSink<>());

        env.execute();
    }
}
