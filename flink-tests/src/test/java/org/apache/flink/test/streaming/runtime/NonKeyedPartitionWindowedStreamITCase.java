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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.NonKeyedPartitionWindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link NonKeyedPartitionWindowedStream}. */
class NonKeyedPartitionWindowedStreamITCase {

    private static final int EVENT_NUMBER = 100;

    private static final String TEST_EVENT = "Test";

    @Test
    void testMapPartition() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromData(createSource());
        int parallelism = 2;
        CloseableIterator<String> resultIterator =
                source.map(v -> v)
                        .setParallelism(parallelism)
                        .fullWindowPartition()
                        .mapPartition(
                                new MapPartitionFunction<String, String>() {
                                    @Override
                                    public void mapPartition(
                                            Iterable<String> values, Collector<String> out) {
                                        StringBuilder sb = new StringBuilder();
                                        for (String value : values) {
                                            sb.append(value);
                                        }
                                        out.collect(sb.toString());
                                    }
                                })
                        .executeAndCollect();
        String expectedResult = createExpectedString(EVENT_NUMBER / parallelism);
        expectInAnyOrder(resultIterator, expectedResult, expectedResult);
    }

    private void expectInAnyOrder(CloseableIterator<String> resultIterator, String... expected) {
        List<String> listExpected = Lists.newArrayList(expected);
        List<String> testResults = Lists.newArrayList(resultIterator);
        Collections.sort(listExpected);
        Collections.sort(testResults);
        assertThat(testResults).isEqualTo(listExpected);
    }

    private Collection<String> createSource() {
        ArrayList<String> source = new ArrayList<>();
        for (int index = 0; index < EVENT_NUMBER; ++index) {
            source.add(TEST_EVENT);
        }
        return source;
    }

    private String createExpectedString(int number) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int index = 0; index < number; ++index) {
            stringBuilder.append(TEST_EVENT);
        }
        return stringBuilder.toString();
    }
}
