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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link MapPartitionOperator}. */
class MapPartitionOperatorTest {

    /** The test environment. */
    private Environment environment;

    /** The test stream task. */
    private StreamTask<?, ?> containingTask;

    /** The test stream config. */
    private StreamConfig config;

    @BeforeEach
    void before() throws Exception {
        environment = MockEnvironment.builder().build();
        containingTask =
                new StreamTask<Object, StreamOperator<Object>>(environment) {
                    @Override
                    protected void init() {}
                };
        config = new MockStreamConfig(new Configuration(), 1);
    }

    @Test
    void testOpen() {
        MapPartitionOperator<String, Integer> mapPartitionOperator = createMapPartitionOperator();
        MockOutput<Integer> output = new MockOutput<>(new ArrayList<>());
        mapPartitionOperator.setup(containingTask, config, output);
        assertDoesNotThrow(mapPartitionOperator::open);
    }

    @Test
    void testProcessElementAndWatermark() throws Exception {
        MapPartitionOperator<String, Integer> mapPartitionOperator = createMapPartitionOperator();
        List<Integer> outputList = new ArrayList<>();
        MockOutput<Integer> output = new MockOutput<>(outputList);
        mapPartitionOperator.setup(containingTask, config, output);
        mapPartitionOperator.open();
        mapPartitionOperator.processElement(new StreamRecord<>("Test"));
        mapPartitionOperator.processElement(new StreamRecord<>("Test"));
        mapPartitionOperator.processElement(new StreamRecord<>("Test"));
        mapPartitionOperator.endInput();
        assertThat(outputList.size()).isOne();
        assertEquals(outputList.get(0), 3);
    }

    private MapPartitionOperator<String, Integer> createMapPartitionOperator() {
        return new MapPartitionOperator<>(
                (MapPartitionFunction<String, Integer>)
                        (values, out) -> {
                            List<String> result = new ArrayList<>();
                            for (String value : values) {
                                result.add(value);
                            }
                            out.collect(result.size());
                        });
    }
}
