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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link SortingDataInput}.
 *
 * <p>These are rather simple unit tests. See also {@link LargeSortingDataInputITCase} for more
 * thorough tests.
 */
public class SortingDataInputTest {
    @Test
    public void simpleFixedLengthKeySorting() throws Exception {
        CollectingDataOutput<Integer> collectingDataOutput = new CollectingDataOutput<>();
        CollectionDataInput<Integer> input =
                new CollectionDataInput<>(
                        Arrays.asList(
                                new StreamRecord<>(1, 3),
                                new StreamRecord<>(1, 1),
                                new StreamRecord<>(2, 1),
                                new StreamRecord<>(2, 3),
                                new StreamRecord<>(1, 2),
                                new StreamRecord<>(2, 2)));
        MockEnvironment environment = MockEnvironment.builder().build();
        SortingDataInput<Integer, Integer> sortingDataInput =
                new SortingDataInput<>(
                        input,
                        new IntSerializer(),
                        new IntSerializer(),
                        (KeySelector<Integer, Integer>) value -> value,
                        environment.getMemoryManager(),
                        environment.getIOManager(),
                        true,
                        1.0,
                        new Configuration(),
                        new DummyInvokable(),
                        new ExecutionConfig());

        DataInputStatus inputStatus;
        do {
            inputStatus = sortingDataInput.emitNext(collectingDataOutput);
        } while (inputStatus != DataInputStatus.END_OF_INPUT);

        assertThat(
                collectingDataOutput.events,
                equalTo(
                        Arrays.asList(
                                new StreamRecord<>(1, 1),
                                new StreamRecord<>(1, 2),
                                new StreamRecord<>(1, 3),
                                new StreamRecord<>(2, 1),
                                new StreamRecord<>(2, 2),
                                new StreamRecord<>(2, 3))));
    }

    @Test
    public void watermarkPropagation() throws Exception {
        CollectingDataOutput<Integer> collectingDataOutput = new CollectingDataOutput<>();
        CollectionDataInput<Integer> input =
                new CollectionDataInput<>(
                        Arrays.asList(
                                new StreamRecord<>(1, 3),
                                new Watermark(1),
                                new StreamRecord<>(1, 1),
                                new Watermark(2),
                                new StreamRecord<>(2, 1),
                                new Watermark(3),
                                new StreamRecord<>(2, 3),
                                new Watermark(4),
                                new StreamRecord<>(1, 2),
                                new Watermark(5),
                                new StreamRecord<>(2, 2),
                                new Watermark(6)));
        MockEnvironment environment = MockEnvironment.builder().build();
        SortingDataInput<Integer, Integer> sortingDataInput =
                new SortingDataInput<>(
                        input,
                        new IntSerializer(),
                        new IntSerializer(),
                        (KeySelector<Integer, Integer>) value -> value,
                        environment.getMemoryManager(),
                        environment.getIOManager(),
                        true,
                        1.0,
                        new Configuration(),
                        new DummyInvokable(),
                        new ExecutionConfig());

        DataInputStatus inputStatus;
        do {
            inputStatus = sortingDataInput.emitNext(collectingDataOutput);
        } while (inputStatus != DataInputStatus.END_OF_INPUT);

        assertThat(
                collectingDataOutput.events,
                equalTo(
                        Arrays.asList(
                                new StreamRecord<>(1, 1),
                                new StreamRecord<>(1, 2),
                                new StreamRecord<>(1, 3),
                                new StreamRecord<>(2, 1),
                                new StreamRecord<>(2, 2),
                                new StreamRecord<>(2, 3),
                                new Watermark(6))));
    }

    @Test
    public void simpleVariableLengthKeySorting() throws Exception {
        CollectingDataOutput<Integer> collectingDataOutput = new CollectingDataOutput<>();
        CollectionDataInput<Integer> input =
                new CollectionDataInput<>(
                        Arrays.asList(
                                new StreamRecord<>(1, 3),
                                new StreamRecord<>(1, 1),
                                new StreamRecord<>(2, 1),
                                new StreamRecord<>(2, 3),
                                new StreamRecord<>(1, 2),
                                new StreamRecord<>(2, 2)));
        MockEnvironment environment = MockEnvironment.builder().build();
        SortingDataInput<Integer, String> sortingDataInput =
                new SortingDataInput<>(
                        input,
                        new IntSerializer(),
                        new StringSerializer(),
                        (KeySelector<Integer, String>) value -> "" + value,
                        environment.getMemoryManager(),
                        environment.getIOManager(),
                        true,
                        1.0,
                        new Configuration(),
                        new DummyInvokable(),
                        new ExecutionConfig());

        DataInputStatus inputStatus;
        do {
            inputStatus = sortingDataInput.emitNext(collectingDataOutput);
        } while (inputStatus != DataInputStatus.END_OF_INPUT);

        assertThat(
                collectingDataOutput.events,
                equalTo(
                        Arrays.asList(
                                new StreamRecord<>(1, 1),
                                new StreamRecord<>(1, 2),
                                new StreamRecord<>(1, 3),
                                new StreamRecord<>(2, 1),
                                new StreamRecord<>(2, 2),
                                new StreamRecord<>(2, 3))));
    }
}
