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

package org.apache.flink.datastream.impl.functions;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.ProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.operators.ProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoInputBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoOutputProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ProcessFunction}. */
public class ProcessFunctionTest {
    @Test
    void testOneInputStreamProcessFunctionOpenAndCloseInvoked() throws Exception {
        AtomicBoolean openInvoked = new AtomicBoolean(false);
        AtomicBoolean closeInvoked = new AtomicBoolean(false);

        OneInputStreamProcessFunction<Integer, Integer> processFunction =
                new OneInputStreamProcessFunction<>() {
                    @Override
                    public void open(NonPartitionedContext<Integer> ctx) throws Exception {
                        openInvoked.set(true);
                    }

                    @Override
                    public void processRecord(
                            Integer record,
                            Collector<Integer> output,
                            PartitionedContext<Integer> ctx)
                            throws Exception {}

                    @Override
                    public void close() throws Exception {
                        closeInvoked.set(true);
                    }
                };

        ProcessOperator<Integer, Integer> processOperator = new ProcessOperator<>(processFunction);

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            assertThat(openInvoked).isTrue();
            testHarness.close();
            assertThat(closeInvoked).isTrue();
        }
    }

    @Test
    void testTwoInputBroadcastStreamProcessFunctionOpenAndCloseInvoked() throws Exception {
        AtomicBoolean openInvoked = new AtomicBoolean(false);
        AtomicBoolean closeInvoked = new AtomicBoolean(false);

        TwoInputBroadcastStreamProcessFunction<Integer, Integer, Integer> processFunction =
                new TwoInputBroadcastStreamProcessFunction<>() {

                    @Override
                    public void open(NonPartitionedContext<Integer> ctx) throws Exception {
                        openInvoked.set(true);
                    }

                    @Override
                    public void processRecordFromNonBroadcastInput(
                            Integer record,
                            Collector<Integer> output,
                            PartitionedContext<Integer> ctx)
                            throws Exception {}

                    @Override
                    public void processRecordFromBroadcastInput(
                            Integer record, NonPartitionedContext<Integer> ctx) throws Exception {}

                    @Override
                    public void close() throws Exception {
                        closeInvoked.set(true);
                    }
                };

        TwoInputBroadcastProcessOperator<Integer, Integer, Integer> processOperator =
                new TwoInputBroadcastProcessOperator<>(processFunction);

        try (TwoInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new TwoInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            assertThat(openInvoked).isTrue();
            testHarness.close();
            assertThat(closeInvoked).isTrue();
        }
    }

    @Test
    void testTwoInputNonBroadcastStreamProcessFunctionOpenAndCloseInvoked() throws Exception {
        AtomicBoolean openInvoked = new AtomicBoolean(false);
        AtomicBoolean closeInvoked = new AtomicBoolean(false);

        TwoInputNonBroadcastStreamProcessFunction<Integer, Integer, Integer> processFunction =
                new TwoInputNonBroadcastStreamProcessFunction<>() {

                    @Override
                    public void open(NonPartitionedContext<Integer> ctx) throws Exception {
                        openInvoked.set(true);
                    }

                    @Override
                    public void processRecordFromFirstInput(
                            Integer record,
                            Collector<Integer> output,
                            PartitionedContext<Integer> ctx)
                            throws Exception {}

                    @Override
                    public void processRecordFromSecondInput(
                            Integer record,
                            Collector<Integer> output,
                            PartitionedContext<Integer> ctx)
                            throws Exception {}

                    @Override
                    public void close() throws Exception {
                        closeInvoked.set(true);
                    }
                };

        TwoInputNonBroadcastProcessOperator<Integer, Integer, Integer> processOperator =
                new TwoInputNonBroadcastProcessOperator<>(processFunction);

        try (TwoInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new TwoInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            assertThat(openInvoked).isTrue();
            testHarness.close();
            assertThat(closeInvoked).isTrue();
        }
    }

    @Test
    void testTwoOutputStreamProcessFunctionOpenAndCloseInvoked() throws Exception {
        AtomicBoolean openInvoked = new AtomicBoolean(false);
        AtomicBoolean closeInvoked = new AtomicBoolean(false);

        TwoOutputStreamProcessFunction<Integer, Integer, Integer> processFunction =
                new TwoOutputStreamProcessFunction<>() {

                    @Override
                    public void open(TwoOutputNonPartitionedContext<Integer, Integer> ctx)
                            throws Exception {
                        openInvoked.set(true);
                    }

                    @Override
                    public void processRecord(
                            Integer record,
                            Collector<Integer> output1,
                            Collector<Integer> output2,
                            TwoOutputPartitionedContext<Integer, Integer> ctx)
                            throws Exception {}

                    @Override
                    public void close() throws Exception {
                        closeInvoked.set(true);
                    }
                };

        TwoOutputProcessOperator<Integer, Integer, Integer> processOperator =
                new TwoOutputProcessOperator<>(
                        processFunction, new OutputTag<Integer>("side-output", Types.INT));

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            assertThat(openInvoked).isTrue();
            testHarness.close();
            assertThat(closeInvoked).isTrue();
        }
    }
}
