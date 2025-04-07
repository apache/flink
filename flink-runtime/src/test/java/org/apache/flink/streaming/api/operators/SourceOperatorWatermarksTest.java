/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.util.MockOutput;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link SourceOperator} watermark alignment. */
@SuppressWarnings("serial")
class SourceOperatorWatermarksTest {

    @Nullable private SourceOperatorTestContext context;
    @Nullable private SourceOperator<Integer, MockSourceSplit> operator;

    @BeforeEach
    void setup() throws Exception {
        context =
                new SourceOperatorTestContext(
                        false,
                        true,
                        WatermarkStrategy.forGenerator(
                                        ctx ->
                                                new SourceOperatorAlignmentTest
                                                        .PunctuatedGenerator())
                                .withTimestampAssigner((r, t) -> r),
                        new MockOutput<>(new ArrayList<>()));
        operator = context.getOperator();
    }

    @AfterEach
    void tearDown() throws Exception {
        context.close();
        context = null;
        operator = null;
    }

    @Test
    void testPerPartitionWatermarksAfterRecovery() throws Exception {
        List<MockSourceSplit> initialSplits = new ArrayList<>();
        initialSplits.add(new MockSourceSplit(0).addRecord(1042).addRecord(1044));
        initialSplits.add(new MockSourceSplit(1).addRecord(42).addRecord(44));
        operator.initializeState(context.createStateContext(initialSplits));
        operator.open();

        CollectingDataOutput<Integer> actualOutput = new CollectingDataOutput<>();

        // after emitting the first element from the first split, there can not be watermark
        // emitted, as a watermark from the other split is still unknown.
        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        assertNoWatermarks(actualOutput);

        // after emitting two more elements (in this order: [1042, 1044, 42] but order doesn't
        // matter for this test), three in total, watermark 42 can be finally emitted
        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        assertWatermark(actualOutput, new Watermark(42));
    }

    private static void assertNoWatermarks(CollectingDataOutput<Integer> actualOutput) {
        assertThat(actualOutput.getEvents()).noneMatch(element -> element instanceof Watermark);
    }

    private void assertWatermark(CollectingDataOutput<Integer> actualOutput, Watermark watermark) {
        assertThat(actualOutput.getEvents()).containsOnlyOnce(watermark);
    }
}
