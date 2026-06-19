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

package org.apache.flink.streaming.api.watermark.generalized;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;
import org.apache.flink.streaming.runtime.watermark.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.AlignedWatermarkCombiner;
import org.apache.flink.streaming.runtime.watermark.InternalBoolWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.InternalLongWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.WatermarkCombiner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WatermarkCombiner}. */
class WatermarkCombinerTest {

    private static final String DEFAULT_WATERMARK_IDENTIFIER = "default";
    private List<Watermark> receivedWatermarks = new ArrayList<>();
    private WatermarkCombiner combiner;

    @BeforeEach
    public void setup() {
        receivedWatermarks.clear();
        combiner = null;
    }

    @Test
    void testAlignedWatermarkCombiner() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | gateNotified | receivedWatermarkValues
        // -----------------------------------------------------------------------------
        //     1   |     1     |           |    false     |    []
        //     2   |           |     1     |    true      |    [1]
        //     3   |           |     2     |    false     |    [1]
        //     4   |     2     |           |    true      |    [1, 2]
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the watermark with value 1.
        // The expected result of this step is the gateNotified should be set to true, and the
        // receivedWatermarks should contain one watermark with a value of 1.

        InternalLongWatermarkDeclaration watermarkDeclaration =
                new InternalLongWatermarkDeclaration(
                        DEFAULT_WATERMARK_IDENTIFIER,
                        new WatermarkCombinationPolicy(
                                WatermarkCombinationFunction.NumericWatermarkCombinationFunction
                                        .MIN,
                                true),
                        WatermarkHandlingStrategy.FORWARD,
                        true);
        AtomicBoolean gateNotified = new AtomicBoolean(false);
        combiner = new AlignedWatermarkCombiner(2, () -> gateNotified.set(true));

        // step 1
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(1L), 0);
        assertThat(gateNotified.get()).isFalse();

        // step 2
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(1L), 1, 1L);
        assertThat(gateNotified.get()).isTrue();

        // clear gateNotified
        gateNotified.set(false);

        // step 3
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(2L), 1, 1L);
        assertThat(gateNotified.get()).isFalse();

        // step 4
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(2L), 0, 1L, 2L);
        assertThat(gateNotified.get()).isTrue();
    }

    @Test
    void testLongWatermarkCombinerWaitForAllChannels() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | receivedWatermarkValues
        // -----------------------------------------------------------------------------
        //     1   |     3     |           |    []
        //     2   |     2     |           |    []
        //     3   |           |     1     |    [2]
        //     4   |           |     3     |    [2, 3]
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the watermark with value 3.
        // The expected result of this step is the receivedWatermarks should be empty.

        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .combineWaitForAllChannels(true)
                        .build();
        InternalLongWatermarkDeclaration internalWatermarkDeclaration =
                (InternalLongWatermarkDeclaration)
                        AbstractInternalWatermarkDeclaration.from(watermarkDeclaration);
        combiner = internalWatermarkDeclaration.createWatermarkCombiner(2, null);

        // step 1
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(3L), 0);

        // step 2
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(2L), 0);

        // step 3
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(1L), 1, 2L);

        // step 4
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(3L), 0, 2L, 3L);
    }

    @Test
    void testLongWatermarkCombinerCombineMax() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | receivedWatermarkValues
        // -----------------------------------------------------------------------------
        //     1   |     1     |           |    [1]
        //     2   |     2     |           |    [1, 2]
        //     3   |           |     2     |    [1, 2]
        //     4   |           |     3     |    [1, 2, 3]
        //     5   |           |     2     |    [1, 2, 3, 2]
        //     6   |     2     |           |    [1, 2, 3, 2]
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the watermark with value 3.
        // The expected result of this step is and the receivedWatermarks should contain one
        // watermark with a value of 1.

        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .build();
        InternalLongWatermarkDeclaration internalWatermarkDeclaration =
                (InternalLongWatermarkDeclaration)
                        AbstractInternalWatermarkDeclaration.from(watermarkDeclaration);
        combiner = internalWatermarkDeclaration.createWatermarkCombiner(2, null);

        // step 1
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(1L), 0, 1L);

        // step 2
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(2L), 0, 1L, 2L);

        // step 3
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(2L), 1, 1L, 2L);

        // step 4
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(3L), 1, 1L, 2L, 3L);

        // step 5
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(2L), 1, 1L, 2L, 3L, 2L);

        // step 6
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(2L), 0, 1L, 2L, 3L, 2L);
    }

    @Test
    void testLongWatermarkCombinerCombineMin() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | receivedWatermarkValues
        // -----------------------------------------------------------------------------
        //     1   |     2     |           |    [2]
        //     2   |           |     1     |    [2, 1]
        //     3   |     3     |           |    [2, 1]
        //     4   |           |     1     |    [2, 1]
        //     5   |           |     2     |    [2, 1, 2]
        //     6   |           |     4     |    [2, 1, 2, 3]
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the watermark with value 2.
        // The expected result of this step is and the receivedWatermarks should contain one
        // watermark with a value of 2.

        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMin()
                        .build();
        InternalLongWatermarkDeclaration internalWatermarkDeclaration =
                (InternalLongWatermarkDeclaration)
                        AbstractInternalWatermarkDeclaration.from(watermarkDeclaration);
        combiner = internalWatermarkDeclaration.createWatermarkCombiner(2, null);

        // step 1
        executeAndCheckCombineStepWithLongWatermark(watermarkDeclaration.newWatermark(2L), 0, 2L);

        // step 2
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(1L), 1, 2L, 1L);

        // step 3
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(3L), 0, 2L, 1L);

        // step 4
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(1L), 1, 2L, 1L);

        // step 5
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(2L), 1, 2L, 1L, 2L);

        // step 6
        executeAndCheckCombineStepWithLongWatermark(
                watermarkDeclaration.newWatermark(4L), 1, 2L, 1L, 2L, 3L);
    }

    @Test
    void testBoolWatermarkCombinerWaitForAllChannels() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | receivedWatermarkValues
        // -----------------------------------------------------------------------------
        //     1   |   true    |           |    []
        //     2   |   true    |           |    []
        //     3   |           |   false   |    [false]
        //     4   |           |   true    |    [false, true]
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the watermark with value true.
        // The expected result of this step is the receivedWatermarks should be empty.

        BoolWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeBool()
                        .combineFunctionAND()
                        .combineWaitForAllChannels(true)
                        .build();
        InternalBoolWatermarkDeclaration internalWatermarkDeclaration =
                (InternalBoolWatermarkDeclaration)
                        AbstractInternalWatermarkDeclaration.from(watermarkDeclaration);
        combiner = internalWatermarkDeclaration.createWatermarkCombiner(2, null);

        // step 1
        executeAndCheckCombineStepWithBoolWatermark(watermarkDeclaration.newWatermark(true), 0);

        // step 2
        executeAndCheckCombineStepWithBoolWatermark(watermarkDeclaration.newWatermark(true), 0);

        // step 3
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(false), 1, false);

        // step 4
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(true), 1, false, true);
    }

    @Test
    void testBoolWatermarkCombinerCombineAnd() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | receivedWatermarkValues
        // -----------------------------------------------------------------------------
        //     1   |   true    |           |    [true]
        //     2   |           |   false   |    [true, false]
        //     3   |           |   true    |    [true, false, true]
        //     4   |   true    |           |    [true, false, true]
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the watermark with value true.
        // The expected result of this step is and the receivedWatermarks should contain one
        // watermark with a value of true.

        BoolWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeBool()
                        .combineFunctionAND()
                        .build();
        InternalBoolWatermarkDeclaration internalWatermarkDeclaration =
                (InternalBoolWatermarkDeclaration)
                        AbstractInternalWatermarkDeclaration.from(watermarkDeclaration);
        combiner = internalWatermarkDeclaration.createWatermarkCombiner(2, null);

        // step 1
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(true), 0, true);

        // step 2
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(false), 1, true, false);

        // step 3
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(true), 1, true, false, true);

        // step 4
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(true), 0, true, false, true);
    }

    @Test
    void testBoolWatermarkCombinerCombineOr() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | receivedWatermarkValues
        // -----------------------------------------------------------------------------
        //     1   |   true    |           |    [true]
        //     2   |           |   false   |    [true]
        //     3   |           |   true    |    [true]
        //     4   |           |   false   |    [true]
        //     5   |   false   |           |    [true, false]
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the watermark with value 2.
        // The expected result of this step is and the receivedWatermarks should contain one
        // watermark with a value of 2.

        BoolWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeBool()
                        .combineFunctionOR()
                        .build();
        InternalBoolWatermarkDeclaration internalWatermarkDeclaration =
                (InternalBoolWatermarkDeclaration)
                        AbstractInternalWatermarkDeclaration.from(watermarkDeclaration);
        combiner = internalWatermarkDeclaration.createWatermarkCombiner(2, null);

        // step 1
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(true), 0, true);

        // step 2
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(false), 1, true);

        // step 3
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(true), 1, true);

        // step 4
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(false), 1, true);

        // step 5
        executeAndCheckCombineStepWithBoolWatermark(
                watermarkDeclaration.newWatermark(false), 0, true, false);
    }

    private void executeAndCheckCombineStepWithLongWatermark(
            LongWatermark watermark, int channelIndex, Long... expectedReceivedWatermarkValues)
            throws Exception {
        combiner.combineWatermark(watermark, channelIndex, receivedWatermarks::add);
        assertThat(receivedWatermarks.stream().map(w -> ((LongWatermark) w).getValue()))
                .containsExactly(expectedReceivedWatermarkValues);
    }

    private void executeAndCheckCombineStepWithBoolWatermark(
            BoolWatermark watermark, int channelIndex, Boolean... expectedReceivedWatermarkValues)
            throws Exception {
        combiner.combineWatermark(watermark, channelIndex, receivedWatermarks::add);
        assertThat(receivedWatermarks.stream().map(w -> ((BoolWatermark) w).getValue()))
                .containsExactly(expectedReceivedWatermarkValues);
    }
}
