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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.watermark.WatermarkUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link RowTimeMiniBatchAssginerOperator}. */
public class RowTimeMiniBatchAssginerOperatorTest extends WatermarkAssignerOperatorTestBase {

    @Test
    public void testRowTimeWatermarkAssigner() throws Exception {
        final RowTimeMiniBatchAssginerOperator operator = new RowTimeMiniBatchAssginerOperator(5);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(1L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(2L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(2));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(3L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(4L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(3));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(5L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(4));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(5));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(7L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(6));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(11L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(10));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(12));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(16L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(15));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(17L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(16));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(20L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(19));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(22L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(20));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(24L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(21));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(25L)));

        testHarness.close();

        List<WatermarkEvent> expected = new ArrayList<>();
        expected.add(WatermarkUtils.createWatermarkEventFromTimestamp(4));
        expected.add(WatermarkUtils.createWatermarkEventFromTimestamp(10));
        expected.add(WatermarkUtils.createWatermarkEventFromTimestamp(15));
        expected.add(WatermarkUtils.createWatermarkEventFromTimestamp(19));
        expected.add(
                WatermarkUtils.createWatermarkEventFromTimestamp(
                        21)); // the last buffered watermark

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        List<WatermarkEvent> watermarks = extractWatermarks(output);
        assertThat(watermarks).isEqualTo(expected);
        // verify all the records are forwarded, there are 13 records.
        assertThat(output).hasSize(expected.size() + 13);
    }

    @Test
    public void testEndWatermarkIsForwarded() throws Exception {
        final RowTimeMiniBatchAssginerOperator operator = new RowTimeMiniBatchAssginerOperator(50);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(1L)));
        testHarness.processWatermark(WatermarkUtils.createWatermarkEventFromTimestamp(2));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(50L)));
        // send end watermark
        testHarness.processWatermark(new WatermarkEvent(TimestampWatermark.MAX_WATERMARK));

        // verify that the end watermark is forwarded and the buffered watermark is not.
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        List<WatermarkEvent> watermarks = extractWatermarks(output);
        assertThat(watermarks).hasSize(1);
        assertThat(watermarks.get(0))
                .isEqualTo(new WatermarkEvent(TimestampWatermark.MAX_WATERMARK));
    }
}
