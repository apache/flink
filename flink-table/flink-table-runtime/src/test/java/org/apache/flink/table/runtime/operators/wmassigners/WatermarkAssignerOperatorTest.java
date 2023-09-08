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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests of {@link WatermarkAssignerOperator}. */
public class WatermarkAssignerOperatorTest extends WatermarkAssignerOperatorTestBase {

    private static final WatermarkGenerator WATERMARK_GENERATOR =
            new BoundedOutOfOrderWatermarkGenerator(0, 1);

    @Test
    public void testWatermarkAssignerWithIdleSource() throws Exception {
        // with timeout 1000 ms
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(0, WATERMARK_GENERATOR, 1000);
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);
        testHarness.open();

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        List<Object> expectedOutput = new ArrayList<>();

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(1L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(2L)));
        testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(3L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(4L)));

        // trigger watermark emit
        testHarness.setProcessingTime(51);
        expectedOutput.add(new Watermark(3));
        assertThat(filterOutRecords(output)).isEqualTo(expectedOutput);

        testHarness.setProcessingTime(1001);
        expectedOutput.add(WatermarkStatus.IDLE);
        assertThat(filterOutRecords(output)).isEqualTo(expectedOutput);

        expectedOutput.add(WatermarkStatus.ACTIVE);
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(4L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(5L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(6L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(7L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(8L)));

        testHarness.setProcessingTime(1060);
        expectedOutput.add(new Watermark(7));
        assertThat(filterOutRecords(output)).isEqualTo(expectedOutput);
    }

    @Test
    public void testWatermarkAssignerOperator() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(0, WATERMARK_GENERATOR, -1);

        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        long currentTime = 0;

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(1L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(2L)));
        testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(3L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(4L)));

        // validate first part of the sequence. we poll elements until our
        // watermark updates to "3", which must be the result of the "4" element.
        {
            ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
            long nextElementValue = 1L;
            long lastWatermark = -1L;

            while (lastWatermark < 3) {
                if (output.size() > 0) {
                    Object next = output.poll();
                    assertThat(next).isNotNull();
                    Tuple2<Long, Long> update =
                            validateElement(next, nextElementValue, lastWatermark);
                    nextElementValue = update.f0;
                    lastWatermark = update.f1;

                    // check the invariant
                    assertThat(lastWatermark).isLessThan(nextElementValue);
                } else {
                    currentTime = currentTime + 10;
                    testHarness.setProcessingTime(currentTime);
                }
            }

            output.clear();
        }

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(4L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(5L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(6L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(7L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(8L)));

        // validate the next part of the sequence. we poll elements until our
        // watermark updates to "7", which must be the result of the "8" element.
        {
            ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
            long nextElementValue = 4L;
            long lastWatermark = 2L;

            while (lastWatermark < 7) {
                if (output.size() > 0) {
                    Object next = output.poll();
                    assertThat(next).isNotNull();
                    Tuple2<Long, Long> update =
                            validateElement(next, nextElementValue, lastWatermark);
                    nextElementValue = update.f0;
                    lastWatermark = update.f1;

                    // check the invariant
                    assertThat(lastWatermark).isLessThan(nextElementValue);
                } else {
                    currentTime = currentTime + 10;
                    testHarness.setProcessingTime(currentTime);
                }
            }

            output.clear();
        }

        testHarness.processWatermark(new Watermark(Long.MAX_VALUE));
        assertThat(((Watermark) testHarness.getOutput().poll()).getTimestamp())
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testCustomizedWatermarkGenerator() throws Exception {
        MyWatermarkGenerator.openCalled = false;
        MyWatermarkGenerator.closeCalled = false;
        WatermarkGenerator generator = new MyWatermarkGenerator(1);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(0, generator, -1);

        testHarness.getExecutionConfig().setAutoWatermarkInterval(5);

        long currentTime = 0;
        List<Watermark> expected = new ArrayList<>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(1L, 0L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(2L, 1L)));
        testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(3L, 1L)));
        currentTime = currentTime + 5;
        testHarness.setProcessingTime(currentTime);
        expected.add(new Watermark(1L));

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(4L, 2L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(2L, 1L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(1L, 0L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(6L, null)));
        currentTime = currentTime + 5;
        testHarness.setProcessingTime(currentTime);
        expected.add(new Watermark(2L));

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(9L, 8L)));
        expected.add(new Watermark(8L));

        // no watermark output
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(8L, 7L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(10L, null)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(11L, 10L)));
        currentTime = currentTime + 5;
        testHarness.setProcessingTime(currentTime);
        expected.add(new Watermark(10L));

        testHarness.close();
        expected.add(Watermark.MAX_WATERMARK);

        // num_watermark + num_records
        assertThat(testHarness.getOutput()).hasSize(expected.size() + 11);
        List<Watermark> results = extractWatermarks(testHarness.getOutput());
        assertThat(results).isEqualTo(expected);
        assertThat(MyWatermarkGenerator.openCalled).isTrue();
        assertThat(MyWatermarkGenerator.closeCalled).isTrue();
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            int rowtimeFieldIndex, WatermarkGenerator watermarkGenerator, long idleTimeout)
            throws Exception {

        return new OneInputStreamOperatorTestHarness<>(
                new WatermarkAssignerOperatorFactory(
                        rowtimeFieldIndex,
                        idleTimeout,
                        new GeneratedWatermarkGenerator(
                                watermarkGenerator.getClass().getName(), "", new Object[] {}) {
                            @Override
                            public WatermarkGenerator newInstance(ClassLoader classLoader) {
                                return watermarkGenerator;
                            }

                            public WatermarkGenerator newInstance(
                                    ClassLoader classLoader, Object... args) {
                                return watermarkGenerator;
                            }
                        }));
    }

    /**
     * The special watermark generator will generate null watermarks and also checks open&close are
     * called.
     */
    private static final class MyWatermarkGenerator extends WatermarkGenerator {

        private static final long serialVersionUID = 1L;
        private static boolean openCalled = false;
        private static boolean closeCalled = false;

        private final int watermarkFieldIndex;

        private MyWatermarkGenerator(int watermarkFieldIndex) {
            this.watermarkFieldIndex = watermarkFieldIndex;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            if (closeCalled) {
                fail("Close called before open.");
            }
            openCalled = true;
        }

        @Nullable
        @Override
        public Long currentWatermark(RowData row) throws Exception {
            if (!openCalled) {
                fail("Open was not called before run.");
            }
            if (row.isNullAt(watermarkFieldIndex)) {
                return null;
            } else {
                return row.getLong(watermarkFieldIndex);
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (!openCalled) {
                fail("Open was not called before close.");
            }
            closeCalled = true;
        }
    }
}
