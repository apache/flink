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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link ProcTimeMiniBatchAssignerOperator}. */
public class ProcTimeMiniBatchAssignerOperatorTest extends WatermarkAssignerOperatorTestBase {

    @Test
    public void testMiniBatchAssignerOperator() throws Exception {
        final ProcTimeMiniBatchAssignerOperator operator =
                new ProcTimeMiniBatchAssignerOperator(100);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        long currentTime = 0;

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(GenericRowData.of(1L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(2L)));
        testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(3L)));
        testHarness.processElement(new StreamRecord<>(GenericRowData.of(4L)));

        {
            ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
            long currentElement = 1L;
            long lastWatermark = 0L;

            while (true) {
                if (output.size() > 0) {
                    Object next = output.poll();
                    assertThat(next).isNotNull();
                    Tuple2<Long, Long> update =
                            validateElement(next, currentElement, lastWatermark);
                    long nextElementValue = update.f0;
                    lastWatermark = update.f1;
                    if (next instanceof Watermark) {
                        assertThat(lastWatermark).isEqualTo(100);
                        break;
                    } else {
                        assertThat(nextElementValue - 1).isEqualTo(currentElement);
                        currentElement += 1;
                        assertThat(lastWatermark).isEqualTo(0);
                    }
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

        {
            ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
            long currentElement = 4L;
            long lastWatermark = 100L;

            while (true) {
                if (output.size() > 0) {
                    Object next = output.poll();
                    assertThat(next).isNotNull();
                    Tuple2<Long, Long> update =
                            validateElement(next, currentElement, lastWatermark);
                    long nextElementValue = update.f0;
                    lastWatermark = update.f1;
                    if (next instanceof Watermark) {
                        assertThat(lastWatermark).isEqualTo(200);
                        break;
                    } else {
                        assertThat(nextElementValue - 1).isEqualTo(currentElement);
                        currentElement += 1;
                        assertThat(lastWatermark).isEqualTo(100);
                    }
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
}
