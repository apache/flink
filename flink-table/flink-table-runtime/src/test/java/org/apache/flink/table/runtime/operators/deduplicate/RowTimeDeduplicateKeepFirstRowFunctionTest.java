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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.assertj.core.api.Assertions.assertThat;

/** Harness tests for {@link RowTimeDeduplicateKeepFirstRowFunction}. */
public class RowTimeDeduplicateKeepFirstRowFunctionTest extends RowTimeDeduplicateFunctionTestBase {

    @Test
    public void testRowTimeDeduplicateKeepFirstRow() throws Exception {
        List<Object> expectedOutput = new ArrayList<>();
        RowTimeDeduplicateKeepFirstRowFunction deduplicateFunction =
                new RowTimeDeduplicateKeepFirstRowFunction(
                        inputRowType, minTtlTime.toMillis(), rowTimeIndex);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(new KeyedProcessOperator<>(deduplicateFunction));

        List<Object> actualOutput = new ArrayList<>();
        testHarness.open();

        testHarness.processWatermark(new Watermark(50));
        // ignore late records
        assertThat(deduplicateFunction.getNumLateRecordsDropped().getCount()).isEqualTo(0);
        testHarness.processElement(insertRecord("key1", 0, 1L));
        expectedOutput.add(new Watermark(50));
        assertThat(deduplicateFunction.getNumLateRecordsDropped().getCount()).isEqualTo(1);

        testHarness.processElement(insertRecord("key1", 14, 101L));
        testHarness.processElement(insertRecord("key1", 13, 99L));
        testHarness.processElement(insertRecord("key1", 15, 99L));
        testHarness.processElement(insertRecord("key1", 12, 100L));
        testHarness.processElement(insertRecord("key2", 11, 101L));

        // test 1: keep first row with row time
        testHarness.processWatermark(new Watermark(102));
        actualOutput.addAll(testHarness.getOutput());
        expectedOutput.add(record(RowKind.INSERT, "key1", 13, 99L));
        expectedOutput.add(record(RowKind.INSERT, "key2", 11, 101L));
        expectedOutput.add(new Watermark(102));
        assertThat(deduplicateFunction.getNumLateRecordsDropped().getCount()).isEqualTo(1);

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        deduplicateFunction =
                new RowTimeDeduplicateKeepFirstRowFunction(
                        inputRowType, minTtlTime.toMillis(), rowTimeIndex);
        testHarness = createTestHarness(new KeyedProcessOperator<>(deduplicateFunction));

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(insertRecord("key1", 12, 300L));
        testHarness.processElement(insertRecord("key2", 11, 301L));
        testHarness.processElement(insertRecord("key3", 5, 299L));

        // test 2:  load snapshot state
        testHarness.processWatermark(new Watermark(302));
        expectedOutput.add(record(RowKind.INSERT, "key3", 5, 299L));
        expectedOutput.add(new Watermark(302));

        // test 3: expire the state
        testHarness.setStateTtlProcessingTime(minTtlTime.toMillis() + 1);
        testHarness.processElement(insertRecord("key1", 12, 400L));
        testHarness.processElement(insertRecord("key2", 11, 401L));
        // previously emitted records have expired so new records should be emitted
        testHarness.processWatermark(402);
        expectedOutput.add(record(RowKind.INSERT, "key1", 12, 400L));
        expectedOutput.add(record(RowKind.INSERT, "key2", 11, 401L));
        expectedOutput.add(new Watermark(402));

        // test 4: expire the state again to test ttl not losing records
        testHarness.setStateTtlProcessingTime(2 * minTtlTime.toMillis() + 1);
        testHarness.processElement(insertRecord("key1", 22, 500L));
        testHarness.processElement(insertRecord("key2", 21, 501L));
        // we test that ttl doesn't expire records that are still in not-yet-fired timers
        testHarness.setStateTtlProcessingTime(3 * minTtlTime.toMillis() + 1);
        // previously emitted records have expired so new records should be emitted
        testHarness.processWatermark(502);
        expectedOutput.add(record(RowKind.INSERT, "key1", 22, 500L));
        expectedOutput.add(record(RowKind.INSERT, "key2", 21, 501L));
        expectedOutput.add(new Watermark(502));

        actualOutput.addAll(testHarness.getOutput());

        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, actualOutput);
        testHarness.close();
    }
}
