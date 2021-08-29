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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Harness tests for {@link RowTimeMiniBatchLatestChangeDeduplicateFunction}. */
public class RowTimeMiniBatchLatestChangeDeduplicateFunctionTest
        extends RowTimeDeduplicateFunctionTestBase {

    @Test
    public void testKeepLastRowWithoutGenerateUpdateBeforeAndWithGenerateInsert() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(false, true, true);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 11, 2L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(updateAfterRecord("book", 14, 3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepLastRowWithoutGenerateUpdateBeforeAndWithoutGenerateInsert()
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(false, false, true);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(updateAfterRecord("book", 11, 2L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(updateAfterRecord("book", 14, 3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepLastRowWithGenerateUpdateBeforeAndWithGenerateInsert() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(true, true, true);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 11, 2L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(updateBeforeRecord("book", 11, 2L));
        expectedOutput.add(updateAfterRecord("book", 14, 3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepLastRowWithGenerateUpdateBeforeAndWithoutGenerateInsert() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(true, false, true);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 11, 2L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(updateBeforeRecord("book", 11, 2L));
        expectedOutput.add(updateAfterRecord("book", 14, 3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepLastRowWithGenerateUpdateBeforeAndWithGenerateInsertAndStateTtl()
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(true, true, true);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 11, 2L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        // clear state.
        testHarness.setStateTtlProcessingTime(10);

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(insertRecord("book", 14, 3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepFirstRowWithoutGenerateUpdateBeforeAndWithGenerateInsert()
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(false, true, false);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 10, 1L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepFirstRowWithoutGenerateUpdateBeforeAndWithoutGenerateInsert()
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(false, false, false);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(updateAfterRecord("book", 10, 1L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepFirstRowWithGenerateUpdateBeforeAndWithGenerateInsert() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(true, true, false);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 10, 1L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepFirstRowWithGenerateUpdateBeforeAndWithoutGenerateInsert()
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(true, false, false);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 10, 1L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testKeepFirstRowWithGenerateUpdateBeforeAndWithGenerateInsertAndStateTtl()
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(true, true, false);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 10, 1L));
        testHarness.processElement(insertRecord("book", 11, 2L));
        testHarness.processElement(insertRecord("book", 13, 1L));
        // output is empty because bundle not trigger yet.
        Assert.assertTrue(testHarness.getOutput().isEmpty());
        // bundle trigger emit.
        testHarness.processElement(insertRecord("book", 12, 1L));
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 10, 1L));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        // clear state.
        testHarness.setStateTtlProcessingTime(10);

        testHarness.processElement(insertRecord("book", 14, 3L));
        testHarness.processElement(insertRecord("book", 15, 1L));
        // watermark trigger emit.
        testHarness.processWatermark(new Watermark(3L));
        expectedOutput.add(insertRecord("book", 15, 1L));
        expectedOutput.add(new Watermark(3L));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            boolean generateUpdateBefore, boolean generateInsert, boolean keepLastRow)
            throws Exception {
        RowTimeMiniBatchLatestChangeDeduplicateFunction func =
                new RowTimeMiniBatchLatestChangeDeduplicateFunction(
                        inputRowType,
                        serializer,
                        minTtlTime.toMilliseconds(),
                        rowTimeIndex,
                        generateUpdateBefore,
                        generateInsert,
                        keepLastRow);
        CountBundleTrigger trigger = new CountBundleTrigger<RowData>(miniBatchSize);
        KeyedMapBundleOperator<RowData, RowData, RowData, RowData> keyedMapBundleOperator =
                new KeyedMapBundleOperator(func, trigger);
        return createTestHarness(keyedMapBundleOperator);
    }
}
