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

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/** Tests for {@link ProcTimeDeduplicateKeepFirstRowFunction}. */
public class ProcTimeDeduplicateKeepFirstRowFunctionTest
        extends ProcTimeDeduplicateFunctionTestBase {

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            ProcTimeDeduplicateKeepFirstRowFunction func) throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator = new KeyedProcessOperator<>(func);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, rowKeySelector, rowKeySelector.getProducedType());
    }

    @Test
    public void test() throws Exception {
        ProcTimeDeduplicateKeepFirstRowFunction func =
                new ProcTimeDeduplicateKeepFirstRowFunction(minTime.toMilliseconds());
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 1L, 13));
        testHarness.close();

        // Keep FirstRow in deduplicate will not send retraction
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 11));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testWithStateTtl() throws Exception {
        ProcTimeDeduplicateKeepFirstRowFunction func =
                new ProcTimeDeduplicateKeepFirstRowFunction(minTime.toMilliseconds());
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 1L, 13));

        testHarness.setStateTtlProcessingTime(30);
        testHarness.processElement(insertRecord("book", 1L, 17));
        testHarness.processElement(insertRecord("book", 2L, 18));
        testHarness.processElement(insertRecord("book", 1L, 19));

        // Keep FirstRow in deduplicate will not send retraction
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 11));
        // (1L,12),(2L,11) has retired, so output (1L,17) and (2L,18)
        expectedOutput.add(insertRecord("book", 1L, 17));
        expectedOutput.add(insertRecord("book", 2L, 18));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }
}
