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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProcTimeMiniBatchDeduplicateKeepLastRowFunction}. */
class ProcTimeMiniBatchDeduplicateKeepLastRowFunctionTest
        extends ProcTimeDeduplicateFunctionTestBase {

    private TypeSerializer<RowData> typeSerializer =
            inputRowType.createSerializer(new SerializerConfigImpl());

    private ProcTimeMiniBatchDeduplicateKeepLastRowFunction createFunction(
            boolean generateUpdateBefore, boolean generateInsert, long minRetentionTime) {

        return new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
                inputRowType,
                typeSerializer,
                minRetentionTime,
                generateUpdateBefore,
                generateInsert,
                true,
                generatedEqualiser,
                null);
    }

    private ProcTimeMiniBatchDeduplicateKeepLastRowFunction createFunctionWithFilter(
            boolean generateUpdateBefore) {

        return new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
                inputRowType,
                typeSerializer,
                minTime.toMillis(),
                generateUpdateBefore,
                false,
                false,
                generatedEqualiser,
                generatedFilterCondition);
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            ProcTimeMiniBatchDeduplicateKeepLastRowFunction func) throws Exception {
        CountBundleTrigger<Tuple2<String, String>> trigger = new CountBundleTrigger<>(3);
        KeyedMapBundleOperator op = new KeyedMapBundleOperator(func, trigger);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                op, rowKeySelector, rowKeySelector.getProducedType());
    }

    @Test
    void testWithoutGenerateUpdateBefore() throws Exception {
        ProcTimeMiniBatchDeduplicateKeepLastRowFunction func =
                createFunction(false, true, minTime.toMillis());
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 10));
        testHarness.processElement(insertRecord("book", 2L, 11));
        // output is empty because bundle not trigger yet.
        assertThat(testHarness.getOutput()).isEmpty();

        testHarness.processElement(insertRecord("book", 1L, 13));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("book", 1L, 13));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 3L, 11));

        expectedOutput.add(updateAfterRecord("book", 1L, 12));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("book", 3L, 11));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testWithoutGenerateUpdateBeforeAndInsert() throws Exception {
        ProcTimeMiniBatchDeduplicateKeepLastRowFunction func =
                createFunction(false, false, minTime.toMillis());
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 10));
        testHarness.processElement(insertRecord("book", 2L, 11));
        // output is empty because bundle not trigger yet.
        assertThat(testHarness.getOutput()).isEmpty();

        testHarness.processElement(insertRecord("book", 1L, 13));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 1L, 13));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 3L, 11));

        expectedOutput.add(updateAfterRecord("book", 1L, 12));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 3L, 11));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testWithGenerateUpdateBefore() throws Exception {
        ProcTimeMiniBatchDeduplicateKeepLastRowFunction func =
                createFunction(true, true, minTime.toMillis());
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 10));
        testHarness.processElement(insertRecord("book", 2L, 11));
        // output is empty because bundle not trigger yet.
        assertThat(testHarness.getOutput()).isEmpty();

        testHarness.processElement(insertRecord("book", 1L, 13));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("book", 1L, 13));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 3L, 11));

        // this will send UPDATE_BEFORE message to downstream
        expectedOutput.add(updateBeforeRecord("book", 1L, 13));
        expectedOutput.add(updateAfterRecord("book", 1L, 12));
        expectedOutput.add(updateBeforeRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("book", 3L, 11));
        testHarness.close();
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testWithGenerateUpdateBeforeAndStateTtl() throws Exception {
        ProcTimeMiniBatchDeduplicateKeepLastRowFunction func =
                createFunction(true, true, minTime.toMillis());
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.setup();
        testHarness.open();

        testHarness.processElement(insertRecord("book", 1L, 10));
        testHarness.processElement(insertRecord("book", 2L, 11));
        // output is empty because bundle not trigger yet.
        assertThat(testHarness.getOutput()).isEmpty();
        testHarness.processElement(insertRecord("book", 1L, 13));

        testHarness.setStateTtlProcessingTime(30);
        testHarness.processElement(insertRecord("book", 1L, 17));
        testHarness.processElement(insertRecord("book", 2L, 18));
        testHarness.processElement(insertRecord("book", 1L, 19));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("book", 1L, 13));
        // because (2L,11), (1L,13) retired, so no UPDATE_BEFORE message send to downstream
        expectedOutput.add(insertRecord("book", 1L, 19));
        expectedOutput.add(insertRecord("book", 2L, 18));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWithFilterCondition(boolean updateBefore) throws Exception {
        ProcTimeMiniBatchDeduplicateKeepLastRowFunction func =
                createFunctionWithFilter(updateBefore);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(updateAfterRecord("book", 1L, 12));
        testHarness.processElement(updateAfterRecord("book", 1L, 15));
        // output is empty because bundle not trigger yet.
        assertThat(testHarness.getOutput()).isEmpty();
        testHarness.processElement(updateAfterRecord("book", 1L, 16));
        // first bundle end

        testHarness.processElement(updateAfterRecord("book", 1L, 16));
        testHarness.processElement(updateAfterRecord("book", 1L, 18));
        testHarness.processElement(updateAfterRecord("book", 1L, 19));
        // second bundle end

        testHarness.processElement(updateAfterRecord("book", 1L, 8));
        testHarness.processElement(updateAfterRecord("book", 1L, 9));
        testHarness.processElement(updateAfterRecord("book", 1L, 7));
        // third bundle end

        testHarness.processElement(updateAfterRecord("book", 1L, 21));
        testHarness.processElement(updateAfterRecord("book", 1L, 22));
        testHarness.processElement(updateAfterRecord("book", 1L, 23));
        // fourth bundle end

        testHarness.processElement(updateAfterRecord("book", 1L, 23));
        testHarness.processElement(updateAfterRecord("book", 1L, 23));
        testHarness.processElement(deleteRecord("book", 1L, null));
        // fifth bundle end

        List<Object> expectedOutput = new ArrayList<>();
        // first bundle result
        expectedOutput.add(insertRecord("book", 1L, 16));

        // second bundle result
        if (updateBefore) {
            expectedOutput.add(updateBeforeRecord("book", 1L, 16));
        }
        expectedOutput.add(updateAfterRecord("book", 1L, 19));

        // third bundle result
        expectedOutput.add(deleteRecord("book", 1L, 19));

        // fourth bundle result
        expectedOutput.add(insertRecord("book", 1L, 23));

        // fifth bundle result
        expectedOutput.add(deleteRecord("book", 1L, 23));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }
}
