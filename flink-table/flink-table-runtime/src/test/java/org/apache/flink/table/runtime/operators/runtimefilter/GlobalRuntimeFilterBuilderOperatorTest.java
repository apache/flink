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

package org.apache.flink.table.runtime.operators.runtimefilter;

import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.IntType;

import org.junit.jupiter.api.Test;

import java.util.Queue;

import static org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils.OVER_MAX_ROW_COUNT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link GlobalRuntimeFilterBuilderOperator}. */
class GlobalRuntimeFilterBuilderOperatorTest {

    @Test
    void testNormalInputAndNormalOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10)) {
            // process elements
            testHarness.processElement(
                    new StreamRecord<RowData>(
                            GenericRowData.of(5, BloomFilter.toBytes(createBloomFilter1()))));
            testHarness.processElement(
                    new StreamRecord<RowData>(
                            GenericRowData.of(5, BloomFilter.toBytes(createBloomFilter2()))));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(2);

            int globalCount = outputRowData.getInt(0);
            BloomFilter globalBloomFilter = BloomFilter.fromBytes(outputRowData.getBinary(1));
            assertThat(globalCount).isEqualTo(10);
            assertThat(globalBloomFilter.testHash("var1".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var2".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var3".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var4".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var5".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var6".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var7".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var8".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var9".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var10".hashCode())).isTrue();
            assertThat(globalBloomFilter.testHash("var11".hashCode())).isFalse();
            assertThat(globalBloomFilter.testHash("var12".hashCode())).isFalse();
            assertThat(globalBloomFilter.testHash("var13".hashCode())).isFalse();
            assertThat(globalBloomFilter.testHash("var14".hashCode())).isFalse();
            assertThat(globalBloomFilter.testHash("var15".hashCode())).isFalse();
        }
    }

    /**
     * Test the case that all input local runtime filters are normal, but the merged global filter
     * is over-max-row-count.
     */
    @Test
    void testNormalInputAndOverMaxRowCountOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(9)) {
            // process elements
            testHarness.processElement(
                    new StreamRecord<RowData>(
                            GenericRowData.of(5, BloomFilter.toBytes(createBloomFilter1()))));
            testHarness.processElement(
                    new StreamRecord<RowData>(
                            GenericRowData.of(5, BloomFilter.toBytes(createBloomFilter2()))));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(2);

            int globalCount = outputRowData.getInt(0);
            assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
            assertThat(outputRowData.isNullAt(1)).isTrue();
        }
    }

    /** Test the case that one of the input local runtime filters is over-max-row-count. */
    @Test
    void testOverMaxRowCountInput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10)) {
            // process elements
            testHarness.processElement(
                    new StreamRecord<RowData>(
                            GenericRowData.of(5, BloomFilter.toBytes(createBloomFilter1()))));
            testHarness.processElement(
                    new StreamRecord<RowData>(GenericRowData.of(OVER_MAX_ROW_COUNT, null)));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(2);

            int globalCount = outputRowData.getInt(0);
            assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
            assertThat(outputRowData.isNullAt(1)).isTrue();
        }
    }

    private static BloomFilter createBloomFilter1() {
        final BloomFilter bloomFilter1 = RuntimeFilterUtils.createOnHeapBloomFilter(10);
        bloomFilter1.addHash("var1".hashCode());
        bloomFilter1.addHash("var2".hashCode());
        bloomFilter1.addHash("var3".hashCode());
        bloomFilter1.addHash("var4".hashCode());
        bloomFilter1.addHash("var5".hashCode());
        return bloomFilter1;
    }

    private static BloomFilter createBloomFilter2() {
        final BloomFilter bloomFilter2 = RuntimeFilterUtils.createOnHeapBloomFilter(10);
        bloomFilter2.addHash("var6".hashCode());
        bloomFilter2.addHash("var7".hashCode());
        bloomFilter2.addHash("var8".hashCode());
        bloomFilter2.addHash("var9".hashCode());
        bloomFilter2.addHash("var10".hashCode());
        return bloomFilter2;
    }

    private static StreamTaskMailboxTestHarness<RowData>
            createGlobalRuntimeFilterBuilderOperatorHarness(int maxRowCount) throws Exception {
        final GlobalRuntimeFilterBuilderOperator operator =
                new GlobalRuntimeFilterBuilderOperator(maxRowCount);
        return new StreamTaskMailboxTestHarnessBuilder<>(
                        OneInputStreamTask::new,
                        InternalTypeInfo.ofFields(new IntType(), new BinaryType()))
                .setupOutputForSingletonOperatorChain(operator)
                .addInput(InternalTypeInfo.ofFields(new IntType(), new BinaryType()))
                .build();
    }
}
