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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Queue;

import static org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils.OVER_MAX_ROW_COUNT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link LocalRuntimeFilterBuilderOperator}. */
public class LocalRuntimeFilterBuilderOperatorTest implements Serializable {

    @Test
    void testNormalOutput() throws Exception {
        // create test harness and process input elements
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createLocalRuntimeFilterBuilderOperatorHarnessAndProcessElements(5, 10)) {

            // test the output bloom filter
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(2);

            int actualCount = outputRowData.getInt(0);
            BloomFilter bloomFilter = BloomFilter.fromBytes(outputRowData.getBinary(1));
            assertThat(actualCount).isEqualTo(5);
            // test elements that should exist
            assertThat(bloomFilterTestString(bloomFilter, "var1")).isTrue();
            assertThat(bloomFilterTestString(bloomFilter, "var2")).isTrue();
            assertThat(bloomFilterTestString(bloomFilter, "var3")).isTrue();
            assertThat(bloomFilterTestString(bloomFilter, "var4")).isTrue();
            assertThat(bloomFilterTestString(bloomFilter, "var5")).isTrue();
            // test elements that should not exist
            assertThat(bloomFilterTestString(bloomFilter, "var6")).isFalse();
            assertThat(bloomFilterTestString(bloomFilter, "var7")).isFalse();
            assertThat(bloomFilterTestString(bloomFilter, "var8")).isFalse();
            assertThat(bloomFilterTestString(bloomFilter, "var9")).isFalse();
            assertThat(bloomFilterTestString(bloomFilter, "var10")).isFalse();
        }
    }

    /** Test the case that the output filter is over-max-row-count. */
    @Test
    void testOverMaxRowCountOutput() throws Exception {
        // create test harness and process input elements
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createLocalRuntimeFilterBuilderOperatorHarnessAndProcessElements(3, 4)) {

            // test the output bloom filter should be null
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(2);

            int actualCount = outputRowData.getInt(0);
            assertThat(actualCount).isEqualTo(OVER_MAX_ROW_COUNT);
            assertThat(outputRowData.isNullAt(1)).isTrue();
        }
    }

    private static boolean bloomFilterTestString(BloomFilter bloomFilter, String string) {
        final Projection<RowData, BinaryRowData> projection = new FirstStringFieldProjection();
        return bloomFilter.testHash(
                projection.apply(GenericRowData.of(StringData.fromString(string))).hashCode());
    }

    public static StreamRecord<RowData> createRowDataRecord(String string, int integer) {
        return new StreamRecord<>(GenericRowData.of(StringData.fromString(string), integer));
    }

    public static StreamTaskMailboxTestHarness<RowData>
            createLocalRuntimeFilterBuilderOperatorHarnessAndProcessElements(
                    int estimatedRowCount, int maxRowCount) throws Exception {
        final GeneratedProjection buildProjectionCode =
                new GeneratedProjection("", "", new Object[0]) {
                    @Override
                    public Projection newInstance(ClassLoader classLoader) {
                        return new FirstStringFieldProjection();
                    }
                };

        final TypeInformation<RowData> inputType =
                InternalTypeInfo.ofFields(new VarCharType(), new IntType());
        final LocalRuntimeFilterBuilderOperator operator =
                new LocalRuntimeFilterBuilderOperator(
                        buildProjectionCode, estimatedRowCount, maxRowCount);
        StreamTaskMailboxTestHarness<RowData> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new,
                                InternalTypeInfo.ofFields(new IntType(), new BinaryType()))
                        .setupOutputForSingletonOperatorChain(operator)
                        .addInput(inputType)
                        .build();

        testHarness.processElement(createRowDataRecord("var1", 111));
        testHarness.processElement(createRowDataRecord("var2", 222));
        testHarness.processElement(createRowDataRecord("var3", 333));
        testHarness.processElement(createRowDataRecord("var4", 444));
        testHarness.processElement(createRowDataRecord("var5", 555));
        testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

        return testHarness;
    }

    static final class FirstStringFieldProjection implements Projection<RowData, BinaryRowData> {

        BinaryRowData innerRow = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(innerRow);

        @Override
        public BinaryRowData apply(RowData row) {
            writer.reset();
            writer.writeString(0, row.getString(0));
            writer.complete();
            return innerRow;
        }
    }
}
