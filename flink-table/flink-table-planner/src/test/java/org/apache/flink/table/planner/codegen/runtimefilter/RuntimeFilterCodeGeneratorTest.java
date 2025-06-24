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

package org.apache.flink.table.planner.codegen.runtimefilter;

import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.operators.runtimefilter.LocalRuntimeFilterBuilderOperatorTest.createLocalRuntimeFilterBuilderOperatorHarnessAndProcessElements;
import static org.apache.flink.table.runtime.operators.runtimefilter.LocalRuntimeFilterBuilderOperatorTest.createRowDataRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RuntimeFilterCodeGenerator}. */
class RuntimeFilterCodeGeneratorTest {
    private StreamTaskMailboxTestHarness<RowData> testHarness;

    @BeforeEach
    void setup() throws Exception {
        final RowType leftType = RowType.of(new IntType(), new VarBinaryType());
        final RowType rightType = RowType.of(new VarCharType(), new IntType());
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(
                        TableConfig.getDefault(), Thread.currentThread().getContextClassLoader());
        final CodeGenOperatorFactory<RowData> operatorFactory =
                RuntimeFilterCodeGenerator.gen(ctx, leftType, rightType, new int[] {0});

        testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                TwoInputStreamTask::new, InternalTypeInfo.of(rightType))
                        .setupOutputForSingletonOperatorChain(operatorFactory)
                        .addInput(InternalTypeInfo.of(leftType))
                        .addInput(InternalTypeInfo.of(rightType))
                        .build();
    }

    @AfterEach
    void cleanup() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    @Test
    void testNormalFilter() throws Exception {
        // finish build phase
        finishBuildPhase(createNormalInput());

        // finish probe phase
        testHarness.processElement(createRowDataRecord("var1", 111), 1);
        testHarness.processElement(createRowDataRecord("var3", 333), 1);
        testHarness.processElement(createRowDataRecord("var5", 555), 1);
        testHarness.processElement(createRowDataRecord("var6", 666), 1);
        testHarness.processElement(createRowDataRecord("var8", 888), 1);
        testHarness.processElement(createRowDataRecord("var9", 999), 1);
        testHarness.processEvent(new EndOfData(StopMode.DRAIN), 1);

        assertThat(getOutputRowData())
                .containsExactly(
                        GenericRowData.of("var1", 111),
                        GenericRowData.of("var3", 333),
                        GenericRowData.of("var5", 555));
    }

    @Test
    void testOverMaxRowCountLimitFilter() throws Exception {
        // finish build phase
        finishBuildPhase(createOverMaxRowCountLimitInput());

        // finish probe phase
        testHarness.processElement(createRowDataRecord("var1", 111), 1);
        testHarness.processElement(createRowDataRecord("var3", 333), 1);
        testHarness.processElement(createRowDataRecord("var5", 555), 1);
        testHarness.processElement(createRowDataRecord("var6", 666), 1);
        testHarness.processElement(createRowDataRecord("var8", 888), 1);
        testHarness.processElement(createRowDataRecord("var9", 999), 1);
        testHarness.processEvent(new EndOfData(StopMode.DRAIN), 1);

        assertThat(getOutputRowData())
                .containsExactly(
                        GenericRowData.of("var1", 111),
                        GenericRowData.of("var3", 333),
                        GenericRowData.of("var5", 555),
                        GenericRowData.of("var6", 666),
                        GenericRowData.of("var8", 888),
                        GenericRowData.of("var9", 999));
    }

    private void finishBuildPhase(StreamRecord<RowData> leftInput) throws Exception {
        testHarness.processElement(leftInput, 0);
        testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);
    }

    private List<GenericRowData> getOutputRowData() {
        return testHarness.getOutput().stream()
                .map(record -> ((StreamRecord<RowData>) record).getValue())
                .map(
                        rowData -> {
                            assertThat(rowData.getArity()).isEqualTo(2);
                            return GenericRowData.of(
                                    rowData.getString(0).toString(), rowData.getInt(1));
                        })
                .collect(Collectors.toList());
    }

    private static StreamRecord<RowData> createNormalInput() throws Exception {
        StreamTaskMailboxTestHarness<RowData> localRuntimeFilterBuilder =
                createLocalRuntimeFilterBuilderOperatorHarnessAndProcessElements(5, 10);
        StreamRecord<RowData> normalFilter =
                (StreamRecord<RowData>) localRuntimeFilterBuilder.getOutput().poll();
        localRuntimeFilterBuilder.close();
        return normalFilter;
    }

    private static StreamRecord<RowData> createOverMaxRowCountLimitInput() {
        return new StreamRecord<>(GenericRowData.of(RuntimeFilterUtils.OVER_MAX_ROW_COUNT, null));
    }
}
