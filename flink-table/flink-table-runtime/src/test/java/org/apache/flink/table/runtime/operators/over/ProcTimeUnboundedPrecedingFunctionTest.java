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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ProcTimeRangeBoundedPrecedingFunction}. */
class ProcTimeUnboundedPrecedingFunctionTest {
    StateTtlConfig ttlConfig = StateConfigUtil.createTtlConfig(2_000);

    private static GeneratedAggsHandleFunction aggsHandleFunction =
            new GeneratedAggsHandleFunction("Function", "", new Object[0]) {
                @Override
                public AggsHandleFunction newInstance(ClassLoader classLoader) {
                    return new SumAggsHandleFunction(1);
                }
            };

    private LogicalType[] inputFieldTypes =
            new LogicalType[] {
                VarCharType.STRING_TYPE, new BigIntType(),
            };

    private LogicalType[] outputFieldTypes =
            new LogicalType[] {
                VarCharType.STRING_TYPE, new BigIntType(), new BigIntType(),
            };
    private LogicalType[] accTypes = new LogicalType[] {new BigIntType()};

    private RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(new int[] {0}, inputFieldTypes);
    private TypeInformation<RowData> keyType = keySelector.getProducedType();

    private RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(outputFieldTypes);

    @Test
    void testStateTtl() throws Exception {
        ProcTimeUnboundedPrecedingFunction<RowData> function =
                new ProcTimeUnboundedPrecedingFunction<>(ttlConfig, aggsHandleFunction, accTypes);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        AbstractKeyedStateBackend stateBackend =
                (AbstractKeyedStateBackend) operator.getKeyedStateBackend();

        assertThat(stateBackend.numKeyValueStateEntries())
                .as("Initial state is not empty")
                .isEqualTo(0);

        // put some records
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement(insertRecord("key", 1L));
        testHarness.processElement(insertRecord("key", 1L));
        testHarness.processElement(insertRecord("key", 1L));

        testHarness.setStateTtlProcessingTime(2001);
        // at this moment the backend state should have been expired, will calculate from new start
        testHarness.processElement(insertRecord("key", 1L));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("key", 1L, 1L));
        expectedOutput.add(insertRecord("key", 1L, 2L));
        expectedOutput.add(insertRecord("key", 1L, 3L));
        // the result of `sum` restart
        expectedOutput.add(insertRecord("key", 1L, 1L));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void accumulatorInsertionIsRejectedWithSchemaEvolutionEnabled() throws Exception {
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createSchemaEvolvingRocksDBHarness(accTypes)) {
            testHarness.open();
            testHarness.processElement(insertRecord("key", 1L));
            snapshot = testHarness.snapshot(0L, 0L);
        }

        // A second aggregate call of the same type inserted before the existing one. The positional
        // accumulator names would read it as appended, leaving the stored sum in the slot that now
        // belongs to the inserted call.
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createSchemaEvolvingRocksDBHarness(
                        new LogicalType[] {new BigIntType(), new BigIntType()})) {
            testHarness.initializeState(snapshot);

            assertThatThrownBy(testHarness::open)
                    .satisfies(
                            anyCauseMatches(
                                    StateMigrationException.class, "must not be incompatible"));
        }
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createSchemaEvolvingRocksDBHarness(
            LogicalType[] accTypes) throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(
                        new KeyedProcessOperator<>(
                                new ProcTimeUnboundedPrecedingFunction<>(
                                        StateConfigUtil.createTtlConfig(0),
                                        aggsHandleFunction,
                                        accTypes)));
        testHarness.setStateBackend(new EmbeddedRocksDBStateBackend());
        Configuration configuration = new Configuration();
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_STATE_SCHEMA_EVOLUTION_ENABLED, true);
        testHarness.getExecutionConfig().configure(configuration, getClass().getClassLoader());
        return testHarness;
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            KeyedProcessOperator<RowData, RowData, RowData> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
    }
}
