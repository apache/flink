/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package org.apache.flink.table.planner.adaptive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.HashJoinOperator;
import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTestBase;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinOperator;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoin;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY;
import static org.apache.flink.table.planner.plan.utils.OperatorType.BroadcastHashJoin;
import static org.apache.flink.table.planner.plan.utils.OperatorType.ShuffleHashJoin;
import static org.apache.flink.table.planner.plan.utils.OperatorType.SortMergeJoin;
import static org.apache.flink.table.runtime.util.JoinUtil.getJoinType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AdaptiveJoinOperatorGenerator}. */
class AdaptiveJoinOperatorGeneratorTest extends Int2HashJoinOperatorTestBase {

    @Test
    void testShuffleHashJoinTransformationCorrectness() throws Exception {

        // all cases to ShuffleHashJoin
        testInnerJoin(true, ShuffleHashJoin, false, ShuffleHashJoin);
        testInnerJoin(false, ShuffleHashJoin, false, ShuffleHashJoin);

        testLeftOutJoin(true, ShuffleHashJoin, false, ShuffleHashJoin);
        testLeftOutJoin(false, ShuffleHashJoin, false, ShuffleHashJoin);

        testRightOutJoin(true, ShuffleHashJoin, false, ShuffleHashJoin);
        testRightOutJoin(false, ShuffleHashJoin, false, ShuffleHashJoin);

        testSemiJoin(ShuffleHashJoin, false, ShuffleHashJoin);

        testAntiJoin(ShuffleHashJoin, false, ShuffleHashJoin);

        // all cases to BroadcastHashJoin
        testInnerJoin(true, ShuffleHashJoin, true, BroadcastHashJoin);
        testInnerJoin(false, ShuffleHashJoin, true, BroadcastHashJoin);

        testLeftOutJoin(false, ShuffleHashJoin, true, BroadcastHashJoin);

        testRightOutJoin(true, ShuffleHashJoin, true, BroadcastHashJoin);

        testSemiJoin(ShuffleHashJoin, true, BroadcastHashJoin);

        testAntiJoin(ShuffleHashJoin, true, BroadcastHashJoin);
    }

    @Test
    void testSortMergeJoinTransformationCorrectness() throws Exception {
        // all cases to SortMergeJoin
        testInnerJoin(true, SortMergeJoin, false, SortMergeJoin);
        testInnerJoin(false, SortMergeJoin, false, SortMergeJoin);

        testLeftOutJoin(true, SortMergeJoin, false, SortMergeJoin);

        testRightOutJoin(true, SortMergeJoin, false, SortMergeJoin);

        testAntiJoin(SortMergeJoin, false, SortMergeJoin);

        testAntiJoin(SortMergeJoin, false, SortMergeJoin);

        // all cases to BroadcastHashJoin
        testInnerJoin(true, SortMergeJoin, true, BroadcastHashJoin);
        testInnerJoin(false, SortMergeJoin, true, BroadcastHashJoin);

        testLeftOutJoin(false, SortMergeJoin, true, BroadcastHashJoin);

        testRightOutJoin(true, SortMergeJoin, true, BroadcastHashJoin);

        testSemiJoin(SortMergeJoin, true, BroadcastHashJoin);

        testAntiJoin(SortMergeJoin, true, BroadcastHashJoin);
    }

    private void testInnerJoin(
            boolean isBuildLeft,
            OperatorType originalJoinType,
            boolean isBroadcast,
            OperatorType expectedOperatorType)
            throws Exception {
        int numKeys = 100;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        buildJoin(
                buildInput,
                probeInput,
                originalJoinType,
                expectedOperatorType,
                false,
                false,
                isBuildLeft,
                isBroadcast,
                numKeys * buildValsPerKey * probeValsPerKey,
                numKeys,
                165);
    }

    private void testLeftOutJoin(
            boolean isBuildLeft,
            OperatorType originalJoinType,
            boolean isBroadcast,
            OperatorType expectedOperatorType)
            throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(
                        isBuildLeft ? numKeys1 : numKeys2, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(
                        isBuildLeft ? numKeys2 : numKeys1, probeValsPerKey, true);

        buildJoin(
                buildInput,
                probeInput,
                originalJoinType,
                expectedOperatorType,
                true,
                false,
                isBuildLeft,
                isBroadcast,
                numKeys1 * buildValsPerKey * probeValsPerKey,
                numKeys1,
                165);
    }

    private void testRightOutJoin(
            boolean isBuildLeft,
            OperatorType originalJoinType,
            boolean isBroadcast,
            OperatorType expectedOperatorType)
            throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        buildJoin(
                buildInput,
                probeInput,
                originalJoinType,
                expectedOperatorType,
                false,
                true,
                isBuildLeft,
                isBroadcast,
                isBuildLeft ? 280 : 270,
                numKeys2,
                -1);
    }

    private void testSemiJoin(
            OperatorType originalJoinType, boolean isBroadcast, OperatorType expectedOperatorType)
            throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        if (originalJoinType == SortMergeJoin && !isBroadcast) {
            numKeys1 = 10;
            numKeys2 = 9;
            buildValsPerKey = 10;
            probeValsPerKey = 3;
        }
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator = newOperator(FlinkJoinType.SEMI, false, isBroadcast, originalJoinType);
        assertOperatorType(operator, expectedOperatorType);
        joinAndAssert(operator, buildInput, probeInput, 90, 9, 45, true);
    }

    private void testAntiJoin(
            OperatorType originalJoinType, boolean isBroadcast, OperatorType expectedOperatorType)
            throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        if (originalJoinType == SortMergeJoin && !isBroadcast) {
            numKeys1 = 10;
            numKeys2 = 9;
            buildValsPerKey = 10;
            probeValsPerKey = 3;
        }
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator = newOperator(FlinkJoinType.ANTI, false, isBroadcast, originalJoinType);
        assertOperatorType(operator, expectedOperatorType);
        joinAndAssert(operator, buildInput, probeInput, 10, 1, 45, true);
    }

    public void buildJoin(
            MutableObjectIterator<BinaryRowData> buildInput,
            MutableObjectIterator<BinaryRowData> probeInput,
            OperatorType originalJoinType,
            OperatorType expectedOperatorType,
            boolean leftOut,
            boolean rightOut,
            boolean buildLeft,
            boolean isBroadcast,
            int expectOutSize,
            int expectOutKeySize,
            int expectOutVal)
            throws Exception {
        FlinkJoinType flinkJoinType = getJoinType(leftOut, rightOut);
        Object operator = newOperator(flinkJoinType, buildLeft, isBroadcast, originalJoinType);
        assertOperatorType(operator, expectedOperatorType);
        joinAndAssert(
                operator,
                buildInput,
                probeInput,
                expectOutSize,
                expectOutKeySize,
                expectOutVal,
                false);
    }

    public Object newOperator(
            FlinkJoinType flinkJoinType,
            boolean buildLeft,
            boolean isBroadcast,
            OperatorType operatorType) {
        AdaptiveJoin adaptiveJoin = genAdaptiveJoin(flinkJoinType, operatorType);
        adaptiveJoin.markAsBroadcastJoin(isBroadcast, buildLeft);

        return adaptiveJoin.genOperatorFactory(getClass().getClassLoader(), new Configuration());
    }

    public void assertOperatorType(Object operator, OperatorType expectedOperatorType) {
        switch (expectedOperatorType) {
            case BroadcastHashJoin:
            case ShuffleHashJoin:
                if (operator instanceof CodeGenOperatorFactory) {
                    assertThat(
                                    ((CodeGenOperatorFactory<?>) operator)
                                            .getGeneratedClass()
                                            .getClassName())
                            .contains("LongHashJoinOperator");
                } else {
                    assertThat(operator).isInstanceOf(SimpleOperatorFactory.class);
                    assertThat(((SimpleOperatorFactory<?>) operator).getOperator())
                            .isInstanceOf(HashJoinOperator.class);
                }
                break;
            case SortMergeJoin:
                assertThat(operator).isInstanceOf(SimpleOperatorFactory.class);
                assertThat(((SimpleOperatorFactory<?>) operator).getOperator())
                        .isInstanceOf(SortMergeJoinOperator.class);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unexpected operator type %s.", expectedOperatorType));
        }
    }

    public AdaptiveJoin genAdaptiveJoin(FlinkJoinType flinkJoinType, OperatorType operatorType) {
        GeneratedJoinCondition condFuncCode =
                new GeneratedJoinCondition(
                        Int2HashJoinOperatorTestBase.MyJoinCondition.class.getCanonicalName(),
                        "",
                        new Object[0]) {
                    @Override
                    public JoinCondition newInstance(ClassLoader classLoader) {
                        return new Int2HashJoinOperatorTestBase.MyJoinCondition(new Object[0]);
                    }
                };

        return new AdaptiveJoinOperatorGenerator(
                new int[] {0},
                new int[] {0},
                flinkJoinType,
                new boolean[] {true},
                RowType.of(new IntType(), new IntType()),
                RowType.of(new IntType(), new IntType()),
                condFuncCode,
                20,
                10000,
                20,
                10000,
                false,
                TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY.defaultValue().getBytes(),
                true,
                operatorType);
    }
}
