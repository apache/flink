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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTestBase;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoin;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.function.Function;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY;
import static org.apache.flink.table.runtime.util.JoinUtil.getJoinType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AdaptiveJoinOperatorGenerator}. */
class AdaptiveJoinOperatorGeneratorTest extends Int2HashJoinOperatorTestBase {

    private final Function<Boolean, Boolean> areEdgesCanBeTransformed =
            new StreamEdgesTransformationChecker();

    // --------- Test if the join operator can be converted to a broadcast hash join  -------------
    @Test
    void testInnerJoinCheckBroadcast() {
        AdaptiveJoin adaptiveJoin =
                genAdaptiveJoin(FlinkJoinType.INNER, OperatorType.ShuffleHashJoin);
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, false));
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));

        adaptiveJoin = genAdaptiveJoin(FlinkJoinType.INNER, OperatorType.SortMergeJoin);
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, false));
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));
    }

    @Test
    void testRightJoinCheckBroadcast() {
        AdaptiveJoin adaptiveJoin =
                genAdaptiveJoin(FlinkJoinType.RIGHT, OperatorType.ShuffleHashJoin);
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));

        adaptiveJoin = genAdaptiveJoin(FlinkJoinType.RIGHT, OperatorType.SortMergeJoin);
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));
    }

    @Test
    void testLeftJoinCheckBroadcast() {
        testBuildRightCheckBroadcast(FlinkJoinType.LEFT);
    }

    @Test
    void testSemiJoinCheckBroadcast() {
        testBuildRightCheckBroadcast(FlinkJoinType.SEMI);
    }

    @Test
    void testAntiJoinCheckBroadcast() {
        testBuildRightCheckBroadcast(FlinkJoinType.ANTI);
    }

    @Test
    void testFullJoinCheckBroadcast() {
        AdaptiveJoin adaptiveJoin =
                genAdaptiveJoin(FlinkJoinType.FULL, OperatorType.ShuffleHashJoin);
        assertThatThrownBy(
                        () ->
                                adaptiveJoin.tryBroadcastOptimization(
                                        2L, 10L, 5L, areEdgesCanBeTransformed))
                .hasMessageContaining("Unexpected join type");
    }

    void testBuildRightCheckBroadcast(FlinkJoinType joinType) {
        AdaptiveJoin adaptiveJoin = genAdaptiveJoin(joinType, OperatorType.ShuffleHashJoin);
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, false));
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));

        adaptiveJoin = genAdaptiveJoin(joinType, OperatorType.SortMergeJoin);
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 5L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(true, false));
        assertThat(adaptiveJoin.tryBroadcastOptimization(2L, 10L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, true));
        assertThat(adaptiveJoin.tryBroadcastOptimization(10L, 2L, 1L, areEdgesCanBeTransformed))
                .isEqualTo(new Tuple2<>(false, false));
    }

    // ---------------------- Test the correctness of the generated join operator -----------------
    @Test
    void testGenerateOperatorCorrectness() {
        Object sortMergeJoinOperator =
                newOperator(FlinkJoinType.INNER, true, false, OperatorType.SortMergeJoin);
        assertThat(sortMergeJoinOperator).isInstanceOf(SimpleOperatorFactory.class);

        Object broadcastHashJoinOperator =
                newOperator(FlinkJoinType.INNER, true, true, OperatorType.SortMergeJoin);
        assertThat(broadcastHashJoinOperator).isInstanceOf(CodeGenOperatorFactory.class);

        Object hashJoinOperator =
                newOperator(FlinkJoinType.INNER, true, false, OperatorType.ShuffleHashJoin);
        assertThat(hashJoinOperator).isInstanceOf(CodeGenOperatorFactory.class);

        Object broadcastHashJoinOperator2 =
                newOperator(FlinkJoinType.INNER, true, true, OperatorType.ShuffleHashJoin);
        assertThat(broadcastHashJoinOperator2).isInstanceOf(CodeGenOperatorFactory.class);
    }

    @Test
    void testShuffleHashJoinTransformationCorrectness() throws Exception {
        OperatorType joinType = OperatorType.ShuffleHashJoin;

        // all cases to ShuffleHashJoin
        testInnerJoin(true, joinType, false);
        testInnerJoin(false, joinType, false);

        testLeftOutJoin(true, joinType, false);
        testLeftOutJoin(false, joinType, false);

        testRightOutJoin(true, joinType, false);
        testRightOutJoin(false, joinType, false);

        testSemiJoin(joinType, false);

        testAntiJoin(joinType, false);

        // all cases to BroadcastHashJoin
        testInnerJoin(true, joinType, true);
        testInnerJoin(false, joinType, true);

        testLeftOutJoin(false, joinType, true);

        testRightOutJoin(true, joinType, true);

        testSemiJoin(joinType, true);

        testAntiJoin(joinType, true);
    }

    @Test
    void testSortMergeJoinTransformationCorrectness() throws Exception {
        OperatorType joinType = OperatorType.SortMergeJoin;

        // all cases to SortMergeJoin
        testInnerJoin(true, joinType, false);
        testInnerJoin(true, joinType, true);

        testLeftOutJoin(true, joinType, false);

        testRightOutJoin(true, joinType, false);

        testAntiJoin(joinType, false);

        testAntiJoin(joinType, false);

        // all cases to BroadcastHashJoin
        testInnerJoin(true, joinType, true);
        testInnerJoin(false, joinType, true);

        testLeftOutJoin(false, joinType, true);

        testRightOutJoin(true, joinType, true);

        testSemiJoin(joinType, true);

        testAntiJoin(joinType, true);
    }

    private void testInnerJoin(
            boolean isBuildLeft, OperatorType originalJoinType, boolean isBroadcast)
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
                false,
                false,
                isBuildLeft,
                isBroadcast,
                numKeys * buildValsPerKey * probeValsPerKey,
                numKeys,
                165);
    }

    private void testLeftOutJoin(
            boolean isBuildLeft, OperatorType originalJoinType, boolean isBroadcast)
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
                true,
                false,
                isBuildLeft,
                isBroadcast,
                numKeys1 * buildValsPerKey * probeValsPerKey,
                numKeys1,
                165);
    }

    private void testRightOutJoin(
            boolean isBuildLeft, OperatorType originalJoinType, boolean isBroadcast)
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
                false,
                true,
                isBuildLeft,
                isBroadcast,
                isBuildLeft ? 280 : 270,
                numKeys2,
                -1);
    }

    private void testSemiJoin(OperatorType originalJoinType, boolean isBroadcast) throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        if (originalJoinType == OperatorType.SortMergeJoin && !isBroadcast) {
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
        joinAndAssert(operator, buildInput, probeInput, 90, 9, 45, true);
    }

    private void testAntiJoin(OperatorType originalJoinType, boolean isBroadcast) throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        if (originalJoinType == OperatorType.SortMergeJoin && !isBroadcast) {
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
        joinAndAssert(operator, buildInput, probeInput, 10, 1, 45, true);
    }

    public void buildJoin(
            MutableObjectIterator<BinaryRowData> buildInput,
            MutableObjectIterator<BinaryRowData> probeInput,
            OperatorType originalJoinType,
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

        long smallerInputSize = 2L;
        long biggerInputSize = 10L;
        adaptiveJoin.tryBroadcastOptimization(
                buildLeft ? smallerInputSize : biggerInputSize,
                buildLeft ? biggerInputSize : smallerInputSize,
                isBroadcast ? 5L : 1L,
                areEdgesCanBeTransformed);

        return adaptiveJoin.genOperatorFactory(getClass().getClassLoader(), new Configuration());
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

    private static class StreamEdgesTransformationChecker
            implements Function<Boolean, Boolean>, Serializable {
        @Override
        public Boolean apply(Boolean aBoolean) {
            return true;
        }
    }
}
