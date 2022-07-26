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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.utils.JoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.operators.join.Int2AdaptiveHashJoinOperatorTest;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for adaptive {@link LongHashJoinGenerator}. */
public class LongAdaptiveHashJoinGeneratorTest extends Int2AdaptiveHashJoinOperatorTest {

    public LongAdaptiveHashJoinGeneratorTest(boolean fallbackToSMJInBuildPhase) {
        super(fallbackToSMJInBuildPhase);
    }

    @Override
    public Object newOperator(
            long memorySize,
            FlinkJoinType flinkJoinType,
            HashJoinType hashJoinType,
            boolean buildLeft,
            boolean reverseJoinFunction) {
        return getLongHashJoinOperator(
                flinkJoinType, hashJoinType, buildLeft, reverseJoinFunction, buildSpillThreshold);
    }

    @Override
    public void testBuildLeftAntiJoinFallbackToSMJ() {}

    @Override
    public void testBuildLeftSemiJoinFallbackToSMJ() {}

    @Override
    public void testBuildFirstHashLeftOutJoinFallbackToSMJ() {}

    @Override
    public void testBuildSecondHashRightOutJoinFallbackToSMJ() {}

    @Override
    public void testBuildFirstHashFullOutJoinFallbackToSMJ() {}

    static Object getLongHashJoinOperator(
            FlinkJoinType flinkJoinType,
            HashJoinType hashJoinType,
            boolean buildLeft,
            boolean reverseJoinFunction,
            long buildSpillThreshold) {
        RowType keyType = RowType.of(new IntType());
        boolean[] filterNulls = new boolean[] {true};
        assertThat(LongHashJoinGenerator.support(hashJoinType, keyType, filterNulls)).isTrue();

        RowType buildType = RowType.of(new IntType(), new IntType());
        RowType probeType = RowType.of(new IntType(), new IntType());
        int[] buildKeyMapping = new int[] {0};
        int[] probeKeyMapping = new int[] {0};
        GeneratedJoinCondition condFunc =
                new GeneratedJoinCondition(
                        MyJoinCondition.class.getCanonicalName(), "", new Object[0]) {
                    @Override
                    public JoinCondition newInstance(ClassLoader classLoader) {
                        return new MyJoinCondition(new Object[0]);
                    }
                };

        SortMergeJoinFunction sortMergeJoinFunction;
        if (buildLeft) {
            sortMergeJoinFunction =
                    JoinOperatorUtil.getSortMergeJoinFunction(
                            Thread.currentThread().getContextClassLoader(),
                            new ExecNodeConfig(TableConfig.getDefault(), new Configuration()),
                            flinkJoinType,
                            buildType,
                            probeType,
                            buildKeyMapping,
                            probeKeyMapping,
                            keyType,
                            buildLeft,
                            filterNulls,
                            condFunc,
                            0);
        } else {
            sortMergeJoinFunction =
                    JoinOperatorUtil.getSortMergeJoinFunction(
                            Thread.currentThread().getContextClassLoader(),
                            new ExecNodeConfig(TableConfig.getDefault(), new Configuration()),
                            flinkJoinType,
                            probeType,
                            buildType,
                            probeKeyMapping,
                            buildKeyMapping,
                            keyType,
                            buildLeft,
                            filterNulls,
                            condFunc,
                            0);
        }
        return LongHashJoinGenerator.gen(
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                hashJoinType,
                keyType,
                buildType,
                probeType,
                buildKeyMapping,
                probeKeyMapping,
                20,
                10000,
                reverseJoinFunction,
                condFunc,
                buildLeft,
                buildSpillThreshold,
                sortMergeJoinFunction);
    }
}
