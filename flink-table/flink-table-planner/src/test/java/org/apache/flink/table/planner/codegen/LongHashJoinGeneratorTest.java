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

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTest;

import org.junit.Test;

/** Test for {@link LongHashJoinGenerator}. */
public class LongHashJoinGeneratorTest extends Int2HashJoinOperatorTest {

    @Override
    public Object newOperator(
            long memorySize,
            FlinkJoinType flinkJoinType,
            HashJoinType hashJoinType,
            boolean buildLeft,
            boolean reverseJoinFunction) {
        return LongAdaptiveHashJoinGeneratorTest.getLongHashJoinOperator(
                flinkJoinType, hashJoinType, buildLeft, reverseJoinFunction);
    }

    @Test
    @Override
    public void testBuildLeftSemiJoin() {}

    @Test
    @Override
    public void testBuildSecondHashFullOutJoin() {}

    @Test
    @Override
    public void testBuildSecondHashRightOutJoin() {}

    @Test
    @Override
    public void testBuildLeftAntiJoin() {}

    @Test
    @Override
    public void testBuildFirstHashLeftOutJoin() {}

    @Test
    @Override
    public void testBuildFirstHashFullOutJoin() {}
}
