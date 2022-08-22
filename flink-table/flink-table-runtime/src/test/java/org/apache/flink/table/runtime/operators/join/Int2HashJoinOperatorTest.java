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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

/** Random test for {@link HashJoinOperator}. */
public class Int2HashJoinOperatorTest extends Int2HashJoinOperatorTestBase {

    // ---------------------- build first inner join -----------------------------------------
    @Test
    public void testBuildFirstHashInnerJoin() throws Exception {

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
                false,
                false,
                true,
                numKeys * buildValsPerKey * probeValsPerKey,
                numKeys,
                165);
    }

    // ---------------------- build first left out join -----------------------------------------
    @Test
    public void testBuildFirstHashLeftOutJoin() throws Exception {

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
                true,
                false,
                true,
                numKeys1 * buildValsPerKey * probeValsPerKey,
                numKeys1,
                165);
    }

    // ---------------------- build first right out join -----------------------------------------
    @Test
    public void testBuildFirstHashRightOutJoin() throws Exception {

        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        buildJoin(buildInput, probeInput, false, true, true, 280, numKeys2, -1);
    }

    // ---------------------- build first full out join -----------------------------------------
    @Test
    public void testBuildFirstHashFullOutJoin() throws Exception {

        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        buildJoin(buildInput, probeInput, true, true, true, 280, numKeys2, -1);
    }

    // ---------------------- build second inner join -----------------------------------------
    @Test
    public void testBuildSecondHashInnerJoin() throws Exception {

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
                false,
                false,
                false,
                numKeys * buildValsPerKey * probeValsPerKey,
                numKeys,
                165);
    }

    // ---------------------- build second left out join -----------------------------------------
    @Test
    public void testBuildSecondHashLeftOutJoin() throws Exception {

        int numKeys1 = 10;
        int numKeys2 = 9;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        buildJoin(
                buildInput,
                probeInput,
                true,
                false,
                false,
                numKeys2 * buildValsPerKey * probeValsPerKey,
                numKeys2,
                165);
    }

    // ---------------------- build second right out join -----------------------------------------
    @Test
    public void testBuildSecondHashRightOutJoin() throws Exception {

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
                false,
                true,
                false,
                numKeys1 * buildValsPerKey * probeValsPerKey,
                numKeys2,
                -1);
    }

    // ---------------------- build second full out join -----------------------------------------
    @Test
    public void testBuildSecondHashFullOutJoin() throws Exception {

        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        buildJoin(buildInput, probeInput, true, true, false, 280, numKeys2, -1);
    }

    @Test
    public void testSemiJoin() throws Exception {

        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(33 * 32 * 1024, FlinkJoinType.SEMI, HashJoinType.SEMI, false, false);
        joinAndAssert(operator, buildInput, probeInput, 90, 9, 45, true);
    }

    @Test
    public void testAntiJoin() throws Exception {

        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(33 * 32 * 1024, FlinkJoinType.ANTI, HashJoinType.ANTI, false, false);
        joinAndAssert(operator, buildInput, probeInput, 10, 1, 45, true);
    }

    @Test
    public void testBuildLeftSemiJoin() throws Exception {
        int numKeys1 = 10;
        int numKeys2 = 9;
        int buildValsPerKey = 10;
        int probeValsPerKey = 3;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(
                        33 * 32 * 1024,
                        FlinkJoinType.SEMI,
                        HashJoinType.BUILD_LEFT_SEMI,
                        true,
                        false);
        joinAndAssert(operator, buildInput, probeInput, 90, 9, 45, true);
    }

    @Test
    public void testBuildLeftAntiJoin() throws Exception {
        int numKeys1 = 10;
        int numKeys2 = 9;
        int buildValsPerKey = 10;
        int probeValsPerKey = 3;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(
                        33 * 32 * 1024,
                        FlinkJoinType.ANTI,
                        HashJoinType.BUILD_LEFT_ANTI,
                        true,
                        false);
        joinAndAssert(operator, buildInput, probeInput, 10, 1, 45, true);
    }
}
