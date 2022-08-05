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

import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.hashtable.BinaryHashTableTest;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Random test for adaptive {@link HashJoinOperator}. */
public class Int2AdaptiveHashJoinOperatorTest extends Int2HashJoinOperatorTestBase {

    // ---------------------- build first inner join -----------------------------------------
    // ------------------- fallback to sort merge join in build or probe phase ---------------
    @Test
    public void testBuildFirstHashInnerJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 405590;
        final int repeatedValue2 = 928820;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 160000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 2;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        // output build side which matched the key
        int expectOutSize = numKeys1 * buildValsPerKey * probeValsPerKey;
        buildJoin(buildInput, probeInput, false, false, true, expectOutSize, numKeys1, -1);
    }

    // ---------------------- build first left out join -----------------------------------------
    // -------------------- fallback to sort merge join in build or probe phase -----------------
    @Test
    public void testBuildFirstHashLeftOutJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 50000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 2;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        // output build side that is in left
        int expectOutSize =
                (numKeys1 - numKeys2) * buildValsPerKey * probeValsPerKey
                        + (numKeys1 - numKeys2) * buildValsPerKey
                        + repeatedValueCountBuild * probeValsPerKey
                        + repeatedValueCountBuild;
        buildJoin(buildInput, probeInput, true, false, true, expectOutSize, numKeys1, -1);
    }

    // ---------------------- build first right out join -----------------------------------------
    // --------------------- fallback to sort merge join in build or probe phase -----------------
    @Test
    public void testBuildFirstHashRightOutJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 50000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 1;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        // output probe side that is in right
        int expectOutSize = numKeys2 * probeValsPerKey * buildValsPerKey + repeatedValueCountBuild;
        buildJoin(buildInput, probeInput, false, true, true, expectOutSize, numKeys2, -1);
    }

    // ---------------------- build first full out join -----------------------------------------
    // --------------------- fallback to sort merge join in build or probe phase ----------------
    @Test
    public void testBuildFirstHashFullOutJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 150000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 1;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        // output build side and probe side simultaneously
        int expectOutSize =
                numKeys1 * buildValsPerKey * probeValsPerKey
                        + repeatedValueCountBuild * 2
                        + numKeys2
                        - numKeys1;
        buildJoin(buildInput, probeInput, true, true, true, expectOutSize, numKeys2, -1);
    }

    // ---------------------- build second left out join -----------------------------------------
    // ---------------------- switch to sort merge join in build or probe phase ------------------
    @Test
    public void testBuildSecondHashLeftOutJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 50000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 1;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        // output probe side that is in left
        int expectOutSize = numKeys2 * probeValsPerKey * buildValsPerKey + repeatedValueCountBuild;
        buildJoin(buildInput, probeInput, true, false, false, expectOutSize, numKeys2, -1);
    }

    // ---------------------- build second right out join -----------------------------------------
    // ---------------------- switch to sort merge join in build or probe phase -------------------
    @Test
    public void testBuildSecondHashRightOutJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 50000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 1;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        // output build side that is in right
        int expectOutSize = numKeys1 * buildValsPerKey + repeatedValueCountBuild * 2;
        buildJoin(buildInput, probeInput, false, true, false, expectOutSize, numKeys1, -1);
    }

    // ---------------------- switch to sort merge join in build or probe phase ------------------
    @Test
    public void testSemiJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 100000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 1;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(33 * 32 * 1024, FlinkJoinType.SEMI, HashJoinType.SEMI, false, false);

        // output probe side that is in left
        joinAndAssert(operator, buildInput, probeInput, numKeys2, numKeys2, 0, true);
    }

    // ---------------------- fallback to sort merge join in build or probe phase ------------------
    @Test
    public void testAntiJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 160000;
        final int buildValsPerKey = 3;
        final int probeValsPerKey = 1;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(33 * 32 * 1024, FlinkJoinType.ANTI, HashJoinType.ANTI, false, false);

        // output probe side that is in left
        int expectOutSize = numKeys2 - numKeys1;
        joinAndAssert(operator, buildInput, probeInput, expectOutSize, expectOutSize, 0, true);
    }

    // ---------------------- fallback to sort merge join in build or probe phase ------------------
    @Test
    public void testBuildLeftSemiJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 405;
        final int repeatedValue2 = 928;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 100000;
        final int numKeys2 = 1000;
        final int buildValsPerKey = 1;
        final int probeValsPerKey = 3;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(
                        33 * 32 * 1024,
                        FlinkJoinType.SEMI,
                        HashJoinType.BUILD_LEFT_SEMI,
                        true,
                        false);

        // output build side in left that is matched with probe side
        int expectOutSize = numKeys2 + repeatedValueCountBuild * 2;
        joinAndAssert(operator, buildInput, probeInput, expectOutSize, numKeys1, -1, true);
    }

    // ---------------------- fallback to sort merge join in build or probe phase ------------------
    @Test
    public void testBuildLeftAntiJoinFallbackToSMJ() throws Exception {
        // the following two values are known to have a hash-code collision on the first recursion
        // level. we use them to make sure one partition grows over-proportionally large
        final int repeatedValue1 = 40559;
        final int repeatedValue2 = 92882;
        final int repeatedValueCountBuild = 100000;

        final int numKeys1 = 500000;
        final int numKeys2 = 100000;
        final int buildValsPerKey = 1;
        final int probeValsPerKey = 3;

        // create a build input that gives 100k pairs with 3 values sharing the same key, plus
        // 1 million pairs with two colliding keys
        MutableObjectIterator<BinaryRowData> build1 =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> build2 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue1, 17, repeatedValueCountBuild);
        MutableObjectIterator<BinaryRowData> build3 =
                new BinaryHashTableTest.ConstantsKeyValuePairsIterator(
                        repeatedValue2, 23, repeatedValueCountBuild);
        List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator =
                newOperator(
                        33 * 32 * 1024,
                        FlinkJoinType.ANTI,
                        HashJoinType.BUILD_LEFT_ANTI,
                        true,
                        false);

        // output build side in left that not matched with probe side
        int expectOutSize = numKeys1 - numKeys2;
        joinAndAssert(
                operator, buildInput, probeInput, expectOutSize, numKeys1 - numKeys2, -1, true);
    }
}
