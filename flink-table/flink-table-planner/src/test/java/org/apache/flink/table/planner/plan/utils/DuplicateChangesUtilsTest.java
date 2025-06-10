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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.planner.plan.trait.DuplicateChanges;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.trait.DuplicateChanges.ALLOW;
import static org.apache.flink.table.planner.plan.trait.DuplicateChanges.DISALLOW;
import static org.apache.flink.table.planner.plan.trait.DuplicateChanges.NONE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DuplicateChangesUtils}. */
class DuplicateChangesUtilsTest {

    @ParameterizedTest
    @MethodSource("testMergingMatrix")
    void testMergeDuplicateChanges(
            Tuple3<DuplicateChanges, DuplicateChanges, DuplicateChanges> testData) {
        DuplicateChanges actual =
                DuplicateChangesUtils.mergeDuplicateChanges(testData.f0, testData.f1);
        assertThat(actual).isEqualTo(testData.f2);
    }

    private static Stream<Tuple3<DuplicateChanges, DuplicateChanges, DuplicateChanges>>
            testMergingMatrix() {
        // input1, input2, expected_merged_result
        return Stream.of(
                // -------------------------------------
                Tuple3.of(ALLOW, ALLOW, ALLOW),
                Tuple3.of(ALLOW, DISALLOW, DISALLOW),
                Tuple3.of(ALLOW, NONE, ALLOW),
                // -------------------------------------
                Tuple3.of(DISALLOW, ALLOW, DISALLOW),
                Tuple3.of(DISALLOW, DISALLOW, DISALLOW),
                Tuple3.of(DISALLOW, NONE, DISALLOW),
                // -------------------------------------
                Tuple3.of(NONE, ALLOW, ALLOW),
                Tuple3.of(NONE, DISALLOW, DISALLOW),
                Tuple3.of(NONE, NONE, NONE));
    }
}
