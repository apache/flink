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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.java.tuple.Tuple3;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the restore of the dynamic rate limit of a {@link SourceOperator}. */
class SourceOperatorDynamicRateRestoreTest {

    @Test
    void testNothingToRestore() {
        assertThat(SourceOperator.restoreDynamicRate(Collections.emptyList(), 0, 2)).isNaN();
    }

    @Test
    void testSameParallelismRestoresOwnRate() {
        final Iterable<Tuple3<Integer, Integer, Double>> union =
                Arrays.asList(Tuple3.of(0, 2, 10.0), Tuple3.of(1, 2, 30.0));

        assertThat(SourceOperator.restoreDynamicRate(union, 0, 2)).isEqualTo(10.0);
        assertThat(SourceOperator.restoreDynamicRate(union, 1, 2)).isEqualTo(30.0);
    }

    @Test
    void testScaleOutSplitsTotalEvenly() {
        final Iterable<Tuple3<Integer, Integer, Double>> union =
                Arrays.asList(Tuple3.of(0, 2, 10.0), Tuple3.of(1, 2, 30.0));

        assertThat(SourceOperator.restoreDynamicRate(union, 3, 4)).isEqualTo(10.0);
    }

    @Test
    void testScaleInSplitsTotalEvenly() {
        final Iterable<Tuple3<Integer, Integer, Double>> union =
                Arrays.asList(Tuple3.of(0, 3, 10.0), Tuple3.of(1, 3, 30.0), Tuple3.of(2, 3, 20.0));

        assertThat(SourceOperator.restoreDynamicRate(union, 0, 2)).isEqualTo(30.0);
    }

    @Test
    void testSameParallelismWithoutOwnEntryUsesTheAverageRate() {
        // subtask 1 had no rate when the state was written, which must not drag the rate down
        final Iterable<Tuple3<Integer, Integer, Double>> union =
                Arrays.asList(Tuple3.of(0, 4, 10.0), Tuple3.of(2, 4, 30.0), Tuple3.of(3, 4, 20.0));

        assertThat(SourceOperator.restoreDynamicRate(union, 1, 4)).isEqualTo(20.0);
    }

    @Test
    void testRescaleWithMissingEntriesIsNotMistakenForUnchangedParallelism() {
        // Three subtasks, one without a rate: two entries remain, as many as the new parallelism.
        final Iterable<Tuple3<Integer, Integer, Double>> union =
                Arrays.asList(Tuple3.of(0, 3, 10.0), Tuple3.of(1, 3, 30.0));

        // average 20 over 3 subtasks is 60 in total, i.e. 30 for each of 2 subtasks
        assertThat(SourceOperator.restoreDynamicRate(union, 0, 2)).isEqualTo(30.0);
        assertThat(SourceOperator.restoreDynamicRate(union, 1, 2)).isEqualTo(30.0);
    }

    @Test
    void testRestoredRateIsClampedToTheConfiguredMax() {
        assertThat(SourceOperator.clampRestoredRate(500.0, 100.0)).isEqualTo(100.0);
        assertThat(SourceOperator.clampRestoredRate(50.0, 100.0)).isEqualTo(50.0);
    }

    @Test
    void testUnusableRestoredRateIsDropped() {
        assertThat(SourceOperator.clampRestoredRate(Double.NaN, 100.0)).isNaN();
        assertThat(SourceOperator.clampRestoredRate(0.0, 100.0)).isNaN();
        assertThat(SourceOperator.clampRestoredRate(-1.0, 100.0)).isNaN();
        assertThat(SourceOperator.clampRestoredRate(Double.POSITIVE_INFINITY, 100.0)).isNaN();
    }
}
