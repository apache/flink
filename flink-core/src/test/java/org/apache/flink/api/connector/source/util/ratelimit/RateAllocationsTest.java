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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link RateAllocations}. */
class RateAllocationsTest {

    private static final double NAN = Double.NaN;

    @Test
    void testDemandAwareWithoutObservationsIsEven() {
        List<SubtaskRateInfo> subtasks =
                Arrays.asList(new SubtaskRateInfo(0, NAN, NAN), new SubtaskRateInfo(1, NAN, NAN));

        Map<Integer, Double> result = RateAllocations.allocate(100, subtasks);

        assertThat(result).containsEntry(0, 50.0).containsEntry(1, 50.0);
    }

    @Test
    void testDemandAwareRedistributesBudgetOfIdleSubtasks() {
        List<SubtaskRateInfo> subtasks =
                Arrays.asList(
                        new SubtaskRateInfo(0, 100, 100),
                        new SubtaskRateInfo(1, 100, 10),
                        new SubtaskRateInfo(2, 100, 20));

        Map<Integer, Double> result = RateAllocations.allocate(300, subtasks);

        assertThat(result.get(1)).isCloseTo(12.0, within(1e-9));
        assertThat(result.get(2)).isCloseTo(24.0, within(1e-9));
        assertThat(result.get(0)).isCloseTo(264.0, within(1e-9));
    }

    @Test
    void testDemandAwareSpreadsLeftoverWhenEverySubtaskIsDemandLimited() {
        List<SubtaskRateInfo> subtasks =
                Arrays.asList(new SubtaskRateInfo(0, 100, 10), new SubtaskRateInfo(1, 100, 10));

        Map<Integer, Double> result = RateAllocations.allocate(200, subtasks);

        assertThat(result.get(0) + result.get(1)).isCloseTo(200.0, within(1e-9));
        assertThat(result.get(0)).isEqualTo(result.get(1));
    }

    @Test
    void testDemandAwareNeverExceedsTotal() {
        List<SubtaskRateInfo> subtasks =
                Arrays.asList(
                        new SubtaskRateInfo(0, 50, 5),
                        new SubtaskRateInfo(1, 50, 49),
                        new SubtaskRateInfo(2, 50, NAN),
                        new SubtaskRateInfo(3, 50, 30));

        Map<Integer, Double> result = RateAllocations.allocate(200, subtasks);

        assertThat(result.values().stream().mapToDouble(Double::doubleValue).sum())
                .isCloseTo(200.0, within(1e-9));
    }

    @Test
    void testIdleSubtaskKeepsAMinimumShare() {
        List<SubtaskRateInfo> subtasks =
                Arrays.asList(
                        new SubtaskRateInfo(0, 100, 0),
                        new SubtaskRateInfo(1, 100, 100),
                        new SubtaskRateInfo(2, 100, 100));

        Map<Integer, Double> result = RateAllocations.allocate(300, subtasks);

        // 10% of the even share of 100
        assertThat(result.get(0)).isCloseTo(10.0, within(1e-9));
        assertThat(result.get(1)).isCloseTo(145.0, within(1e-9));
        assertThat(result.get(2)).isCloseTo(145.0, within(1e-9));
    }

    @Test
    void testRatesArePositiveFiniteAndWithinTotalWhenSubtasksAreIdle() {
        List<SubtaskRateInfo> subtasks =
                Arrays.asList(
                        new SubtaskRateInfo(0, 100, 0),
                        new SubtaskRateInfo(1, 100, 0),
                        new SubtaskRateInfo(2, NAN, NAN));

        Map<Integer, Double> result = RateAllocations.allocate(300, subtasks);

        assertThat(result).hasSize(3);
        assertThat(result.values()).allSatisfy(rate -> assertThat(rate).isPositive().isFinite());
        assertThat(result.values().stream().mapToDouble(Double::doubleValue).sum())
                .isLessThanOrEqualTo(300.0 + 1e-9);
    }

    @Test
    void testUnboundedTotalStaysFinite() {
        List<SubtaskRateInfo> subtasks =
                Arrays.asList(new SubtaskRateInfo(0, NAN, NAN), new SubtaskRateInfo(1, NAN, NAN));

        Map<Integer, Double> result = RateAllocations.allocate(Double.MAX_VALUE, subtasks);

        assertThat(result.values()).allSatisfy(rate -> assertThat(rate).isFinite());
    }
}
