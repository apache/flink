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

package org.apache.flink.runtime.rest.messages.util.stats;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.flink.runtime.rest.messages.util.stats.StatsSummaryDto}. */
class StatsSummaryDtoTest {
    @Test
    void testEquals() {
        StatsSummaryDto dtoWithoutNaNDoubleValue =
                new StatsSummaryDto(1L, 1L, 1L, 0d, 0d, 0d, 0d, 0d);
        StatsSummaryDto dtoWithANaNDoubleValue =
                new StatsSummaryDto(1L, 1L, 1L, 0d, 0d, 0d, 0d, Double.NaN);
        assertThat(dtoWithoutNaNDoubleValue).isNotEqualTo(dtoWithANaNDoubleValue);
        assertThat(dtoWithANaNDoubleValue).isNotEqualTo(dtoWithoutNaNDoubleValue);

        StatsSummaryDto dtoWithAllNaNDouble1 =
                new StatsSummaryDto(
                        1L, 1L, 1L, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        StatsSummaryDto dtoWithAllNaNDouble2 =
                new StatsSummaryDto(
                        1L, 1L, 1L, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        assertThat(dtoWithAllNaNDouble1).isEqualTo(dtoWithAllNaNDouble2);
        assertThat(dtoWithAllNaNDouble2).isEqualTo(dtoWithAllNaNDouble1);

        StatsSummaryDto dtoWithoutNaNDoubleValue1 =
                new StatsSummaryDto(1L, 1L, 1L, 0d, 0d, 0d, 0d, 0d);
        StatsSummaryDto dtoWithoutNaNDoubleValue2 =
                new StatsSummaryDto(1L, 1L, 1L, 0d, 0d, 0d, 0d, 0d);
        assertThat(dtoWithoutNaNDoubleValue1).isEqualTo(dtoWithoutNaNDoubleValue2);
        assertThat(dtoWithoutNaNDoubleValue2).isEqualTo(dtoWithoutNaNDoubleValue1);

        StatsSummaryDto dtoWithoutNaNDoubleValue3 =
                new StatsSummaryDto(1L, 1L, 1L, 0d, 1d, 0d, 0d, 0d);
        StatsSummaryDto dtoWithoutNaNDoubleValue4 =
                new StatsSummaryDto(1L, 1L, 1L, 0d, 0d, 0d, 0d, 0d);
        assertThat(dtoWithoutNaNDoubleValue3).isNotEqualTo(dtoWithoutNaNDoubleValue4);
        assertThat(dtoWithoutNaNDoubleValue4).isNotEqualTo(dtoWithoutNaNDoubleValue3);
    }
}
