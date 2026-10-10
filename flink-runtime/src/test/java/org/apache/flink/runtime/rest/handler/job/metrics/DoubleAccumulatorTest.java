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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;

public class DoubleAccumulatorTest {

    @ParameterizedTest
    @MethodSource("dataSkewTests")
    public void testDataSkew(double n1, double n2, double n3, double expectedSkew) {
        DoubleAccumulator.DoubleDataSkew dataSkew =
                DoubleAccumulator.DoubleDataSkewFactory.get().get(n1);
        dataSkew.add(n2);
        dataSkew.add(n3);
        assertThat(dataSkew.getValue()).isCloseTo(expectedSkew, within(0.5));
    }

    @Test
    public void testDataSkewOnEmptyList() {
        DoubleAccumulator.DoubleDataSkew dataSkew = new DoubleAccumulator.DoubleDataSkew();
        assertThat(dataSkew.getValue()).isEqualTo(0.0);
    }

    @Test
    public void testDataSkewOnSingleValueList() {
        DoubleAccumulator.DoubleDataSkew dataSkew =
                DoubleAccumulator.DoubleDataSkewFactory.get().get(123);
        assertThat(dataSkew.getValue()).isEqualTo(0.0);
    }

    @Test
    public void testPercentile() {
        // Sample: 10, 20, ..., 100 (N=10). commons-math3 LEGACY interpolation uses
        // pos = percentile * (N + 1) / 100:
        //   p50 -> pos = 5.5  -> 50 + 0.5 * (60 - 50) = 55
        //   p90 -> pos = 9.9  -> 90 + 0.9 * (100 - 90) = 99
        //   p99 -> pos = 10.89 -> pos >= N, clamped to max = 100
        assertThat(percentileOf(DoubleAccumulator.DoublePercentileFactory.p50()).getValue())
                .isCloseTo(55.0, within(0.001));
        assertThat(percentileOf(DoubleAccumulator.DoublePercentileFactory.p90()).getValue())
                .isCloseTo(99.0, within(0.001));
        assertThat(percentileOf(DoubleAccumulator.DoublePercentileFactory.p99()).getValue())
                .isCloseTo(100.0, within(0.001));
    }

    @Test
    public void testPercentileOnSingleValueList() {
        DoubleAccumulator.DoublePercentile percentile =
                DoubleAccumulator.DoublePercentileFactory.p90().get(42.0);
        assertThat(percentile.getValue()).isEqualTo(42.0);
    }

    @Test
    public void testPercentileName() {
        assertThat(DoubleAccumulator.DoublePercentileFactory.p50().get(1.0).getName())
                .isEqualTo("p50");
        assertThat(DoubleAccumulator.DoublePercentileFactory.p90().get(1.0).getName())
                .isEqualTo("p90");
        assertThat(DoubleAccumulator.DoublePercentileFactory.p99().get(1.0).getName())
                .isEqualTo("p99");
    }

    private static DoubleAccumulator.DoublePercentile percentileOf(
            DoubleAccumulator.DoublePercentileFactory factory) {
        DoubleAccumulator.DoublePercentile percentile = factory.get(10.0);
        for (double value : new double[] {20, 30, 40, 50, 60, 70, 80, 90, 100}) {
            percentile.add(value);
        }
        return percentile;
    }

    private static Stream<Arguments> dataSkewTests() {
        // Data set, followed by the expected data skew percentage
        return Stream.of(
                // Avg: (23 + 3 + 10) / 3 = 12
                // Avg Absolute Deviation = ( (23 - 12) + (12 - 3) + ( 12 - 10) ) / 3 = 7.33
                // Skew Percentage = 7.33/12 * 100 -> 61%
                Arguments.of(23.0, 3.0, 10.0, 61.0),
                // Avg: (300 + 0 + 0) / 3 = 100
                // Avg Absolute Deviation = ( (300 - 100) + (100 - 0) + (100 - 0) ) / 3 =  133
                // Skew Percentage = 133/100 * 100 -> 133% should be capped at 100
                Arguments.of(300.0, 0.0, 0.0, 100.0),
                // Test against any possible division by zero errors
                Arguments.of(0.0, 0.0, 0.0, 0.0),
                // Test low skew,
                Arguments.of(50.0, 51.0, 52.0, 1.0));
    }
}
