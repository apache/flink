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
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer.strategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test class for {@link AIMDScalingStrategy}. */
class AIMDScalingStrategyTest {

    @Test
    void testScaleUpAdditively() {
        final int increaseRate = 10;
        final int currentRate = 4;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder(1000)
                        .setIncreaseRate(increaseRate)
                        .setDecreaseFactor(0.5)
                        .build();

        assertThat(scalingStrategy.scaleUp(currentRate)).isEqualTo(increaseRate + currentRate);
    }

    @Test
    void testScaleUpRespectsRateThreshold() {
        final int increaseRate = 10;
        final int currentRate = 14;
        final int rateThreshold = 20;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder(rateThreshold)
                        .setIncreaseRate(increaseRate)
                        .setDecreaseFactor(0.5)
                        .build();

        assertThat(scalingStrategy.scaleUp(increaseRate + currentRate)).isEqualTo(rateThreshold);
    }

    @Test
    void testScaleDownByFactor() {
        final double decreaseFactor = 0.4;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder(1000)
                        .setIncreaseRate(10)
                        .setDecreaseFactor(decreaseFactor)
                        .build();

        // round(10 * 0.4 = 4) = 4
        assertThat(scalingStrategy.scaleDown(10)).isEqualTo(4);

        // round(314 * 0.4 = 125.6) = 126
        assertThat(scalingStrategy.scaleDown(314)).isEqualTo(126);
    }

    @Test
    void testScaleDownByFactorStopsAtOne() {
        final double decreaseFactor = 0.1;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder(1000)
                        .setIncreaseRate(10)
                        .setDecreaseFactor(decreaseFactor)
                        .build();

        // even though round(1 * 0.1 = 0.1) = 0 , the lowest we go is 1
        assertThat(scalingStrategy.scaleDown(1)).isEqualTo(1);
    }

    @Test
    void testInvalidIncreaseRate() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder(100)
                                        .setIncreaseRate(0)
                                        .setDecreaseFactor(0.5)
                                        .build())
                .withMessageContaining("increaseRate must be positive integer.");
    }

    @Test
    void testInvalidDecreaseFactor() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder(100)
                                        .setIncreaseRate(1)
                                        .setDecreaseFactor(0)
                                        .build())
                .withMessageContaining("decreaseFactor must be strictly between 0.0 and 1.0.");

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder(100)
                                        .setIncreaseRate(1)
                                        .setDecreaseFactor(1.0)
                                        .build())
                .withMessageContaining("decreaseFactor must be strictly between 0.0 and 1.0.");
    }

    @Test
    void testInvalidRateThreshold() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder(0)
                                        .setIncreaseRate(1)
                                        .setDecreaseFactor(0.5)
                                        .build())
                .withMessageContaining("rateThreshold must be a positive integer.");
    }
}
