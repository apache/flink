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

package org.apache.flink.api.common.watermark;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WatermarkDeclarations}. */
class WatermarkDeclarationsTest {

    private static final String DEFAULT_WATERMARK_IDENTIFIER = "default";

    @Test
    void testCreatedLongWatermarkDeclarationDefaultValue() {
        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER).typeLong().build();
        assertThat(watermarkDeclaration.getIdentifier()).isEqualTo(DEFAULT_WATERMARK_IDENTIFIER);
        assertThat(watermarkDeclaration.getCombinationPolicy().getWatermarkCombinationFunction())
                .isEqualTo(WatermarkCombinationFunction.NumericWatermarkCombinationFunction.MIN);
        assertThat(watermarkDeclaration.getCombinationPolicy().isCombineWaitForAllChannels())
                .isEqualTo(false);
        assertThat(watermarkDeclaration.getDefaultHandlingStrategy())
                .isEqualTo(WatermarkHandlingStrategy.FORWARD);
    }

    @Test
    void testCreatedBoolWatermarkDeclarationDefaultValue() {
        BoolWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER).typeBool().build();
        assertThat(watermarkDeclaration.getIdentifier()).isEqualTo(DEFAULT_WATERMARK_IDENTIFIER);
        assertThat(watermarkDeclaration.getCombinationPolicy().getWatermarkCombinationFunction())
                .isEqualTo(WatermarkCombinationFunction.BoolWatermarkCombinationFunction.AND);
        assertThat(watermarkDeclaration.getCombinationPolicy().isCombineWaitForAllChannels())
                .isEqualTo(false);
        assertThat(watermarkDeclaration.getDefaultHandlingStrategy())
                .isEqualTo(WatermarkHandlingStrategy.FORWARD);
    }

    @Test
    void testBuildLongWatermarkDeclaration() {
        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .combineWaitForAllChannels(true)
                        .defaultHandlingStrategyIgnore()
                        .build();
        assertThat(watermarkDeclaration.getIdentifier()).isEqualTo(DEFAULT_WATERMARK_IDENTIFIER);
        assertThat(watermarkDeclaration.getCombinationPolicy().getWatermarkCombinationFunction())
                .isEqualTo(WatermarkCombinationFunction.NumericWatermarkCombinationFunction.MAX);
        assertThat(watermarkDeclaration.getCombinationPolicy().isCombineWaitForAllChannels())
                .isEqualTo(true);
        assertThat(watermarkDeclaration.getDefaultHandlingStrategy())
                .isEqualTo(WatermarkHandlingStrategy.IGNORE);
    }

    @Test
    void testBuildBoolWatermarkDeclaration() {
        BoolWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeBool()
                        .combineFunctionOR()
                        .combineWaitForAllChannels(true)
                        .defaultHandlingStrategyIgnore()
                        .build();
        assertThat(watermarkDeclaration.getIdentifier()).isEqualTo(DEFAULT_WATERMARK_IDENTIFIER);
        assertThat(watermarkDeclaration.getCombinationPolicy().getWatermarkCombinationFunction())
                .isEqualTo(WatermarkCombinationFunction.BoolWatermarkCombinationFunction.OR);
        assertThat(watermarkDeclaration.getCombinationPolicy().isCombineWaitForAllChannels())
                .isEqualTo(true);
        assertThat(watermarkDeclaration.getDefaultHandlingStrategy())
                .isEqualTo(WatermarkHandlingStrategy.IGNORE);
    }
}
