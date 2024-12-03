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

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.Objects;

/**
 * The {@link WatermarkCombinationPolicy} defines when and how to the combine {@link Watermark}s.
 *
 * <p>The watermark combination process will first check the setting of {@link
 * #combineWaitForAllChannels}. If it is set to true, the ProcessFunction must receive watermarks
 * from all input channels before it can proceed with the combination.
 *
 * <p>The actual combination of watermarks will then be executed using the specified {@link
 * #watermarkCombinationFunction}.
 */
@Experimental
public class WatermarkCombinationPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    private final WatermarkCombinationFunction watermarkCombinationFunction;

    // Whether the combine process should be executed after the process function receives watermarks
    // from both upstream channels.
    private final boolean combineWaitForAllChannels;

    public WatermarkCombinationPolicy(
            WatermarkCombinationFunction watermarkCombinationFunction,
            boolean combineWaitForAllChannels) {
        this.watermarkCombinationFunction = watermarkCombinationFunction;
        this.combineWaitForAllChannels = combineWaitForAllChannels;
    }

    public WatermarkCombinationFunction getWatermarkCombinationFunction() {
        return watermarkCombinationFunction;
    }

    public boolean isCombineWaitForAllChannels() {
        return combineWaitForAllChannels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WatermarkCombinationPolicy that = (WatermarkCombinationPolicy) o;
        return combineWaitForAllChannels == that.combineWaitForAllChannels
                && Objects.equals(watermarkCombinationFunction, that.watermarkCombinationFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(watermarkCombinationFunction, combineWaitForAllChannels);
    }
}
