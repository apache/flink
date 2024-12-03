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

package org.apache.flink.streaming.runtime.watermark;

import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;
import org.apache.flink.util.Preconditions;

/** An alignable {@link BoolWatermarkDeclaration}. */
public class AlignableBoolWatermarkDeclaration extends BoolWatermarkDeclaration
        implements Alignable {
    private final boolean isAligned;

    public AlignableBoolWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicyForChannel,
            WatermarkHandlingStrategy defaultHandlingStrategyForFunction,
            boolean isAligned) {
        super(identifier, combinationPolicyForChannel, defaultHandlingStrategyForFunction);
        Preconditions.checkArgument(
                !isAligned || combinationPolicyForChannel.isCombineWaitForAllChannels(),
                "The aligned watermark must wait for all channels during the combining process. Please set the WatermarkCombinationPolicy#combineWaitForAllChannels to true.");
        this.isAligned = isAligned;
    }

    public boolean isAligned() {
        return isAligned;
    }
}
