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
import org.apache.flink.api.common.functions.Function;

/**
 * The {@link WatermarkCombinationFunction} defines the comparison/combination semantics among
 * {@link Watermark}s.
 */
@Experimental
public interface WatermarkCombinationFunction extends Function {
    /**
     * The {@link BoolWatermarkCombinationFunction} enum defines the combination semantics for
     * boolean watermarks. It includes logical operations such as {@code OR} and {@code AND}.
     */
    @Experimental
    enum BoolWatermarkCombinationFunction implements WatermarkCombinationFunction {
        /** Logical OR combination for boolean watermarks. */
        OR,

        /** Logical AND combination for boolean watermarks. */
        AND
    }

    /**
     * The {@link NumericWatermarkCombinationFunction} enum defines the combination semantics for
     * numeric watermarks. It includes operations such as {@code MIN} and {@code MAX}.
     */
    @Experimental
    enum NumericWatermarkCombinationFunction implements WatermarkCombinationFunction {
        /** Minimum value combination for numeric watermarks. */
        MIN,

        /** Maximum value combination for numeric watermarks. */
        MAX
    }
}
