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

package org.apache.flink.api.watermark;

import org.apache.flink.annotation.Experimental;

/**
 * The {@link WatermarkSpecs} class defines the comparison semantics among {@link
 * GeneralizedWatermark}s. It includes enumerations for specifying different types of watermark
 * comparisons and watermark types.
 *
 * @see GeneralizedWatermark
 */
@Experimental
public final class WatermarkSpecs {

    /**
     * The {@link BoolWatermarkComparison} enum defines the comparison semantics for boolean
     * watermarks. It includes logical operations such as {@code OR} and {@code AND}.
     */
    @Experimental
    public enum BoolWatermarkComparison {
        /** Logical OR comparison for boolean watermarks. */
        OR,

        /** Logical AND comparison for boolean watermarks. */
        AND
    }

    /**
     * The {@link NumericWatermarkComparison} enum defines the comparison semantics for numeric
     * watermarks. It includes operations such as {@code MIN} and {@code MAX}.
     */
    @Experimental
    public enum NumericWatermarkComparison {
        /** Minimum value comparison for numeric watermarks. */
        MIN,

        /** Maximum value comparison for numeric watermarks. */
        MAX
    }
}
