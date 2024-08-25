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
 * The {@link LongWatermarkDeclaration} interface extends the {@code WatermarkDeclaration} interface
 * and provides additional functionality specific to long-type watermarks. It includes methods for
 * obtaining comparison semantics and creating new long watermarks.
 *
 * <p>Implementing classes should provide a concrete implementation of the {@code
 * getComparisonSemantics} method to define how watermarks are compared.
 *
 * @see WatermarkDeclaration
 */
@Experimental
public interface LongWatermarkDeclaration extends WatermarkDeclaration {

    /**
     * Returns the numeric comparison semantics.
     *
     * @return a {@code WatermarkSpecs.NumericWatermarkComparison} representing the comparison
     *     semantics for long watermarks
     */
    WatermarkSpecs.NumericWatermarkComparison getComparisonSemantics();

    /**
     * Creates a new {@code LongWatermark} with the specified value.
     *
     * @param val the long value for the new watermark
     * @return a new instance of {@code LongWatermark} with the specified value
     */
    default LongWatermark newWatermark(long val) {
        return new LongWatermark(val, this.getIdentifier());
    }
}
