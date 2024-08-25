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
 * The {@link BoolWatermarkDeclaration} interface extends the {@code WatermarkDeclaration} interface
 * and provides additional functionality specific to boolean-type watermarks. It includes methods
 * for obtaining comparison semantics and creating new boolean watermarks.
 *
 * <p>This interface is marked as {@code Experimental}, indicating that it may change in future
 * releases.
 *
 * <p>Implementing classes should provide a concrete implementation of the {@code
 * getComparisonSemantics} method to define how boolean watermarks are compared.
 *
 * @see WatermarkDeclaration
 */
@Experimental
public interface BoolWatermarkDeclaration extends WatermarkDeclaration {

    /**
     * Returns the boolean comparison semantics for watermarks.
     *
     * @return a {@code WatermarkSpecs.BoolWatermarkComparison} representing the comparison
     *     semantics for bool watermarks
     */
    WatermarkSpecs.BoolWatermarkComparison getComparisonSemantics();

    /**
     * Creates a new {@code BoolWatermark} with the specified boolean value.
     *
     * @param val the boolean value for the new watermark
     * @return a new instance of {@code BoolWatermark} with the specified boolean value
     */
    default BoolWatermark newWatermark(boolean val) {
        return new BoolWatermark(val, this.getIdentifier());
    }
}
