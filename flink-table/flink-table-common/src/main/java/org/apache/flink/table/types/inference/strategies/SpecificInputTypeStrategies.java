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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.logical.StructuredType;

import static org.apache.flink.table.types.inference.InputTypeStrategies.comparable;

/**
 * Entry point for specific input type strategies not covered in {@link InputTypeStrategies}.
 *
 * <p>This primarily serves the purpose of reducing visibility of individual type strategy
 * implementations to avoid polluting the API classpath.
 */
@Internal
public final class SpecificInputTypeStrategies {

    /** See {@link CastInputTypeStrategy}. */
    public static final InputTypeStrategy CAST = new CastInputTypeStrategy();

    /** See {@link MapInputTypeStrategy}. */
    public static final InputTypeStrategy MAP = new MapInputTypeStrategy();

    /** See {@link CurrentWatermarkTypeStrategy}. */
    public static final InputTypeStrategy CURRENT_WATERMARK =
            new CurrentWatermarkInputTypeStrategy();

    // --------------------------------------------------------------------------------------------
    // Strategies composed of other strategies
    // --------------------------------------------------------------------------------------------

    /**
     * Strategy specific for {@link BuiltInFunctionDefinitions#ARRAY}.
     *
     * <p>It expects at least one argument. All the arguments must have a common super type.
     */
    public static final InputTypeStrategy ARRAY =
            new CommonInputTypeStrategy(ConstantArgumentCount.from(1));

    /**
     * Strategy that checks all types are fully comparable with each other. Requires exactly two
     * arguments.
     */
    public static final InputTypeStrategy TWO_FULLY_COMPARABLE =
            comparable(ConstantArgumentCount.of(2), StructuredType.StructuredComparison.FULL);

    /**
     * Strategy that checks all types are equals comparable with each other. Requires exactly two
     * arguments.
     */
    public static final InputTypeStrategy TWO_EQUALS_COMPARABLE =
            comparable(ConstantArgumentCount.of(2), StructuredType.StructuredComparison.EQUALS);

    private SpecificInputTypeStrategies() {
        // no instantiation
    }
}
