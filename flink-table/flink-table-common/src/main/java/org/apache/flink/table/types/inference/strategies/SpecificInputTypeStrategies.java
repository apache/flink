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
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;

import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.and;
import static org.apache.flink.table.types.inference.InputTypeStrategies.comparable;
import static org.apache.flink.table.types.inference.InputTypeStrategies.compositeSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.repeatingSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.symbol;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;

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

    /**
     * Input strategy for {@link BuiltInFunctionDefinitions#JSON_OBJECT}.
     *
     * <p>The first argument defines the on-null behavior and is followed by any number of key-value
     * pairs. Keys must be character string literals, while values are arbitrary expressions.
     */
    public static final InputTypeStrategy JSON_OBJECT =
            compositeSequence()
                    .argument(symbol(JsonOnNull.class))
                    .finishWithVarying(
                            repeatingSequence(
                                    and(logical(LogicalTypeFamily.CHARACTER_STRING), LITERAL),
                                    or(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.BINARY_STRING),
                                            logical(LogicalTypeFamily.TIMESTAMP),
                                            logical(LogicalTypeFamily.CONSTRUCTED),
                                            logical(LogicalTypeRoot.BOOLEAN),
                                            logical(LogicalTypeFamily.NUMERIC))));

    /** Input strategy for {@link BuiltInFunctionDefinitions#JSON_ARRAY}. */
    public static final InputTypeStrategy JSON_ARRAY =
            varyingSequence(
                    symbol(JsonOnNull.class),
                    or(
                            logical(LogicalTypeFamily.CHARACTER_STRING),
                            logical(LogicalTypeFamily.BINARY_STRING),
                            logical(LogicalTypeFamily.TIMESTAMP),
                            logical(LogicalTypeFamily.CONSTRUCTED),
                            logical(LogicalTypeRoot.BOOLEAN),
                            logical(LogicalTypeFamily.NUMERIC)));

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
