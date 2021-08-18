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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.CommonTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;
import org.apache.flink.table.types.inference.strategies.FirstTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ForceNullableTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MappingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MatchFamilyTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MissingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.NullableIfArgsTypeStrategy;
import org.apache.flink.table.types.inference.strategies.UseArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.VaryingStringTypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Strategies for inferring an output or accumulator data type of a function call.
 *
 * @see TypeStrategy
 */
@Internal
public final class TypeStrategies {

    /** Placeholder for a missing type strategy. */
    public static final TypeStrategy MISSING = new MissingTypeStrategy();

    /** Type strategy that returns a common, least restrictive type of all arguments. */
    public static final TypeStrategy COMMON = new CommonTypeStrategy();

    /** Type strategy that returns a fixed {@link DataType}. */
    public static TypeStrategy explicit(DataType dataType) {
        return new ExplicitTypeStrategy(dataType);
    }

    /** Type strategy that returns the n-th input argument. */
    public static TypeStrategy argument(int pos) {
        return new UseArgumentTypeStrategy(pos);
    }

    /** Type strategy that returns the first type that could be inferred. */
    public static TypeStrategy first(TypeStrategy... strategies) {
        return new FirstTypeStrategy(Arrays.asList(strategies));
    }

    /** Type strategy that returns the given argument if it is of the same logical type family. */
    public static TypeStrategy matchFamily(int argumentPos, LogicalTypeFamily family) {
        return new MatchFamilyTypeStrategy(argumentPos, family);
    }

    /**
     * Type strategy that maps an {@link InputTypeStrategy} to a {@link TypeStrategy} if the input
     * strategy infers identical types.
     */
    public static TypeStrategy mapping(Map<InputTypeStrategy, TypeStrategy> mappings) {
        return new MappingTypeStrategy(mappings);
    }

    /** Type strategy which forces the given {@param initialStrategy} to be nullable. */
    public static TypeStrategy forceNullable(TypeStrategy initialStrategy) {
        return new ForceNullableTypeStrategy(initialStrategy);
    }

    /**
     * A type strategy that can be used to make a result type nullable if any of the selected input
     * arguments is nullable. Otherwise the type will be not null.
     */
    public static TypeStrategy nullableIfArgs(
            ConstantArgumentCount includedArgs, TypeStrategy initialStrategy) {
        return new NullableIfArgsTypeStrategy(includedArgs, initialStrategy);
    }

    /**
     * A type strategy that can be used to make a result type nullable if any of the input arguments
     * is nullable. Otherwise the type will be not null.
     */
    public static TypeStrategy nullableIfArgs(TypeStrategy initialStrategy) {
        return nullableIfArgs(ConstantArgumentCount.any(), initialStrategy);
    }

    /**
     * A type strategy that ensures that the result type is either {@link LogicalTypeRoot#VARCHAR}
     * or {@link LogicalTypeRoot#VARBINARY} from their corresponding non-varying roots.
     */
    public static TypeStrategy varyingString(TypeStrategy initialStrategy) {
        return new VaryingStringTypeStrategy(initialStrategy);
    }

    /**
     * Type strategy specific for aggregations that partially produce different nullability
     * depending whether the result is grouped or not.
     */
    public static TypeStrategy aggArg0(
            Function<LogicalType, LogicalType> aggType, boolean nullableIfGroupingEmpty) {
        return callContext -> {
            final DataType argDataType = callContext.getArgumentDataTypes().get(0);
            final LogicalType argType = argDataType.getLogicalType();
            LogicalType result = aggType.apply(argType);
            if (nullableIfGroupingEmpty && !callContext.isGroupedAggregation()) {
                // null only if condition is met, otherwise arguments nullability
                result = result.copy(true);
            } else if (!nullableIfGroupingEmpty) {
                // never null
                result = result.copy(false);
            }
            return Optional.of(fromLogicalToDataType(result));
        };
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private TypeStrategies() {
        // no instantiation
    }
}
