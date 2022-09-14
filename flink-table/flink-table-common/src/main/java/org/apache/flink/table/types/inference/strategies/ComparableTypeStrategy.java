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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An {@link InputTypeStrategy} that checks if all input arguments can be compared with each other
 * with the minimal provided comparison.
 *
 * <p>It requires at least one argument. In case of one argument, the argument must be comparable
 * with itself (e.g. for aggregations).
 *
 * <p>For the rules which types are comparable with which types see {@link
 * #areComparable(LogicalType, LogicalType)}.
 */
@Internal
public final class ComparableTypeStrategy implements InputTypeStrategy {
    private final StructuredComparison requiredComparison;
    private final ConstantArgumentCount argumentCount;

    public ComparableTypeStrategy(
            ConstantArgumentCount argumentCount, StructuredComparison requiredComparison) {
        Preconditions.checkArgument(
                argumentCount.getMinCount().map(c -> c >= 1).orElse(false),
                "Comparable type strategy requires at least one argument. Actual minimal argument count: %s",
                argumentCount.getMinCount().map(Objects::toString).orElse("<None>"));
        Preconditions.checkArgument(requiredComparison != StructuredComparison.NONE);
        this.requiredComparison = requiredComparison;
        this.argumentCount = argumentCount;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return argumentCount;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (argumentDataTypes.size() == 1) {
            final LogicalType argType = argumentDataTypes.get(0).getLogicalType();
            if (!areComparable(argType, argType)) {
                return callContext.fail(
                        throwOnFailure,
                        "Type '%s' should support %s comparison with itself.",
                        argType,
                        comparisonToString());
            }
        } else {
            for (int i = 0; i < argumentDataTypes.size() - 1; i++) {
                final LogicalType firstType = argumentDataTypes.get(i).getLogicalType();
                final LogicalType secondType = argumentDataTypes.get(i + 1).getLogicalType();

                if (!areComparable(firstType, secondType)) {
                    return callContext.fail(
                            throwOnFailure,
                            "All types in a comparison should support %s comparison with each other. "
                                    + "Can not compare %s with %s",
                            comparisonToString(),
                            firstType,
                            secondType);
                }
            }
        }

        return Optional.of(argumentDataTypes);
    }

    private String comparisonToString() {
        return requiredComparison == StructuredComparison.EQUALS
                ? "'EQUALS'"
                : "both 'EQUALS' and 'ORDER'";
    }

    private boolean areComparable(LogicalType firstType, LogicalType secondType) {
        return areComparableWithNormalizedNullability(firstType.copy(true), secondType.copy(true));
    }

    private boolean areComparableWithNormalizedNullability(
            LogicalType firstType, LogicalType secondType) {
        // A hack to support legacy types. To be removed when we drop the legacy types.
        if (firstType instanceof LegacyTypeInformationType
                || secondType instanceof LegacyTypeInformationType) {
            return true;
        }

        // everything is comparable with null, it should return null in that case
        if (firstType.is(LogicalTypeRoot.NULL) || secondType.is(LogicalTypeRoot.NULL)) {
            return true;
        }

        if (firstType.getTypeRoot() == secondType.getTypeRoot()) {
            return areTypesOfSameRootComparable(firstType, secondType);
        }

        if (firstType.is(LogicalTypeFamily.NUMERIC) && secondType.is(LogicalTypeFamily.NUMERIC)) {
            return true;
        }

        // DATE + ALL TIMESTAMPS
        if (firstType.is(LogicalTypeFamily.DATETIME) && secondType.is(LogicalTypeFamily.DATETIME)) {
            return true;
        }

        // VARCHAR + CHAR (we do not compare collations here)
        if (firstType.is(LogicalTypeFamily.CHARACTER_STRING)
                && secondType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return true;
        }

        // VARBINARY + BINARY
        if (firstType.is(LogicalTypeFamily.BINARY_STRING)
                && secondType.is(LogicalTypeFamily.BINARY_STRING)) {
            return true;
        }

        return false;
    }

    private boolean areTypesOfSameRootComparable(LogicalType firstType, LogicalType secondType) {
        switch (firstType.getTypeRoot()) {
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
                return areConstructedTypesComparable(firstType, secondType);
            case DISTINCT_TYPE:
                return areDistinctTypesComparable(firstType, secondType);
            case STRUCTURED_TYPE:
                return areStructuredTypesComparable(firstType, secondType);
            case RAW:
                return areRawTypesComparable(firstType, secondType);
            default:
                return true;
        }
    }

    private boolean areRawTypesComparable(LogicalType firstType, LogicalType secondType) {
        return firstType.equals(secondType)
                && Comparable.class.isAssignableFrom(
                        ((RawType<?>) firstType).getOriginatingClass());
    }

    private boolean areDistinctTypesComparable(LogicalType firstType, LogicalType secondType) {
        DistinctType firstDistinctType = (DistinctType) firstType;
        DistinctType secondDistinctType = (DistinctType) secondType;
        return firstType.equals(secondType)
                && areComparable(
                        firstDistinctType.getSourceType(), secondDistinctType.getSourceType());
    }

    private boolean areStructuredTypesComparable(LogicalType firstType, LogicalType secondType) {
        return firstType.equals(secondType) && hasRequiredComparison((StructuredType) firstType);
    }

    private boolean areConstructedTypesComparable(LogicalType firstType, LogicalType secondType) {
        List<LogicalType> firstChildren = firstType.getChildren();
        List<LogicalType> secondChildren = secondType.getChildren();

        if (firstChildren.size() != secondChildren.size()) {
            return false;
        }

        for (int i = 0; i < firstChildren.size(); i++) {
            if (!areComparable(firstChildren.get(i), secondChildren.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(Signature.Argument.ofGroupVarying("COMPARABLE")));
    }

    private Boolean hasRequiredComparison(StructuredType structuredType) {
        switch (requiredComparison) {
            case EQUALS:
                return structuredType.getComparison().isEquality();
            case FULL:
                return structuredType.getComparison().isComparison();
            case NONE:
            default:
                // this is not important, required comparison will never be NONE
                return true;
        }
    }
}
