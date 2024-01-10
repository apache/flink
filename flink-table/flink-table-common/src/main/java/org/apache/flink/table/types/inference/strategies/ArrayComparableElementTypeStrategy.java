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
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * An {@link InputTypeStrategy} that checks if the input argument is an ARRAY type and check whether
 * its' elements are comparable.
 *
 * <p>It requires one argument.
 *
 * <p>For the rules which types are comparable with which types see {@link
 * #areComparable(LogicalType, LogicalType)}.
 */
@Internal
public final class ArrayComparableElementTypeStrategy implements InputTypeStrategy {
    private final StructuredComparison requiredComparison;
    private final ConstantArgumentCount argumentCount;

    public ArrayComparableElementTypeStrategy(StructuredComparison requiredComparison) {
        Preconditions.checkArgument(requiredComparison != StructuredComparison.NONE);
        this.requiredComparison = requiredComparison;
        this.argumentCount = ConstantArgumentCount.of(1);
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return argumentCount;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final DataType argumentType = argumentDataTypes.get(0);
        if (!argumentType.getLogicalType().is(LogicalTypeRoot.ARRAY)) {
            return callContext.fail(throwOnFailure, "All arguments requires to be an ARRAY type");
        }
        final DataType elementDataType = ((CollectionDataType) argumentType).getElementDataType();
        final LogicalType elementLogicalDataType = elementDataType.getLogicalType();
        if (!areComparable(elementLogicalDataType, elementLogicalDataType)) {
            return callContext.fail(
                    throwOnFailure,
                    "Type '%s' should support %s comparison with itself.",
                    elementLogicalDataType,
                    comparisonToString());
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

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(Signature.Argument.ofGroup("ARRAY<COMPARABLE>")));
    }
}
