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
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * An {@link ArgumentTypeStrategy} for an ARRAY argument whose elements, or a MAP argument whose
 * keys, a function compares with each other.
 *
 * <p>The elements or keys must support equality, see {@link
 * LogicalTypeChecks#areComparable(LogicalType, LogicalType, StructuredComparison)}.
 */
@Internal
public final class EqualsComparableElementArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final ArgumentTypeStrategy collectionStrategy;

    public EqualsComparableElementArgumentTypeStrategy(LogicalTypeRoot collectionRoot) {
        Preconditions.checkArgument(
                collectionRoot == LogicalTypeRoot.ARRAY || collectionRoot == LogicalTypeRoot.MAP);
        this.collectionStrategy = new RootArgumentTypeStrategy(collectionRoot, null);
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        return collectionStrategy
                .inferArgumentType(callContext, argumentPos, throwOnFailure)
                .flatMap(type -> checkElementEquality(callContext, type, throwOnFailure));
    }

    @Override
    public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
        return collectionStrategy.getExpectedArgument(functionDefinition, argumentPos);
    }

    /** Checks that the elements of an ARRAY type, or the keys of a MAP type, support equality. */
    static Optional<DataType> checkElementEquality(
            CallContext callContext, DataType collectionType, boolean throwOnFailure) {
        // the element type of an ARRAY and the key type of a MAP are the first child
        final List<LogicalType> children = collectionType.getLogicalType().getChildren();
        if (children.isEmpty()) {
            return Optional.of(collectionType);
        }
        final LogicalType elementType = children.get(0);
        if (!LogicalTypeChecks.areComparable(
                elementType, elementType, StructuredComparison.EQUALS)) {
            final boolean isMap = collectionType.getLogicalType().is(LogicalTypeRoot.MAP);
            return callContext.fail(
                    throwOnFailure,
                    "%s of type %s cannot be compared, because the type has no equality. "
                            + "Cast the %s to a comparable type first.",
                    isMap ? "Map keys" : "Array elements",
                    elementType.copy(true).asSummaryString(),
                    isMap ? "keys" : "elements");
        }
        return Optional.of(collectionType);
    }
}
