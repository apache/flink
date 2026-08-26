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
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.Optional;

/**
 * Strategy for an argument that must be an array of map entries, i.e. an {@code ARRAY} whose
 * element is a {@code ROW} with exactly two fields. The first field becomes the map key, the second
 * one the map value.
 */
@Internal
public final class ArrayOfEntriesArgumentTypeStrategy implements ArgumentTypeStrategy {

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final DataType actualType = callContext.getArgumentDataTypes().get(argumentPos);
        if (!actualType.getLogicalType().is(LogicalTypeRoot.ARRAY)) {
            return callContext.fail(
                    throwOnFailure,
                    "The 'input' argument must be ARRAY<ROW<key, value>>, but actual type was '%s'.",
                    actualType.getLogicalType().asSummaryString());
        }

        final LogicalType elementType =
                ((CollectionDataType) actualType).getElementDataType().getLogicalType();
        if (!elementType.is(LogicalTypeRoot.ROW)
                || LogicalTypeChecks.getFieldCount(elementType) != 2) {
            return callContext.fail(
                    throwOnFailure,
                    "The 'input' argument must be ARRAY<ROW<key, value>>, but the array element "
                            + "type was '%s'. The element must be a ROW with exactly two fields.",
                    elementType.asSummaryString());
        }

        // the key field must support equality, otherwise duplicate keys cannot be detected
        final LogicalType keyType = LogicalTypeChecks.getFieldTypes(elementType).get(0);
        if (!LogicalTypeChecks.areComparable(keyType, keyType, StructuredComparison.EQUALS)) {
            return callContext.fail(
                    throwOnFailure,
                    "The map key type '%s' does not support equality comparison and therefore "
                            + "cannot be used as the first field of a map entry.",
                    keyType.asSummaryString());
        }

        return Optional.of(actualType);
    }

    @Override
    public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
        return Argument.of("ARRAY<ROW<key, value>>");
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ArrayOfEntriesArgumentTypeStrategy;
    }

    @Override
    public int hashCode() {
        return ArrayOfEntriesArgumentTypeStrategy.class.hashCode();
    }
}
