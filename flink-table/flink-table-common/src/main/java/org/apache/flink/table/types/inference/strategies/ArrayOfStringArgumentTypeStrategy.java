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
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;

import static java.lang.Boolean.TRUE;

/** Strategy for an argument that must be an array of strings. */
@Internal
public final class ArrayOfStringArgumentTypeStrategy implements ArgumentTypeStrategy {
    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {

        final List<DataType> actualDataTypes = callContext.getArgumentDataTypes();
        DataType actualType = actualDataTypes.get(argumentPos);
        if (!actualType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.ARRAY)) {
            return callContext.fail(
                    throwOnFailure,
                    "Invalid input arguments. Expected signatures are:\n"
                            + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                            + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)",
                    actualDataTypes.toArray());
        }
        DataType elementDataType = ((CollectionDataType) actualType).getElementDataType();
        Optional<DataType> closedDataType =
                StrategyUtils.findDataType(
                        callContext,
                        throwOnFailure,
                        elementDataType,
                        LogicalTypeRoot.VARCHAR,
                        TRUE);
        if (closedDataType.isPresent()) {
            return Optional.of(actualType);
        }
        return callContext.fail(
                throwOnFailure,
                "The input argument should be ARRAY<STRING>",
                actualDataTypes.toArray());
    }

    @Override
    public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
        return Argument.of("ARRAY<STRING>");
    }
}
