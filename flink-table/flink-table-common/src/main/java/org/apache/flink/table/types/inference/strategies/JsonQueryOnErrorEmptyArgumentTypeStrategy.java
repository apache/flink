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
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;

/**
 * Argument strategy for {@link BuiltInFunctionDefinitions#JSON_QUERY} to check the `ON EMPTY`
 * and/or `ON ERROR` behaviour in combination with the return type.
 */
@Internal
public class JsonQueryOnErrorEmptyArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final SymbolArgumentTypeStrategy<JsonQueryOnEmptyOrError> symbolStrategy =
            new SymbolArgumentTypeStrategy<>(JsonQueryOnEmptyOrError.class);

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final List<DataType> actualDataTypes = callContext.getArgumentDataTypes();

        final Optional<DataType> symbolType =
                symbolStrategy.inferArgumentType(callContext, argumentPos, throwOnFailure);
        if (!symbolType.isPresent()) {
            return Optional.empty();
        }

        final LogicalType returnType = actualDataTypes.get(2).getLogicalType();
        if (returnType.is(LogicalTypeRoot.ARRAY)
                && callContext.getArgumentValue(argumentPos, JsonQueryOnEmptyOrError.class).get()
                        == JsonQueryOnEmptyOrError.EMPTY_OBJECT) {
            String behaviour = argumentPos == 5 ? "on error" : "on empty";
            return callContext.fail(
                    throwOnFailure,
                    String.format(
                            "Illegal %s behavior 'EMPTY OBJECT' for return type: %s",
                            behaviour, returnType.asSummaryString()),
                    actualDataTypes.toArray());
        }

        return symbolType;
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return symbolStrategy.getExpectedArgument(functionDefinition, argumentPos);
    }
}
