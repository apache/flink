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
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Argument type strategy that checks and casts for a common, least restrictive type of all
 * arguments.
 *
 * <p>Nullability can be preserved if needed.
 */
@Internal
public final class CommonArgumentTypeStrategy implements ArgumentTypeStrategy {

    private static final Signature.Argument COMMON_ARGUMENT = Signature.Argument.of("<COMMON>");

    private final boolean preserveNullability;

    public CommonArgumentTypeStrategy(boolean preserveNullability) {
        this.preserveNullability = preserveNullability;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final List<LogicalType> actualTypes =
                callContext.getArgumentDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());
        return LogicalTypeMerging.findCommonType(actualTypes)
                .map(
                        commonType ->
                                preserveNullability
                                        ? commonType.copy(actualTypes.get(argumentPos).isNullable())
                                        : commonType)
                .map(TypeConversions::fromLogicalToDataType);
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return COMMON_ARGUMENT;
    }
}
