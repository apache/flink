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
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Optional;

/**
 * An {@link ArgumentTypeStrategy} that expects a {@link LogicalTypeFamily#INTEGER_NUMERIC} starting
 * from 0.
 */
@Internal
public final class IndexArgumentTypeStrategy implements ArgumentTypeStrategy {
    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final DataType indexType = callContext.getArgumentDataTypes().get(argumentPos);

        if (indexType.getLogicalType().is(LogicalTypeFamily.INTEGER_NUMERIC)) {
            if (callContext.isArgumentLiteral(argumentPos)) {
                Optional<Number> literalVal =
                        callContext.getArgumentValue(argumentPos, Number.class);
                if (literalVal.isPresent() && literalVal.get().longValue() < 0) {
                    return callContext.fail(
                            throwOnFailure,
                            "Index must be an integer starting from '0', but was '%s'.",
                            literalVal.get());
                }
            }

            return Optional.of(indexType);
        } else {
            return callContext.fail(throwOnFailure, "Index can only be an INTEGER NUMERIC type.");
        }
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.ofGroup(LogicalTypeFamily.INTEGER_NUMERIC);
    }
}
