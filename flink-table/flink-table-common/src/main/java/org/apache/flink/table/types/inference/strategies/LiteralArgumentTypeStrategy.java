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

import java.util.Objects;
import java.util.Optional;

/** Strategy that checks if an argument is a literal. */
@Internal
public final class LiteralArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final boolean allowNull;

    public LiteralArgumentTypeStrategy(boolean allowNull) {
        this.allowNull = allowNull;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        if (!callContext.isArgumentLiteral(argumentPos)) {
            return callContext.fail(throwOnFailure, "Literal expected.");
        }
        if (callContext.isArgumentNull(argumentPos) && !allowNull) {
            return callContext.fail(throwOnFailure, "Literal must not be NULL.");
        }
        return Optional.of(callContext.getArgumentDataTypes().get(argumentPos));
    }

    @Override
    public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
        if (allowNull) {
            return Argument.ofGroup("LITERAL");
        }
        return Argument.ofGroup("LITERAL NOT NULL");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LiteralArgumentTypeStrategy that = (LiteralArgumentTypeStrategy) o;
        return allowNull == that.allowNull;
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowNull);
    }
}
