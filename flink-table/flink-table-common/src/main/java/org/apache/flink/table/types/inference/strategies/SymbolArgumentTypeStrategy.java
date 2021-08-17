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
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.SymbolType;

import java.util.Objects;
import java.util.Optional;

/** Strategy for a symbol argument of a specific {@link TableSymbol} enum. */
@Internal
public class SymbolArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final Class<? extends Enum<? extends TableSymbol>> symbolClass;

    public SymbolArgumentTypeStrategy(Class<? extends Enum<? extends TableSymbol>> symbolClass) {
        this.symbolClass = symbolClass;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final DataType argumentType = callContext.getArgumentDataTypes().get(argumentPos);
        if (argumentType.getLogicalType().getTypeRoot() != LogicalTypeRoot.SYMBOL) {
            if (throwOnFailure) {
                throw callContext.newValidationError(
                        "Unsupported argument type. Expected symbol type '%s' but actual type was '%s'.",
                        symbolClass.getSimpleName(), argumentType);
            } else {
                return Optional.empty();
            }
        }

        final SymbolType<?> symbolType = (SymbolType<?>) argumentType.getLogicalType();
        if (symbolType.getSymbolClass() != symbolClass) {
            if (throwOnFailure) {
                throw callContext.newValidationError(
                        "Unsupported argument symbol type. Expected symbol '%s' but actual symbol was '%s'.",
                        symbolClass.getSimpleName(),
                        symbolType.getDefaultConversion().getSimpleName());
            } else {
                return Optional.empty();
            }
        }

        return Optional.of(argumentType);
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.of(String.format("<%s>", symbolClass.getSimpleName()));
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        SymbolArgumentTypeStrategy that = (SymbolArgumentTypeStrategy) other;
        return Objects.equals(symbolClass, that.symbolClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbolClass);
    }
}
