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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Strategy for a symbol argument of a specific {@link TableSymbol} enum. */
@Internal
public class SymbolArgumentTypeStrategy<T extends Enum<? extends TableSymbol>>
        implements ArgumentTypeStrategy {

    private final Class<T> symbolClass;
    private final Set<T> allowedVariants;

    public SymbolArgumentTypeStrategy(Class<T> symbolClass) {
        this(symbolClass, new HashSet<>(Arrays.asList(symbolClass.getEnumConstants())));
    }

    public SymbolArgumentTypeStrategy(Class<T> symbolClass, Set<T> allowedVariants) {
        this.symbolClass = symbolClass;
        this.allowedVariants = allowedVariants;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final DataType argumentType = callContext.getArgumentDataTypes().get(argumentPos);
        if (argumentType.getLogicalType().getTypeRoot() != LogicalTypeRoot.SYMBOL
                || !callContext.isArgumentLiteral(argumentPos)) {
            return callContext.fail(
                    throwOnFailure,
                    "Unsupported argument type. Expected symbol type '%s' but actual type was '%s'.",
                    symbolClass.getSimpleName(),
                    argumentType);
        }

        Optional<T> val = callContext.getArgumentValue(argumentPos, symbolClass);
        if (!val.isPresent()) {
            return callContext.fail(
                    throwOnFailure,
                    "Unsupported argument symbol type. Expected symbol '%s' but actual symbol was %s.",
                    symbolClass.getSimpleName(),
                    callContext
                            .getArgumentValue(argumentPos, Enum.class)
                            .map(e -> "'" + e.getClass().getSimpleName() + "'")
                            .orElse("invalid"));
        }
        if (!this.allowedVariants.contains(val.get())) {
            return callContext.fail(
                    throwOnFailure,
                    "Unsupported argument symbol variant. Expected one of the following variants %s but actual symbol was %s.",
                    this.allowedVariants,
                    val.get());
        }

        return Optional.of(argumentType);
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.ofGroup(symbolClass);
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
