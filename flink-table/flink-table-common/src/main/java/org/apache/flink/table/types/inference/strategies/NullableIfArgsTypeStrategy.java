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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * A type strategy that can be used to make a result type nullable if any of the selected input
 * arguments is nullable. Otherwise the type will be not null.
 */
@Internal
public final class NullableIfArgsTypeStrategy implements TypeStrategy {

    private final ConstantArgumentCount includedArguments;

    private final TypeStrategy initialStrategy;

    public NullableIfArgsTypeStrategy(
            ConstantArgumentCount includedArguments, TypeStrategy initialStrategy) {
        this.includedArguments = Preconditions.checkNotNull(includedArguments);
        this.initialStrategy = Preconditions.checkNotNull(initialStrategy);
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        return initialStrategy
                .inferType(callContext)
                .map(
                        inferredDataType -> {
                            final List<DataType> argumentDataTypes =
                                    callContext.getArgumentDataTypes();

                            if (argumentDataTypes.isEmpty()) {
                                return inferredDataType.notNull();
                            }

                            final int fromArg = includedArguments.getMinCount().orElse(0);

                            final int toArg =
                                    Math.min(
                                            includedArguments
                                                    .getMaxCount()
                                                    .map(c -> c + 1)
                                                    .orElse(argumentDataTypes.size()),
                                            argumentDataTypes.size());

                            final boolean isNullableArgument =
                                    IntStream.range(fromArg, toArg)
                                            .mapToObj(argumentDataTypes::get)
                                            .anyMatch(
                                                    dataType ->
                                                            dataType.getLogicalType().isNullable());

                            if (isNullableArgument) {
                                return inferredDataType.nullable();
                            } else {
                                return inferredDataType.notNull();
                            }
                        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NullableIfArgsTypeStrategy that = (NullableIfArgsTypeStrategy) o;
        return includedArguments.equals(that.includedArguments)
                && initialStrategy.equals(that.initialStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includedArguments, initialStrategy);
    }
}
