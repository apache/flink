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
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Strategy for inferring and validating a function signature like {@code f(STRING, NUMERIC)} or
 * {@code f(s STRING, n NUMERIC)} using a sequence of {@link ArgumentTypeStrategy}s.
 */
@Internal
public final class SequenceInputTypeStrategy implements InputTypeStrategy {

    private final List<? extends ArgumentTypeStrategy> argumentStrategies;

    private final @Nullable List<String> argumentNames;

    public SequenceInputTypeStrategy(
            List<? extends ArgumentTypeStrategy> argumentStrategies,
            @Nullable List<String> argumentNames) {
        Preconditions.checkArgument(
                argumentNames == null || argumentNames.size() == argumentStrategies.size());
        this.argumentStrategies = argumentStrategies;
        this.argumentNames = argumentNames;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(argumentStrategies.size());
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> dataTypes = callContext.getArgumentDataTypes();
        if (dataTypes.size() != argumentStrategies.size()) {
            return Optional.empty();
        }
        final List<DataType> inferredDataTypes = new ArrayList<>(dataTypes.size());
        for (int i = 0; i < argumentStrategies.size(); i++) {
            final ArgumentTypeStrategy argumentTypeStrategy = argumentStrategies.get(i);
            final Optional<DataType> inferredDataType =
                    argumentTypeStrategy.inferArgumentType(callContext, i, throwOnFailure);
            if (!inferredDataType.isPresent()) {
                return Optional.empty();
            }
            inferredDataTypes.add(inferredDataType.get());
        }
        return Optional.of(inferredDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        final List<Signature.Argument> arguments = new ArrayList<>();
        for (int i = 0; i < argumentStrategies.size(); i++) {
            if (argumentNames == null) {
                arguments.add(argumentStrategies.get(i).getExpectedArgument(definition, i));
            } else {
                arguments.add(
                        Signature.Argument.of(
                                argumentNames.get(i),
                                argumentStrategies
                                        .get(i)
                                        .getExpectedArgument(definition, i)
                                        .getType()));
            }
        }

        return Collections.singletonList(Signature.of(arguments));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SequenceInputTypeStrategy that = (SequenceInputTypeStrategy) o;
        return Objects.equals(argumentStrategies, that.argumentStrategies)
                && Objects.equals(argumentNames, that.argumentNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentStrategies, argumentNames);
    }
}
