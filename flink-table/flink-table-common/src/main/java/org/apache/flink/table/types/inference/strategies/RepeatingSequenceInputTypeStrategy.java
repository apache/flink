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
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link InputTypeStrategy} composed of an arbitrarily often repeating list of {@link
 * ArgumentTypeStrategy}s.
 */
@Internal
public class RepeatingSequenceInputTypeStrategy implements InputTypeStrategy {

    private final List<ArgumentTypeStrategy> argumentStrategies;

    public RepeatingSequenceInputTypeStrategy(List<ArgumentTypeStrategy> argumentStrategies) {
        this.argumentStrategies = argumentStrategies;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return new ArgumentCount() {
            @Override
            public boolean isValidCount(int count) {
                return count % argumentStrategies.size() == 0;
            }

            @Override
            public Optional<Integer> getMinCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Integer> getMaxCount() {
                return Optional.empty();
            }
        };
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> dataTypes = callContext.getArgumentDataTypes();
        final List<DataType> inferredDataTypes = new ArrayList<>(dataTypes.size());

        for (int i = 0; i < callContext.getArgumentDataTypes().size(); i++) {
            final ArgumentTypeStrategy argumentStrategy =
                    argumentStrategies.get(i % argumentStrategies.size());

            final Optional<DataType> inferredDataType =
                    argumentStrategy.inferArgumentType(callContext, i, throwOnFailure);
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
            final Signature.Argument argument =
                    argumentStrategies.get(i).getExpectedArgument(definition, i);

            final Signature.Argument newArgument;
            if (i == 0) {
                newArgument = Signature.Argument.of(String.format("[%s", argument.getType()));
            } else if (i == argumentStrategies.size() - 1) {
                newArgument = Signature.Argument.of(String.format("%s]...", argument.getType()));
            } else {
                newArgument = argument;
            }

            arguments.add(newArgument);
        }

        final Signature signature = Signature.of(arguments);
        return Collections.singletonList(signature);
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

        final RepeatingSequenceInputTypeStrategy that = (RepeatingSequenceInputTypeStrategy) other;
        return Objects.equals(argumentStrategies, that.argumentStrategies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentStrategies);
    }
}
