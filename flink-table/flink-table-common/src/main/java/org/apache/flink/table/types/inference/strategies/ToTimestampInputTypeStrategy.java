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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;

/**
 * Input type strategy for {@link BuiltInFunctionDefinitions#TO_TIMESTAMP} that validates the format
 * pattern at compile time when provided as a literal.
 */
@Internal
public class ToTimestampInputTypeStrategy implements InputTypeStrategy {

    private static final ArgumentTypeStrategy CHARACTER_STRING_FAMILY_ARG =
            logical(LogicalTypeFamily.CHARACTER_STRING);

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.between(1, 2);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> result = new ArrayList<>();
        final int numberOfArguments = callContext.getArgumentDataTypes().size();

        final Optional<DataType> timestampArg =
                CHARACTER_STRING_FAMILY_ARG.inferArgumentType(callContext, 0, throwOnFailure);
        if (timestampArg.isEmpty()) {
            return Optional.empty();
        }
        result.add(timestampArg.get());

        if (numberOfArguments > 1) {
            final Optional<DataType> patternArg =
                    validatePatternArgument(callContext, throwOnFailure);
            if (patternArg.isEmpty()) {
                return Optional.empty();
            }
            result.add(patternArg.get());
        }

        return Optional.of(result);
    }

    private Optional<DataType> validatePatternArgument(
            final CallContext callContext, final boolean throwOnFailure) {
        final Optional<DataType> patternArg =
                CHARACTER_STRING_FAMILY_ARG.inferArgumentType(callContext, 1, throwOnFailure);
        if (patternArg.isEmpty()) {
            return Optional.empty();
        }

        if (callContext.isArgumentLiteral(1)) {
            final Optional<String> patternOpt = callContext.getArgumentValue(1, String.class);
            if (patternOpt.isEmpty()) {
                return callContext.fail(throwOnFailure, "Pattern can not be a null literal");
            }
            try {
                DateTimeFormatter.ofPattern(patternOpt.get());
            } catch (IllegalArgumentException e) {
                return callContext.fail(
                        throwOnFailure,
                        "Invalid pattern for parsing TIMESTAMP: %s",
                        e.getMessage());
            }
        }

        return patternArg;
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return List.of(
                Signature.of(Argument.ofGroup(LogicalTypeFamily.CHARACTER_STRING)),
                Signature.of(
                        Argument.ofGroup(LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("pattern", LogicalTypeFamily.CHARACTER_STRING)));
    }
}
