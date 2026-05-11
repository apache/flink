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
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;

/**
 * Input type strategy for {@link BuiltInFunctionDefinitions#REGEXP_EXTRACT}. Validates literal
 * regex patterns at planning time.
 */
@Internal
public class RegexpExtractInputTypeStrategy implements InputTypeStrategy {

    private static final int ARG_STR = 0;
    private static final int ARG_REGEX = 1;
    private static final int ARG_EXTRACT_INDEX = 2;

    private static final ArgumentTypeStrategy STRING_ARG =
            logical(LogicalTypeFamily.CHARACTER_STRING);
    private static final ArgumentTypeStrategy INT_ARG = logical(LogicalTypeRoot.INTEGER);

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.between(2, 3);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            final CallContext callContext, final boolean throwOnFailure) {
        if (STRING_ARG.inferArgumentType(callContext, ARG_STR, throwOnFailure).isEmpty()) {
            return Optional.empty();
        }
        if (STRING_ARG.inferArgumentType(callContext, ARG_REGEX, throwOnFailure).isEmpty()) {
            return Optional.empty();
        }
        if (callContext.getArgumentDataTypes().size() > ARG_EXTRACT_INDEX
                && INT_ARG.inferArgumentType(callContext, ARG_EXTRACT_INDEX, throwOnFailure)
                        .isEmpty()) {
            return Optional.empty();
        }

        final Optional<List<DataType>> patternError =
                validateLiteralPattern(callContext, throwOnFailure);
        if (patternError.isPresent()) {
            return patternError;
        }

        return Optional.of(callContext.getArgumentDataTypes());
    }

    private static Optional<List<DataType>> validateLiteralPattern(
            final CallContext callContext, final boolean throwOnFailure) {
        if (!callContext.isArgumentLiteral(ARG_REGEX) || callContext.isArgumentNull(ARG_REGEX)) {
            return Optional.empty();
        }
        final Optional<String> pattern = callContext.getArgumentValue(ARG_REGEX, String.class);
        if (pattern.isEmpty()) {
            return Optional.empty();
        }
        try {
            Pattern.compile(pattern.get());
            return Optional.empty();
        } catch (PatternSyntaxException e) {
            return callContext.fail(
                    throwOnFailure,
                    "Invalid regular expression for REGEXP_EXTRACT: %s",
                    e.getMessage());
        }
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return List.of(
                Signature.of(
                        Argument.ofGroup("str", LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("regex", LogicalTypeFamily.CHARACTER_STRING)),
                Signature.of(
                        Argument.ofGroup("str", LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("regex", LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("extractIndex", LogicalTypeRoot.INTEGER)));
    }
}
