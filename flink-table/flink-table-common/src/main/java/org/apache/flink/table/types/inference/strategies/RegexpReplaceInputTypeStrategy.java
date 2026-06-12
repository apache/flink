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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;

/**
 * Input type strategy for {@link BuiltInFunctionDefinitions#REGEXP_REPLACE}. Validates literal
 * regex patterns at planning time.
 */
@Internal
public class RegexpReplaceInputTypeStrategy implements InputTypeStrategy {

    private static final int ARG_STR = 0;
    private static final int ARG_REGEX = 1;
    private static final int ARG_REPLACEMENT = 2;

    private static final ArgumentTypeStrategy STRING_ARG =
            logical(LogicalTypeFamily.CHARACTER_STRING);

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(3);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            final CallContext callContext, final boolean throwOnFailure) {
        final Optional<DataType> inferredStrType =
                STRING_ARG.inferArgumentType(callContext, ARG_STR, throwOnFailure);
        if (inferredStrType.isEmpty()) {
            return Optional.empty();
        }
        final Optional<DataType> inferredRegexType =
                STRING_ARG.inferArgumentType(callContext, ARG_REGEX, throwOnFailure);
        if (inferredRegexType.isEmpty()) {
            return Optional.empty();
        }
        final Optional<DataType> inferredReplacementType =
                STRING_ARG.inferArgumentType(callContext, ARG_REPLACEMENT, throwOnFailure);
        if (inferredReplacementType.isEmpty()) {
            return Optional.empty();
        }

        final Optional<List<DataType>> patternError =
                StrategyUtils.validateLiteralPattern(callContext, ARG_REGEX, throwOnFailure);
        if (patternError.isPresent()) {
            return patternError;
        }

        final List<DataType> inferredDataTypes = new ArrayList<>(3);
        inferredDataTypes.add(inferredStrType.get());
        inferredDataTypes.add(inferredRegexType.get());
        inferredDataTypes.add(inferredReplacementType.get());
        return Optional.of(inferredDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return List.of(
                Signature.of(
                        Argument.ofGroup("str", LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("regex", LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("replacement", LogicalTypeFamily.CHARACTER_STRING)));
    }
}
