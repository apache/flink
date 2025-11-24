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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;

/**
 * A strategy for {@link BuiltInFunctionDefinitions#TO_TIMESTAMP_LTZ} where the first argument is a
 * string that needs to be parsed.
 */
@Internal
public class ToTimestampLtzInputTypeStrategy implements InputTypeStrategy {

    private static final ArgumentTypeStrategy CHARACTER_STRING_FAMILY_ARG =
            logical(LogicalTypeFamily.CHARACTER_STRING);

    private static final InputTypeStrategy NUMERIC_TO_TIMESTAMP_LTZ_STRATEGY =
            or(
                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                    sequence(
                            logical(LogicalTypeFamily.NUMERIC),
                            logical(LogicalTypeFamily.INTEGER_NUMERIC)));

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.between(1, 3);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> result = new ArrayList<>();
        final int numberOfArguments = callContext.getArgumentDataTypes().size();
        final Optional<DataType> value =
                CHARACTER_STRING_FAMILY_ARG.inferArgumentType(
                        callContext,
                        0,
                        false); // do not throw, so that we can check for the numeric args
        if (value.isEmpty()) {
            return NUMERIC_TO_TIMESTAMP_LTZ_STRATEGY.inferInputTypes(callContext, false);
        }
        result.add(value.get());

        if (numberOfArguments > 1) {
            final Optional<DataType> patternArg =
                    validatePatternArgument(callContext, throwOnFailure);
            if (patternArg.isEmpty()) {
                return Optional.empty();
            } else {
                result.add(patternArg.get());
            }
        }

        if (numberOfArguments > 2) {
            final Optional<DataType> timezoneArg =
                    validateTimezoneArgument(callContext, throwOnFailure);
            if (timezoneArg.isEmpty()) {
                return Optional.empty();
            } else {
                result.add(timezoneArg.get());
            }
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
            final Optional<String> errorMsg = isPatternInvalid(patternOpt.get());
            if (errorMsg.isPresent()) {
                return callContext.fail(
                        throwOnFailure,
                        "Invalid pattern for parsing TIMESTAMP_LTZ: %s",
                        errorMsg.get());
            }
        }

        return patternArg;
    }

    private Optional<DataType> validateTimezoneArgument(
            final CallContext callContext, final boolean throwOnFailure) {
        final Optional<DataType> timezoneArg =
                CHARACTER_STRING_FAMILY_ARG.inferArgumentType(callContext, 2, throwOnFailure);
        if (timezoneArg.isEmpty()) {
            return Optional.empty();
        }

        if (callContext.isArgumentLiteral(2)) {
            final Optional<String> timezoneOpt = callContext.getArgumentValue(2, String.class);
            if (timezoneOpt.isEmpty()) {
                return callContext.fail(throwOnFailure, "Timezone can not be a null literal");
            }
            final Optional<String> errorMsg = isTimezoneInvalid(timezoneOpt.get());
            if (errorMsg.isPresent()) {
                return callContext.fail(
                        throwOnFailure,
                        "Invalid timezone for parsing TIMESTAMP_LTZ: %s",
                        errorMsg.get());
            }
        }

        return timezoneArg;
    }

    private Optional<String> isTimezoneInvalid(String timezone) {
        try {
            ZoneId.of(timezone);
        } catch (DateTimeException e) {
            return Optional.of(e.getMessage());
        }

        return Optional.empty();
    }

    private Optional<String> isPatternInvalid(String pattern) {
        try {
            DateTimeFormatter.ofPattern(pattern);
        } catch (IllegalArgumentException e) {
            return Optional.of(e.getMessage());
        }

        return Optional.empty();
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return List.of(
                Signature.of(Argument.ofGroup(LogicalTypeFamily.CHARACTER_STRING)),
                Signature.of(
                        Argument.ofGroup(LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("pattern", LogicalTypeFamily.CHARACTER_STRING)),
                Signature.of(
                        Argument.ofGroup(LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("pattern", LogicalTypeFamily.CHARACTER_STRING),
                        Argument.ofGroup("timezone", LogicalTypeFamily.CHARACTER_STRING)),
                Signature.of(Argument.ofGroup(LogicalTypeFamily.NUMERIC)),
                Signature.of(
                        Argument.ofGroup(LogicalTypeFamily.NUMERIC),
                        Argument.ofGroup(LogicalTypeFamily.INTEGER_NUMERIC)));
    }
}
