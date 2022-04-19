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
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;

/**
 * Type strategy for EXTRACT, checking the first value is a valid literal of type {@link
 * TimeIntervalUnit}, and that the combination of the second argument type and the interval unit is
 * correct.
 */
@Internal
class ExtractInputTypeStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(2);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> args = callContext.getArgumentDataTypes();
        final LogicalType temporalArg = args.get(1).getLogicalType();
        if (!temporalArg.isAnyOf(LogicalTypeFamily.DATETIME, LogicalTypeFamily.INTERVAL)) {
            return callContext.fail(
                    throwOnFailure,
                    "EXTRACT requires 2nd argument to be a temporal type, but type is %s",
                    temporalArg);
        }
        final Optional<TimeIntervalUnit> timeIntervalUnit =
                callContext.getArgumentValue(0, TimeIntervalUnit.class);
        if (!timeIntervalUnit.isPresent()) {
            return callContext.fail(
                    throwOnFailure,
                    "EXTRACT requires 1st argument to be a TimeIntervalUnit literal");
        }

        switch (timeIntervalUnit.get()) {
            case MILLENNIUM:
            case CENTURY:
            case DECADE:
            case YEAR:
            case QUARTER:
            case MONTH:
            case WEEK:
            case DAY:
            case EPOCH:
                return Optional.of(args);
            case HOUR:
            case MINUTE:
            case SECOND:
            case MILLISECOND:
            case MICROSECOND:
            case NANOSECOND:
                if (temporalArg.isAnyOf(LogicalTypeFamily.TIME, LogicalTypeFamily.TIMESTAMP)
                        || temporalArg.is(INTERVAL_DAY_TIME)) {
                    return Optional.of(args);
                }
        }

        return callContext.fail(
                throwOnFailure,
                "EXTRACT does not support TimeIntervalUnit %s for type %s",
                timeIntervalUnit.get(),
                temporalArg);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(
                        Argument.ofGroup(TimeIntervalUnit.class), Argument.ofGroup("TEMPORAL")));
    }
}
