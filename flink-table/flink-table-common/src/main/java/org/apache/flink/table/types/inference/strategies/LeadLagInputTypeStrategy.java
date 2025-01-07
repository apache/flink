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
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Type strategy for {@link BuiltInFunctionDefinitions#LAG} and { @link
 * BuiltInFunctionDefinitions#LEAD}.
 *
 * <p>The second argument needs to be NUMERIC if provided. Third argument must have a common type
 * with the first.
 */
@Internal
public final class LeadLagInputTypeStrategy implements InputTypeStrategy {

    private static final ArgumentTypeStrategy NUMERIC_ARGUMENT =
            InputTypeStrategies.logical(LogicalTypeFamily.NUMERIC);

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.between(1, 3);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();

        if (argumentDataTypes.size() == 1) {
            return Optional.of(argumentDataTypes);
        }

        Optional<DataType> offsetArg =
                NUMERIC_ARGUMENT.inferArgumentType(callContext, 1, throwOnFailure);

        if (!offsetArg.isPresent()) {
            return Optional.empty();
        }

        final DataType arg0 = argumentDataTypes.get(0);
        if (argumentDataTypes.size() == 2) {
            return Optional.of(Arrays.asList(arg0, offsetArg.get()));
        } else {
            LogicalType defaultType = argumentDataTypes.get(2).getLogicalType();
            Optional<LogicalType> commonType =
                    LogicalTypeMerging.findCommonType(
                            Arrays.asList(arg0.getLogicalType(), defaultType));

            if (!commonType.isPresent()) {
                return callContext.fail(
                        throwOnFailure,
                        "The default value must have a common "
                                + "type with the given expression. ARG0: %s, default: %s",
                        arg0,
                        defaultType);
            }

            DataType commonDataType = commonType.map(TypeConversions::fromLogicalToDataType).get();
            return Optional.of(Arrays.asList(commonDataType, offsetArg.get(), commonDataType));
        }
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Arrays.asList(
                Signature.of(Argument.ofGroup("ANY")),
                Signature.of(Argument.ofGroup("ANY"), Argument.ofGroup("NUMERIC")),
                Signature.of(
                        Argument.ofGroup("COMMON"),
                        Argument.ofGroup("NUMERIC"),
                        Argument.ofGroup("COMMON")));
    }
}
