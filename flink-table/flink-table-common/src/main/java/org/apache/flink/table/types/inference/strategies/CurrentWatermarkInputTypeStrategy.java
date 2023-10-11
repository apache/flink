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
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link InputTypeStrategy} specific for {@link BuiltInFunctionDefinitions#CURRENT_WATERMARK}.
 *
 * <p>It expects a single argument representing a rowtime attribute.
 */
@Internal
class CurrentWatermarkInputTypeStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(1);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();

        if (argumentDataTypes == null || argumentDataTypes.isEmpty()) {
            return Optional.of(Collections.emptyList());
        }

        final DataType dataType = argumentDataTypes.get(0);
        if (!LogicalTypeChecks.canBeTimeAttributeType(dataType.getLogicalType())) {
            return callContext.fail(
                    throwOnFailure,
                    "CURRENT_WATERMARK() must be called with a single rowtime attribute argument, but '%s' cannot be a time attribute.",
                    dataType.getLogicalType().asSummaryString());
        }

        if (!LogicalTypeChecks.isRowtimeAttribute(dataType.getLogicalType())) {
            return callContext.fail(
                    throwOnFailure,
                    "The argument of CURRENT_WATERMARK() must be a rowtime attribute, but was '%s'.",
                    dataType.getLogicalType().asSummaryString());
        }

        return Optional.of(Collections.singletonList(dataType));
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        final List<Signature> signatures = new ArrayList<>();
        signatures.add(createExpectedSignature(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE));
        signatures.add(createExpectedSignature(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
        return signatures;
    }

    private static Signature createExpectedSignature(LogicalTypeRoot typeRoot) {
        final String argument = String.format("<%s *%s*>", typeRoot, TimestampKind.ROWTIME);
        return Signature.of(Signature.Argument.of(argument));
    }
}
