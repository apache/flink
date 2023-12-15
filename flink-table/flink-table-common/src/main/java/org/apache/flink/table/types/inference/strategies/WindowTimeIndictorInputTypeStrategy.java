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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * An {@link InputTypeStrategy} for {@link BuiltInFunctionDefinitions#ROWTIME} and {@link
 * BuiltInFunctionDefinitions#PROCTIME}.
 */
@Internal
public final class WindowTimeIndictorInputTypeStrategy implements InputTypeStrategy {
    private final TimestampKind timestampKind;

    /** @param timestampKind if null the window can be either proctime or rowtime window. */
    public WindowTimeIndictorInputTypeStrategy(@Nullable TimestampKind timestampKind) {
        this.timestampKind = timestampKind;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(1);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final LogicalType type = callContext.getArgumentDataTypes().get(0).getLogicalType();

        if (timestampKind == TimestampKind.PROCTIME && !LogicalTypeChecks.isTimeAttribute(type)) {
            return callContext.fail(
                    throwOnFailure, "Reference to a rowtime or proctime window required.");
        }

        if (timestampKind == TimestampKind.ROWTIME && LogicalTypeChecks.isProctimeAttribute(type)) {
            return callContext.fail(
                    throwOnFailure, "A proctime window cannot provide a rowtime attribute.");
        }

        if (!LogicalTypeChecks.canBeTimeAttributeType(type) && !type.is(LogicalTypeRoot.BIGINT)) {
            return callContext.fail(
                    throwOnFailure, "Reference to a rowtime or proctime window required.");
        }

        return Optional.of(callContext.getArgumentDataTypes());
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(Signature.Argument.ofGroup("WINDOW REFERENCE")));
    }
}
