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
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast;

/**
 * {@link InputTypeStrategy} specific for {@link BuiltInFunctionDefinitions#CAST}.
 *
 * <p>It expects two arguments where the type of first one must be castable to the type of the
 * second one. The second one must be a type literal.
 */
@Internal
class CastInputTypeStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(2);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        // check for type literal
        if (!callContext.isArgumentLiteral(1)
                || !callContext.getArgumentValue(1, DataType.class).isPresent()) {
            return Optional.empty();
        }

        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final LogicalType fromType = argumentDataTypes.get(0).getLogicalType();
        final LogicalType toType = argumentDataTypes.get(1).getLogicalType();

        // A hack to support legacy types. To be removed when we drop the legacy types.
        if (fromType instanceof LegacyTypeInformationType) {
            return Optional.of(argumentDataTypes);
        }
        if (!supportsExplicitCast(fromType, toType)) {
            if (throwOnFailure) {
                throw callContext.newValidationError(
                        "Unsupported cast from '%s' to '%s'.", fromType, toType);
            }
            return Optional.empty();
        }
        return Optional.of(argumentDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(
                        Signature.Argument.of("<ANY>"), Signature.Argument.of("<TYPE LITERAL>")));
    }
}
