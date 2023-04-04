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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link InputTypeStrategy} specific for {@link BuiltInFunctionDefinitions#MAP_FROM_ENTRIES}.
 *
 * <p>It checks if an argument is an array type of row with two fields.
 */
@Internal
class MapFromEntriesInputTypeStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(1);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();

        final DataType dataType = argumentDataTypes.get(0);
        final LogicalType logicalType = dataType.getLogicalType();
        if (logicalType.is(LogicalTypeRoot.ARRAY)
                && ((ArrayType) logicalType).getElementType().is(LogicalTypeRoot.ROW)
                && ((ArrayType) logicalType).getElementType().getChildren().size() == 2) {
            return Optional.of(Collections.singletonList(dataType));
        } else {
            return callContext.fail(
                    throwOnFailure,
                    "Unsupported argument type. Expected type 'ARRAY<ROW<`f0` ANY, `f1` ANY>>' but actual type was '%s'.",
                    logicalType.asSummaryString());
        }
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(Signature.Argument.of("ARRAY<ROW<`f0` ANY, `f1` ANY>>")));
    }
}
