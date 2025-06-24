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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.List;
import java.util.Optional;

/**
 * An {@link ArgumentTypeStrategy} that expects:
 *
 * <p>a {@link LogicalTypeFamily#NUMERIC} if the first argument is an {@link LogicalTypeRoot#ARRAY}
 * or {@link LogicalTypeRoot#MULTISET}
 *
 * <p>the type to be equal to the key type of {@link LogicalTypeRoot#MAP} if the first argument is a
 * map.
 */
@Internal
public final class ItemAtIndexArgumentTypeStrategy implements ArgumentTypeStrategy {
    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final LogicalType collectionType = argumentDataTypes.get(0).getLogicalType();
        final DataType indexType = argumentDataTypes.get(1);

        if (collectionType.is(LogicalTypeRoot.ARRAY)) {
            if (indexType.getLogicalType().is(LogicalTypeFamily.INTEGER_NUMERIC)) {

                if (callContext.isArgumentLiteral(1)) {
                    Optional<Integer> literalVal = callContext.getArgumentValue(1, Integer.class);
                    if (literalVal.isPresent() && literalVal.get() <= 0) {
                        return callContext.fail(
                                throwOnFailure,
                                "The provided index must be a valid SQL index starting from 1, but was '%s'",
                                literalVal.get());
                    }
                }

                return Optional.of(indexType);
            } else {
                return callContext.fail(
                        throwOnFailure, "Array can be indexed only using an INTEGER NUMERIC type.");
            }
        }

        if (collectionType.is(LogicalTypeRoot.MAP)) {
            MapType mapType = (MapType) collectionType;
            if (LogicalTypeCasts.supportsImplicitCast(
                    indexType.getLogicalType(), mapType.getKeyType())) {
                final KeyValueDataType mapDataType = (KeyValueDataType) argumentDataTypes.get(0);
                return Optional.of(mapDataType.getKeyDataType());
            } else {
                return callContext.fail(
                        throwOnFailure,
                        "Expected index for a MAP to be of type: %s",
                        mapType.getKeyType());
            }
        }

        return Optional.empty();
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.of("[<INTEGER NUMERIC> | <MAP_KEY_TYPE>]");
    }
}
