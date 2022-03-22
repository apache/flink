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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Input type strategy for {@code AT}. */
@Internal
public class AtInputTypeStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(2);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        List<DataType> args = callContext.getArgumentDataTypes();
        LogicalType collection = args.get(0).getLogicalType();

        if (collection.is(LogicalTypeRoot.MAP)) {
            LogicalType key = args.get(1).getLogicalType();
            if (!LogicalTypeCasts.supportsAvoidingCast(key, collection.getChildren().get(0))) {
                return callContext.fail(
                        throwOnFailure,
                        "The provided key type '%s' is not compatible with provided map key type '%s'",
                        key,
                        collection);
            }

            return Optional.of(Arrays.asList(args.get(0), args.get(0).getChildren().get(0)));
        }

        if (collection.is(LogicalTypeRoot.ARRAY)) {
            LogicalType index = args.get(1).getLogicalType();
            DataType expectedIndexType = DataTypes.INT();
            if (!LogicalTypeCasts.supportsAvoidingCast(index, expectedIndexType.getLogicalType())) {
                return callContext.fail(
                        throwOnFailure,
                        "The provided index type '%s' is not compatible with INT",
                        index);
            }
            if (index.isNullable()) {
                return callContext.fail(
                        throwOnFailure, "The provided index type '%s' is nullable", index);
            }

            // Validate if literal
            if (callContext.isArgumentLiteral(1)) {
                Optional<Integer> literalVal = callContext.getArgumentValue(1, Integer.class);
                if (literalVal.isPresent() && literalVal.get() <= 0) {
                    return callContext.fail(
                            throwOnFailure,
                            "The provided index must be a valid SQL index starting from 1, but was '%s'",
                            literalVal.get());
                }
            }

            return Optional.of(Arrays.asList(args.get(0), expectedIndexType));
        }

        return callContext.fail(throwOnFailure, "Unexpected first argument type '%ss'", collection);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Arrays.asList(
                Signature.of(
                        Argument.ofGroup(LogicalTypeRoot.ARRAY), Argument.of(new IntType(false))),
                Signature.of(
                        Argument.ofGroup(LogicalTypeRoot.MAP), Argument.ofGroup("MAP KEY TYPE")));
    }
}
