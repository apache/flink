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
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.ArrayType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Specific {@link ArgumentTypeStrategy} for {@link BuiltInFunctionDefinitions#ARRAY_MIN}. */
@Internal
public class ComparableArrayInputTypeStrategy implements InputTypeStrategy {
    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(1);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        DataType inputDataType = callContext.getArgumentDataTypes().get(0);
        if (!(inputDataType.getLogicalType() instanceof ArrayType)) {
            return Optional.empty();
        }
        DataType elementDataType = ((CollectionDataType) inputDataType).getElementDataType();
        if (!Comparable.class.isAssignableFrom(elementDataType.getConversionClass())) {
            return Optional.empty();
        }
        return Optional.of(callContext.getArgumentDataTypes());
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(Signature.Argument.of("<ARRAY<COMPARABLE>>")));
    }
}
