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
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Optional;

/**
 * An output type strategy for {@link BuiltInFunctionDefinitions#AT}.
 *
 * <p>Returns either the element of an {@link LogicalTypeFamily#COLLECTION} type or the value of
 * {@link LogicalTypeRoot#MAP}.
 */
@Internal
public final class ItemAtTypeStrategy implements TypeStrategy {
    @Override
    public Optional<DataType> inferType(CallContext callContext) {

        DataType arrayOrMapType = callContext.getArgumentDataTypes().get(0);
        final Optional<DataType> legacyArrayElement =
                StrategyUtils.extractLegacyArrayElement(arrayOrMapType);

        if (legacyArrayElement.isPresent()) {
            return legacyArrayElement;
        }

        if (arrayOrMapType.getLogicalType().is(LogicalTypeRoot.ARRAY)) {
            return Optional.of(
                    ((CollectionDataType) arrayOrMapType).getElementDataType().nullable());
        } else if (arrayOrMapType instanceof KeyValueDataType) {
            return Optional.of(((KeyValueDataType) arrayOrMapType).getValueDataType().nullable());
        }

        return Optional.empty();
    }
}
