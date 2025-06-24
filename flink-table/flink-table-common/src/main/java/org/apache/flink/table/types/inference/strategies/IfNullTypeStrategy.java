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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Type strategy specific for avoiding nulls. <br>
 * If arg0 is non-nullable, output datatype is exactly the datatype of arg0. Otherwise, output
 * datatype is the common type of arg0 and arg1. In the second case, output type is nullable only if
 * both args are nullable.
 */
@Internal
class IfNullTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final DataType inputDataType = argumentDataTypes.get(0);
        final DataType nullReplacementDataType = argumentDataTypes.get(1);

        if (!inputDataType.getLogicalType().isNullable()) {
            return Optional.of(inputDataType);
        }

        return LogicalTypeMerging.findCommonType(
                        Arrays.asList(
                                inputDataType.getLogicalType(),
                                nullReplacementDataType.getLogicalType()))
                .map(t -> t.copy(nullReplacementDataType.getLogicalType().isNullable()))
                .map(TypeConversions::fromLogicalToDataType);
    }
}
