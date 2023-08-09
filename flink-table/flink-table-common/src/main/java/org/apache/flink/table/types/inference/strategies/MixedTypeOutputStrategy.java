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

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.List;
import java.util.Optional;

/**
 * MixedTypeOutputStrategy strategy that returns a common, least restrictive type of all arguments.
 */
public class MixedTypeOutputStrategy implements TypeStrategy {
    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        // Obtain the index value from the first argument.
        Optional<Integer> fieldIndex = callContext.getArgumentValue(0, Integer.class);
        Optional<DataType> result = Optional.empty();
        if (fieldIndex.isPresent()) {
            // The index starts from 1 in the subsequence, so we adjust the index value by adding 1.
            int adjustedIndex = fieldIndex.get();
            // Ensure the index is within the valid range.
            if (adjustedIndex >= 0 && adjustedIndex < argumentDataTypes.size()) {
                result = Optional.of(argumentDataTypes.get(adjustedIndex));
            }
        }
        return result.map(
                (type) -> {
                    return argumentDataTypes.get(0).getLogicalType().isNullable()
                            ? (DataType) type.nullable()
                            : type;
                });
    }
}
