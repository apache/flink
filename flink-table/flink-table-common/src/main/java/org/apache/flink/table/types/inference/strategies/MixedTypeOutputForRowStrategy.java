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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.List;
import java.util.Optional;

/**
 * MixedTypeOutputStrategy strategy that returns a common, least restrictive type of all arguments.
 */
public class MixedTypeOutputForRowStrategy implements TypeStrategy {
    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        // Obtain the index value from the first argument.
        Optional<Integer> fieldIndex = callContext.getArgumentValue(1, Integer.class);
        Optional<DataType> result = Optional.empty();
        if (fieldIndex.isPresent()) {
            // The index starts from 1 in the subsequence, so we adjust the index value by adding 1.
            int adjustedIndex = fieldIndex.get();
            List<DataType> children = argumentDataTypes.get(0).getChildren();
            if (adjustedIndex > 0 && adjustedIndex <= children.size()) {
                result = Optional.of(children.get(adjustedIndex - 1));
            } else {
                throw new ValidationException(
                        "the input should not smaller than 1 and larger than " + children.size());
            }
        } else {
            return Optional.of(DataTypes.INT());
        }
        return result.map(
                (type) -> {
                    return argumentDataTypes.get(1).getLogicalType().isNullable()
                            ? (DataType) type.nullable()
                            : type;
                });
    }
}
