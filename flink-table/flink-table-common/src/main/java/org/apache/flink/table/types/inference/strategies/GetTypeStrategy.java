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
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.List;
import java.util.Optional;

/**
 * Type strategy that returns a type of a field nested inside a composite type that is described by
 * the second argument. The second argument must be a literal that describes either the nested field
 * name or index.
 */
@Internal
class GetTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        DataType rowDataType = argumentDataTypes.get(0);

        Optional<DataType> result = Optional.empty();

        Optional<String> fieldName = callContext.getArgumentValue(1, String.class);
        if (fieldName.isPresent()) {
            result = DataTypeUtils.getField(rowDataType, fieldName.get());
        }

        Optional<Integer> fieldIndex = callContext.getArgumentValue(1, Integer.class);
        if (fieldIndex.isPresent()) {
            result = DataTypeUtils.getField(rowDataType, fieldIndex.get());
        }

        return result.map(
                type -> {
                    if (rowDataType.getLogicalType().isNullable()) {
                        return type.nullable();
                    } else {
                        return type;
                    }
                });
    }
}
