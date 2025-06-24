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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Type strategy that returns a {@link DataTypes#ARRAY(DataType)} with element type equal to the
 * type of the first argument if it's not nullable or element to add is not nullable, otherwise it
 * returns {@link DataTypes#ARRAY(DataType)} with type equal to the type of the element to add to
 * array.
 */
@Internal
public class ArrayAppendPrependTypeStrategy implements TypeStrategy {
    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final DataType arrayDataType = argumentDataTypes.get(0);
        final DataType elementToAddDataType = argumentDataTypes.get(1);
        final LogicalType arrayElementLogicalType =
                arrayDataType.getLogicalType().getChildren().get(0);
        if (elementToAddDataType.getLogicalType().isNullable()
                && !arrayElementLogicalType.isNullable()) {
            return Optional.of(
                    DataTypes.ARRAY(fromLogicalToDataType(arrayElementLogicalType).nullable()));
        }
        return Optional.of(arrayDataType);
    }
}
