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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasScale;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** Strategy that returns a decimal type but with a scale of 0. */
@Internal
class DecimalScale0TypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final DataType argumentDataType = callContext.getArgumentDataTypes().get(0);
        final LogicalType argumentType = argumentDataType.getLogicalType();

        // a hack to make legacy types possible until we drop them
        if (argumentType instanceof LegacyTypeInformationType) {
            return Optional.of(argumentDataType);
        }

        if (hasRoot(argumentType, LogicalTypeRoot.DECIMAL)) {
            if (hasScale(argumentType, 0)) {
                return Optional.of(argumentDataType);
            }
            final LogicalType inferredType =
                    new DecimalType(argumentType.isNullable(), getPrecision(argumentType), 0);
            return Optional.of(fromLogicalToDataType(inferredType));
        }

        return Optional.empty();
    }
}
