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
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.inference.strategies.StrategyUtils.isDecimalComputation;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Type strategy that returns the quotient of an exact numeric division that includes at least one
 * decimal.
 */
@Internal
class DecimalDivideTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final LogicalType dividend = argumentDataTypes.get(0).getLogicalType();
        final LogicalType divisor = argumentDataTypes.get(1).getLogicalType();

        // a hack to make legacy types possible until we drop them
        if (dividend instanceof LegacyTypeInformationType) {
            return Optional.of(argumentDataTypes.get(0));
        }

        if (divisor instanceof LegacyTypeInformationType) {
            return Optional.of(argumentDataTypes.get(1));
        }

        if (!isDecimalComputation(dividend, divisor)) {
            return Optional.empty();
        }

        final DecimalType decimalType =
                LogicalTypeMerging.findDivisionDecimalType(
                        getPrecision(dividend),
                        getScale(dividend),
                        getPrecision(divisor),
                        getScale(divisor));

        return Optional.of(fromLogicalToDataType(decimalType));
    }
}
