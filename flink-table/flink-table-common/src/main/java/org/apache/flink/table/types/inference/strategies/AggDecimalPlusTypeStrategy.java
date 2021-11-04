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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Type strategy that returns the result decimal addition used, internally by {@code SumAggFunction}
 * to implement SUM aggregation on a Decimal type. Uses the {@link
 * LogicalTypeMerging#findSumAggType(LogicalType)} and prevents the {@link DecimalPlusTypeStrategy}
 * from overriding the special calculation for precision and scale needed by SUM.
 */
@Internal
class AggDecimalPlusTypeStrategy implements TypeStrategy {

    private static final String ERROR_MSG =
            "Both args of "
                    + AggDecimalPlusTypeStrategy.class.getSimpleName()
                    + " should be of type["
                    + DecimalType.class.getSimpleName()
                    + "]";

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final LogicalType addend1 = argumentDataTypes.get(0).getLogicalType();
        final LogicalType addend2 = argumentDataTypes.get(1).getLogicalType();

        Preconditions.checkArgument(
                LogicalTypeChecks.hasRoot(addend1, LogicalTypeRoot.DECIMAL), ERROR_MSG);
        Preconditions.checkArgument(
                LogicalTypeChecks.hasRoot(addend2, LogicalTypeRoot.DECIMAL), ERROR_MSG);

        return Optional.of(
                TypeConversions.fromLogicalToDataType(LogicalTypeMerging.findSumAggType(addend2)));
    }
}
