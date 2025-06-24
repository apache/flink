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
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Type strategy that returns the result type of a decimal addition, used internally for
 * implementing native SUM/AVG aggregations on a Decimal type. Here is used to prevent the normal
 * {@link DecimalPlusTypeStrategy} from overriding the special calculation for precision and scale
 * needed by the aggregate function. {@link LogicalTypeMerging#findAdditionDecimalType} will adjust
 * the precision according to the two input arguments, but for hive we just keep the precision as
 * input type because of the input type precision is the same.
 */
@Internal
public class HiveAggDecimalPlusTypeStrategy implements TypeStrategy {

    private static final String ERROR_MSG =
            "Both args of "
                    + HiveAggDecimalPlusTypeStrategy.class.getSimpleName()
                    + " should be of type["
                    + DecimalType.class.getSimpleName()
                    + "]";

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final LogicalType addend1 = argumentDataTypes.get(0).getLogicalType();
        final LogicalType addend2 = argumentDataTypes.get(1).getLogicalType();

        Preconditions.checkArgument(addend1.is(LogicalTypeRoot.DECIMAL), ERROR_MSG);
        Preconditions.checkArgument(addend2.is(LogicalTypeRoot.DECIMAL), ERROR_MSG);

        return Optional.of(fromLogicalToDataType(addend2));
    }
}
