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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.hiveAggDecimalPlus;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;

/**
 * API for hive aggregation functions that are expressed in terms of expressions.
 *
 * <p>Compared to {@link DeclarativeAggregateFunction}, this API provides extra {@link
 * #setArguments} method that is used to set input arguments information before call other methods.
 * We need this API because the aggBuffer and result type of some hive aggregate functions such as
 * sum is decided by the input type, this can be achieved by implements {@link #setArguments} method
 * correctly.
 *
 * <p>Moreover, if the hive aggregate function implements this API, planner will use the hash
 * aggregate strategy instead of sort aggregation that performance will be better.
 */
@Internal
public abstract class HiveDeclarativeAggregateFunction extends DeclarativeAggregateFunction {

    protected static final int MAX_SCALE = 38;

    /**
     * Set input arguments for the function if need some inputs to infer the aggBuffer type and
     * result type. Otherwise, just implements it do nothing.
     */
    public abstract void setArguments(CallContext callContext);

    /** This method is used to infer result type when generate {@code AggregateCall} of calcite. */
    public TypeInference getTypeInference(DataTypeFactory factory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(new HiveAggregateFunctionOutputStrategy(this))
                .build();
    }

    protected void checkArgumentNum(List<DataType> arguments) {
        if (arguments.size() != 1) {
            throw new TableException("Exactly one argument is expected.");
        }
    }

    protected void checkMinMaxArgumentType(LogicalType logicalType, String functionName) {
        // Flink doesn't support to compare nested type now, so here can't support it, see
        // ScalarOperatorGens#generateComparison for more detail
        if (logicalType.is(LogicalTypeRoot.ARRAY)
                || logicalType.is(LogicalTypeRoot.MAP)
                || logicalType.is(LogicalTypeRoot.ROW)) {
            throw new TableException(
                    String.format(
                            "Native hive %s aggregate function does not support type: %s. "
                                    + "Please set option '%s' to false to fall back to Hive's own %s function.",
                            functionName,
                            logicalType.getTypeRoot(),
                            TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED.key(),
                            functionName));
        }
    }

    protected UnresolvedCallExpression adjustedPlus(
            DataType dataType, Expression arg1, Expression arg2) {
        if (dataType.getLogicalType().is(DECIMAL)) {
            return hiveAggDecimalPlus(arg1, arg2);
        } else {
            return plus(arg1, arg2);
        }
    }

    @Override
    public FunctionKind getKind() {
        return FunctionKind.AGGREGATE;
    }

    /** OutputTypeStrategy for native hive aggregate function. */
    class HiveAggregateFunctionOutputStrategy implements TypeStrategy {

        private final HiveDeclarativeAggregateFunction declarativeAggregateFunction;

        public HiveAggregateFunctionOutputStrategy(
                HiveDeclarativeAggregateFunction declarativeAggregateFunction) {
            this.declarativeAggregateFunction = declarativeAggregateFunction;
        }

        @Override
        public Optional<DataType> inferType(CallContext callContext) {
            // set the input arguments first if you need some info to infer the aggBuffer/return
            // type
            declarativeAggregateFunction.setArguments(callContext);
            return Optional.of(declarativeAggregateFunction.getResultType());
        }
    }
}
