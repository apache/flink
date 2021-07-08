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

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.io.IOException;
import java.util.List;

import scala.Some;

import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeGetResultType;
import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeSetArgs;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Hive {@link AggSqlFunction}. Override getFunction to clone function and invoke {@code
 * HiveUDAF#setArgumentTypesAndConstants}. Override SqlReturnTypeInference to invoke {@code
 * HiveUDAF#getHiveResultType} instead of {@code HiveUDAF#getResultType}.
 *
 * @deprecated TODO hack code, its logical should be integrated to AggSqlFunction
 */
@Deprecated
public class HiveAggSqlFunction extends AggSqlFunction {

    private final AggregateFunction aggregateFunction;

    public HiveAggSqlFunction(
            FunctionIdentifier identifier,
            AggregateFunction aggregateFunction,
            FlinkTypeFactory typeFactory) {
        super(
                identifier,
                identifier.toString(),
                aggregateFunction,
                fromLegacyInfoToDataType(new GenericTypeInfo<>(Object.class)),
                fromLegacyInfoToDataType(new GenericTypeInfo<>(Object.class)),
                typeFactory,
                false,
                new Some<>(createReturnTypeInference(aggregateFunction, typeFactory)));
        this.aggregateFunction = aggregateFunction;
    }

    @Override
    public AggregateFunction makeFunction(Object[] constantArguments, LogicalType[] argTypes) {
        AggregateFunction clone;
        try {
            clone = InstantiationUtil.clone(aggregateFunction);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return (AggregateFunction) invokeSetArgs(clone, constantArguments, argTypes);
    }

    private static SqlReturnTypeInference createReturnTypeInference(
            AggregateFunction function, FlinkTypeFactory typeFactory) {
        return opBinding -> {
            List<RelDataType> sqlTypes = opBinding.collectOperandTypes();
            LogicalType[] parameters = UserDefinedFunctionUtils.getOperandTypeArray(opBinding);

            Object[] constantArguments = new Object[sqlTypes.size()];
            // Can not touch the literals, Calcite make them in previous RelNode.
            // In here, all inputs are input refs.
            return invokeGetResultType(function, constantArguments, parameters, typeFactory);
        };
    }
}
