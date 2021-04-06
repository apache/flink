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

import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.FlinkTableFunction;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple3;

import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeGetResultType;
import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeSetArgs;
import static org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.buildRelDataType;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType;

/**
 * Hive {@link TableSqlFunction}. Override getFunction to clone function and invoke {@code
 * HiveGenericUDTF#setArgumentTypesAndConstants}. Override SqlReturnTypeInference to invoke {@code
 * HiveGenericUDTF#getHiveResultType} instead of {@code HiveGenericUDTF#getResultType(Object[],
 * Class[])}.
 *
 * @deprecated TODO hack code, its logical should be integrated to TableSqlFunction
 */
@Deprecated
public class HiveTableSqlFunction extends TableSqlFunction {

    private final TableFunction hiveUdtf;
    private final HiveOperandTypeChecker operandTypeChecker;

    public HiveTableSqlFunction(
            FunctionIdentifier identifier,
            TableFunction hiveUdtf,
            DataType implicitResultType,
            FlinkTypeFactory typeFactory,
            FlinkTableFunction functionImpl,
            HiveOperandTypeChecker operandTypeChecker) {
        super(
                identifier,
                identifier.toString(),
                hiveUdtf,
                implicitResultType,
                typeFactory,
                functionImpl,
                scala.Option.apply(operandTypeChecker));
        this.hiveUdtf = hiveUdtf;
        this.operandTypeChecker = operandTypeChecker;
    }

    @Override
    public TableFunction makeFunction(Object[] constantArguments, LogicalType[] argTypes) {
        TableFunction clone;
        try {
            clone = InstantiationUtil.clone(hiveUdtf);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return (TableFunction) invokeSetArgs(clone, constantArguments, argTypes);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments) {
        Preconditions.checkNotNull(operandTypeChecker.previousArgTypes);
        FlinkTypeFactory factory = (FlinkTypeFactory) typeFactory;
        Object[] argumentsArray =
                convertArguments(
                        Arrays.stream(operandTypeChecker.previousArgTypes)
                                .map(factory::createFieldTypeFromLogicalType)
                                .collect(Collectors.toList()),
                        arguments);
        DataType resultType =
                fromLogicalTypeToDataType(
                        FlinkTypeFactory.toLogicalType(
                                invokeGetResultType(
                                        hiveUdtf,
                                        argumentsArray,
                                        operandTypeChecker.previousArgTypes,
                                        (FlinkTypeFactory) typeFactory)));
        Tuple3<String[], int[], LogicalType[]> fieldInfo =
                UserDefinedFunctionUtils.getFieldInfo(resultType);
        return buildRelDataType(
                typeFactory, fromDataTypeToLogicalType(resultType), fieldInfo._1(), fieldInfo._2());
    }

    /**
     * This method is copied from calcite, and modify it to not rely on Function. TODO
     * FlinkTableFunction need implement getElementType.
     */
    private static Object[] convertArguments(
            List<RelDataType> operandTypes, List<Object> arguments0) {
        List<Object> arguments = new ArrayList<>(arguments0.size());
        // Construct a list of arguments, if they are all constants.
        for (Pair<RelDataType, Object> pair : Pair.zip(operandTypes, arguments0)) {
            arguments.add(coerce(pair.right, pair.left));
        }
        return arguments.toArray();
    }

    private static Object coerce(Object value, RelDataType type) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlTypeName()) {
            case CHAR:
                return ((NlsString) value).getValue();
            case BINARY:
                return ((BitString) value).getAsByteArray();
            case DECIMAL:
                return value;
            case BIGINT:
                return ((BigDecimal) value).longValue();
            case INTEGER:
                return ((BigDecimal) value).intValue();
            case SMALLINT:
                return ((BigDecimal) value).shortValue();
            case TINYINT:
                return ((BigDecimal) value).byteValue();
            case DOUBLE:
                return ((BigDecimal) value).doubleValue();
            case REAL:
                return ((BigDecimal) value).floatValue();
            case FLOAT:
                return ((BigDecimal) value).floatValue();
            case DATE:
                return LocalDate.ofEpochDay(((DateString) value).getDaysSinceEpoch());
            case TIME:
                return LocalTime.ofNanoOfDay(((TimeString) value).getMillisOfDay() * 1000_000);
            case TIMESTAMP:
                return SqlDateTimeUtils.unixTimestampToLocalDateTime(
                        ((TimestampString) value).getMillisSinceEpoch());
            default:
                throw new RuntimeException("Not support type: " + type);
        }
    }

    public static HiveOperandTypeChecker operandTypeChecker(String name, TableFunction udtf) {
        return new HiveOperandTypeChecker(
                name, udtf, UserDefinedFunctionUtils.checkAndExtractMethods(udtf, "eval"));
    }

    /**
     * Checker for remember previousArgTypes.
     *
     * @deprecated TODO hack code, should modify calcite getRowType to pass operand types
     */
    @Deprecated
    public static class HiveOperandTypeChecker extends OperandMetadata {

        private LogicalType[] previousArgTypes;

        private HiveOperandTypeChecker(String name, TableFunction udtf, Method[] methods) {
            super(name, udtf, methods);
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            previousArgTypes = UserDefinedFunctionUtils.getOperandTypeArray(callBinding);
            return super.checkOperandTypes(callBinding, throwOnFailure);
        }
    }
}
