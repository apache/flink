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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.util.function.QuadFunction;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import javax.annotation.Nullable;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/** Custom type system for Flink. */
@Internal
public class FlinkTypeSystem extends RelDataTypeSystemImpl {

    public static final FlinkTypeSystem INSTANCE = new FlinkTypeSystem();
    public static final DecimalType DECIMAL_SYSTEM_DEFAULT =
            new DecimalType(DecimalType.MAX_PRECISION, 18);

    private FlinkTypeSystem() {}

    @Override
    public int getMaxNumericPrecision() {
        // set the maximum precision of a NUMERIC or DECIMAL type to DecimalType.MAX_PRECISION.
        return DecimalType.MAX_PRECISION;
    }

    @Override
    public int getMaxNumericScale() {
        // the max scale can't be greater than precision
        return DecimalType.MAX_PRECISION;
    }

    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
        switch (typeName) {
            case VARCHAR:
            case VARBINARY:
                // Calcite will limit the length of the VARCHAR field to 65536
                return Integer.MAX_VALUE;
            case TIMESTAMP:
                // by default we support timestamp with microseconds precision (Timestamp(6))
                return TimestampType.DEFAULT_PRECISION;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // by default we support timestamp with local time zone with microseconds precision
                // Timestamp(6) with local time zone
                return LocalZonedTimestampType.DEFAULT_PRECISION;
        }
        return super.getDefaultPrecision(typeName);
    }

    @Override
    public int getMaxPrecision(SqlTypeName typeName) {
        switch (typeName) {
            case VARCHAR:
            case CHAR:
            case VARBINARY:
            case BINARY:
                return Integer.MAX_VALUE;

            case TIMESTAMP:
                // The maximum precision of TIMESTAMP is 3 in Calcite,
                // change it to 9 to support nanoseconds precision
                return TimestampType.MAX_PRECISION;

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // The maximum precision of TIMESTAMP_WITH_LOCAL_TIME_ZONE is 3 in Calcite,
                // change it to 9 to support nanoseconds precision
                return LocalZonedTimestampType.MAX_PRECISION;
        }
        return super.getMaxPrecision(typeName);
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        // when union a number of CHAR types of different lengths, we should cast to a VARCHAR
        // this fixes the problem of CASE WHEN with different length string literals but get wrong
        // result with additional space suffix
        return true;
    }

    @Override
    public RelDataType deriveAvgAggType(
            RelDataTypeFactory typeFactory, RelDataType argRelDataType) {
        LogicalType argType = FlinkTypeFactory.toLogicalType(argRelDataType);
        LogicalType resultType = LogicalTypeMerging.findAvgAggType(argType);
        return unwrapTypeFactory(typeFactory).createFieldTypeFromLogicalType(resultType);
    }

    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argRelDataType) {
        LogicalType argType = FlinkTypeFactory.toLogicalType(argRelDataType);
        LogicalType resultType = LogicalTypeMerging.findSumAggType(argType);
        return unwrapTypeFactory(typeFactory).createFieldTypeFromLogicalType(resultType);
    }

    @Override
    public RelDataType deriveDecimalPlusType(
            RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
        return deriveDecimalType(
                typeFactory, type1, type2, LogicalTypeMerging::findAdditionDecimalType);
    }

    @Override
    public RelDataType deriveDecimalModType(
            RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
        return deriveDecimalRelDataType(
                typeFactory,
                type1,
                type2,
                (p1, s1, p2, s2) -> {
                    if (s1 == 0 && s2 == 0) {
                        return type2;
                    }
                    DecimalType result = LogicalTypeMerging.findModuloDecimalType(p1, s1, p2, s2);
                    return typeFactory.createSqlType(
                            DECIMAL, result.getPrecision(), result.getScale());
                });
    }

    @Override
    public RelDataType deriveDecimalDivideType(
            RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
        return deriveDecimalType(
                typeFactory, type1, type2, LogicalTypeMerging::findDivisionDecimalType);
    }

    @Override
    public RelDataType deriveDecimalMultiplyType(
            RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
        return deriveDecimalType(
                typeFactory, type1, type2, LogicalTypeMerging::findMultiplicationDecimalType);
    }

    /** Use derivation from {@link LogicalTypeMerging} to derive decimal type. */
    private @Nullable RelDataType deriveDecimalType(
            RelDataTypeFactory typeFactory,
            RelDataType type1,
            RelDataType type2,
            QuadFunction<Integer, Integer, Integer, Integer, DecimalType> deriveImpl) {
        return deriveDecimalRelDataType(
                typeFactory,
                type1,
                type2,
                (p1, s1, p2, s2) -> {
                    DecimalType result = deriveImpl.apply(p1, s1, p2, s2);
                    return typeFactory.createSqlType(
                            DECIMAL, result.getPrecision(), result.getScale());
                });
    }

    private @Nullable RelDataType deriveDecimalRelDataType(
            RelDataTypeFactory typeFactory,
            RelDataType type1,
            RelDataType type2,
            QuadFunction<Integer, Integer, Integer, Integer, RelDataType> deriveImpl) {
        if (canDeriveDecimal(type1, type2)) {
            RelDataType decType1 = adjustType(typeFactory, type1);
            RelDataType decType2 = adjustType(typeFactory, type2);
            return deriveImpl.apply(
                    decType1.getPrecision(),
                    decType1.getScale(),
                    decType2.getPrecision(),
                    decType2.getScale());
        } else {
            return null;
        }
    }

    /**
     * Java numeric will always have invalid precision/scale, use its default decimal
     * precision/scale instead.
     */
    private RelDataType adjustType(RelDataTypeFactory typeFactory, RelDataType relDataType) {
        return RelDataTypeFactoryImpl.isJavaType(relDataType)
                ? typeFactory.decimalOf(relDataType)
                : relDataType;
    }

    private boolean canDeriveDecimal(RelDataType type1, RelDataType type2) {
        return SqlTypeUtil.isExactNumeric(type1)
                && SqlTypeUtil.isExactNumeric(type2)
                && (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2));
    }
}
