/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The <code>TIMESTAMPADD</code> function, which adds an interval to a datetime (TIMESTAMP, TIME or
 * DATE).
 *
 * <p>The SQL syntax is
 *
 * <blockquote>
 *
 * <code>TIMESTAMPADD(<i>timestamp interval</i>, <i>quantity</i>,
 * <i>datetime</i>)</code>
 *
 * </blockquote>
 *
 * <p>The interval time unit can one of the following literals:
 *
 * <ul>
 *   <li>NANOSECOND (and synonym SQL_TSI_FRAC_SECOND)
 *   <li>MICROSECOND (and synonyms SQL_TSI_MICROSECOND, FRAC_SECOND)
 *   <li>SECOND (and synonym SQL_TSI_SECOND)
 *   <li>MINUTE (and synonym SQL_TSI_MINUTE)
 *   <li>HOUR (and synonym SQL_TSI_HOUR)
 *   <li>DAY (and synonym SQL_TSI_DAY)
 *   <li>WEEK (and synonym SQL_TSI_WEEK)
 *   <li>MONTH (and synonym SQL_TSI_MONTH)
 *   <li>QUARTER (and synonym SQL_TSI_QUARTER)
 *   <li>YEAR (and synonym SQL_TSI_YEAR)
 * </ul>
 *
 * <p>Returns modified datetime.
 *
 * <p>This class was copied over from Calcite to fix the return type deduction issue on timestamp
 * with local time zone type (CALCITE-4698).
 */
public class SqlTimestampAddFunction extends SqlFunction {

    private static final int MILLISECOND_PRECISION = 3;
    private static final int MICROSECOND_PRECISION = 6;

    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
            opBinding -> {
                final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                return deduceType(
                        typeFactory,
                        opBinding.getOperandLiteralValue(0, TimeUnit.class),
                        opBinding.getOperandType(1),
                        opBinding.getOperandType(2));
            };

    // BEGIN FLINK MODIFICATION
    // Reason: this method is changed to deduce return type on timestamp with local time zone
    // correctly
    // Whole class should be removed after CALCITE-4698 is fixed
    public static RelDataType deduceType(
            RelDataTypeFactory typeFactory,
            TimeUnit timeUnit,
            RelDataType intervalType,
            RelDataType datetimeType) {
        RelDataType type;
        switch (timeUnit) {
            case MILLISECOND:
                type =
                        typeFactory.createSqlType(
                                timestampOrTimestampLtz(datetimeType),
                                Math.max(MILLISECOND_PRECISION, datetimeType.getPrecision()));
                break;
            case MICROSECOND:
                type =
                        typeFactory.createSqlType(
                                timestampOrTimestampLtz(datetimeType),
                                Math.max(MICROSECOND_PRECISION, datetimeType.getPrecision()));
                break;
            case HOUR:
            case MINUTE:
            case SECOND:
                if (datetimeType.getFamily() == SqlTypeFamily.TIME) {
                    type = datetimeType;
                } else if (datetimeType.getFamily() == SqlTypeFamily.TIMESTAMP) {
                    type =
                            typeFactory.createSqlType(
                                    timestampOrTimestampLtz(datetimeType),
                                    datetimeType.getPrecision());
                } else {
                    type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                }
                break;
            default:
                type = datetimeType;
        }
        return typeFactory.createTypeWithNullability(
                type, intervalType.isNullable() || datetimeType.isNullable());
    }

    private static SqlTypeName timestampOrTimestampLtz(RelDataType datetimeType) {
        return datetimeType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                ? SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                : SqlTypeName.TIMESTAMP;
    }
    // END FLINK MODIFICATION

    /** Creates a SqlTimestampAddFunction. */
    SqlTimestampAddFunction() {
        super(
                "TIMESTAMPADD",
                SqlKind.TIMESTAMP_ADD,
                RETURN_TYPE_INFERENCE,
                null,
                OperandTypes.family(
                        SqlTypeFamily.ANY, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME),
                SqlFunctionCategory.TIMEDATE);
    }
}
