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

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT UNTIL WE UPGRADE TO CALCITE 1.13.
 */

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 *
 * The <code>TIMESTAMPADD</code> function, which adds an interval to a
 * timestamp.
 *
 * <p>The SQL syntax is
 *
 * <blockquote>
 * <code>TIMESTAMPADD(<i>timestamp interval</i>, <i>quantity</i>,
 * <i>timestamp</i>)</code>
 * </blockquote>
 *
 * <p>The interval time unit can one of the following literals:<ul>
 * <li>MICROSECOND (and synonyms SQL_TSI_MICROSECOND, FRAC_SECOND,
 *     SQL_TSI_FRAC_SECOND)
 * <li>SECOND (and synonym SQL_TSI_SECOND)
 * <li>MINUTE (and synonym  SQL_TSI_MINUTE)
 * <li>HOUR (and synonym  SQL_TSI_HOUR)
 * <li>DAY (and synonym SQL_TSI_DAY)
 * <li>WEEK (and synonym  SQL_TSI_WEEK)
 * <li>MONTH (and synonym SQL_TSI_MONTH)
 * <li>QUARTER (and synonym SQL_TSI_QUARTER)
 * <li>YEAR (and synonym  SQL_TSI_YEAR)
 * </ul>
 *
 * <p>Returns modified timestamp.
 *
 * __Note__: Due to the change of [[org.apache.calcite.rex.RexLiteral]] we should keep using this
 * class until upgrade to calcite 1.13.
 */
class SqlTimestampAddFunction extends SqlFunction {

    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
            new SqlReturnTypeInference() {
                public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                    final TimeUnit unit = (TimeUnit) opBinding.getOperandLiteralValue(0);
                    switch (unit) {
                        case HOUR:
                        case MINUTE:
                        case SECOND:
                        case MILLISECOND:
                        case MICROSECOND:
                            return typeFactory.createTypeWithNullability(
                                    typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
                                    opBinding.getOperandType(1).isNullable()
                                    || opBinding.getOperandType(2).isNullable());
                        default:
                            return opBinding.getOperandType(2);
                    }
                }
            };

    /** Creates a SqlTimestampAddFunction. */
    SqlTimestampAddFunction() {
        super("TIMESTAMPADD", SqlKind.TIMESTAMP_ADD, RETURN_TYPE_INFERENCE, null,
              OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER,
                                  SqlTypeFamily.TIMESTAMP),
              SqlFunctionCategory.TIMEDATE);
    }
}

// End SqlTimestampAddFunction.java
