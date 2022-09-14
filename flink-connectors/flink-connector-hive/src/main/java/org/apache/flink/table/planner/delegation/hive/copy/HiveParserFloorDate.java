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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMonotonicUnaryFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/** Counterpart of hive's org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate. */
public class HiveParserFloorDate extends SqlMonotonicUnaryFunction {

    public static final SqlFunction YEAR = new HiveParserFloorDate("FLOOR_YEAR");
    public static final SqlFunction QUARTER = new HiveParserFloorDate("FLOOR_QUARTER");
    public static final SqlFunction MONTH = new HiveParserFloorDate("FLOOR_MONTH");
    public static final SqlFunction WEEK = new HiveParserFloorDate("FLOOR_WEEK");
    public static final SqlFunction DAY = new HiveParserFloorDate("FLOOR_DAY");
    public static final SqlFunction HOUR = new HiveParserFloorDate("FLOOR_HOUR");
    public static final SqlFunction MINUTE = new HiveParserFloorDate("FLOOR_MINUTE");
    public static final SqlFunction SECOND = new HiveParserFloorDate("FLOOR_SECOND");

    private HiveParserFloorDate(String name) {
        super(
                name,
                SqlKind.FLOOR,
                ReturnTypes.ARG0_OR_EXACT_NO_SCALE,
                null,
                OperandTypes.sequence(
                        "'"
                                + SqlKind.FLOOR
                                + "(<DATE> TO <TIME_UNIT>)'\n"
                                + "'"
                                + SqlKind.FLOOR
                                + "(<TIME> TO <TIME_UNIT>)'\n"
                                + "'"
                                + SqlKind.FLOOR
                                + "(<TIMESTAMP> TO <TIME_UNIT>)'",
                        OperandTypes.DATETIME,
                        OperandTypes.ANY),
                SqlFunctionCategory.NUMERIC);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        // Monotonic iff its first argument is, but not strict.
        return call.getOperandMonotonicity(0).unstrict();
    }
}
