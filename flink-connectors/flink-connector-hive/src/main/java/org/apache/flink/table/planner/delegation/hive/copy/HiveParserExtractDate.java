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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate.
 */
public class HiveParserExtractDate extends SqlFunction {

    public static final SqlFunction YEAR = new HiveParserExtractDate("YEAR");
    public static final SqlFunction QUARTER = new HiveParserExtractDate("QUARTER");
    public static final SqlFunction MONTH = new HiveParserExtractDate("MONTH");
    public static final SqlFunction WEEK = new HiveParserExtractDate("WEEKOFYEAR");
    public static final SqlFunction DAY = new HiveParserExtractDate("DAY");
    public static final SqlFunction HOUR = new HiveParserExtractDate("HOUR");
    public static final SqlFunction MINUTE = new HiveParserExtractDate("MINUTE");
    public static final SqlFunction SECOND = new HiveParserExtractDate("SECOND");

    private HiveParserExtractDate(String name) {
        super(
                name,
                SqlKind.EXTRACT,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
                SqlFunctionCategory.SYSTEM);
    }
}
