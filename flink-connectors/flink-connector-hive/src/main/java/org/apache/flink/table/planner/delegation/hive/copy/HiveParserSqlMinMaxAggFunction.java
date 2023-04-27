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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;

/**
 * Counterpart of hive's
 * org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction.
 */
public class HiveParserSqlMinMaxAggFunction extends SqlAggFunction {

    public HiveParserSqlMinMaxAggFunction(
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            boolean isMin) {
        super(
                isMin ? "min" : "max",
                null,
                isMin ? SqlKind.MIN : SqlKind.MAX,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz == SqlSplittableAggFunction.class) {
            return clazz.cast(SqlSplittableAggFunction.SelfSplitter.INSTANCE);
        }
        return super.unwrap(clazz);
    }
}
