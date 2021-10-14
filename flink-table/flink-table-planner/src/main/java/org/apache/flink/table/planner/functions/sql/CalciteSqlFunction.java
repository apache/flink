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

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * This is a simple override class for Calcite's {@link SqlFunction} which provide {@link
 * #isDeterministic()} as a constructor parameter to simplify construct a new {@link SqlFunction} in
 * {@link FlinkSqlOperatorTable}.
 */
public class CalciteSqlFunction extends SqlFunction {

    // ~ Instance fields --------------------------------------------------------

    private final boolean deterministic;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SqlFunction for a call to a builtin function.
     *
     * @param name Name of builtin function
     * @param kind kind of operator implemented by function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param category categorization for function
     * @param deterministic whether this operator is guaranteed to always return the same result
     *     given the same operands
     */
    public CalciteSqlFunction(
            String name,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category,
            boolean deterministic) {
        super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
        this.deterministic = deterministic;
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }
}
