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
package org.apache.calcite.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ExplicitReturnTypeInference;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import java.util.List;

/**
 * SQL function that computes keys by which rows can be partitioned and aggregated.
 *
 * <p>Grouped window functions always occur in the GROUP BY clause. They often have auxiliary
 * functions that access information about the group. For example, {@code HOP} is a group function,
 * and its auxiliary functions are {@code HOP_START} and {@code HOP_END}. Here they are used in a
 * streaming query:
 *
 * <p>Note: we copied the implementation from Calcite's {@link
 * org.apache.calcite.sql.SqlGroupedWindowFunction} because of CALCITE-4563, Calcite currently
 * doesn't allow to set the SqlReturnTypeInference of auxiliary SqlGroupedWindowFunction.
 *
 * <p>The motivation is using TIMESTAMP type for the window start and window end no matter the time
 * attribute column is TIMESTAMP or TIMESTAMP_LTZ.
 */
public class SqlGroupedWindowFunction extends SqlFunction {

    /** The grouped function, if this an auxiliary function; null otherwise. */
    public final SqlGroupedWindowFunction groupFunction;

    private final WindowStartEndReturnTypeInference windowStartEndInf =
            new WindowStartEndReturnTypeInference();

    /**
     * Creates a SqlGroupedWindowFunction.
     *
     * @param name Function name
     * @param kind Kind
     * @param groupFunction Group function, if this is an auxiliary; null, if this is a group
     *     function
     * @param returnTypeInference Strategy to use for return type inference
     * @param operandTypeInference Strategy to use for parameter type inference
     * @param operandTypeChecker Strategy to use for parameter type checking
     * @param category Categorization for function
     */
    public SqlGroupedWindowFunction(
            String name,
            SqlKind kind,
            SqlGroupedWindowFunction groupFunction,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category) {
        super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
        this.groupFunction = groupFunction;
        Preconditions.checkArgument(groupFunction == null || groupFunction.groupFunction == null);
    }

    /**
     * Creates a SqlGroupedWindowFunction.
     *
     * @param name Function name
     * @param kind Kind
     * @param groupFunction Group function, if this is an auxiliary; null, if this is a group
     *     function
     */
    public SqlGroupedWindowFunction(
            String name,
            SqlKind kind,
            SqlGroupedWindowFunction groupFunction,
            SqlOperandTypeChecker operandTypeChecker) {
        this(
                name,
                kind,
                groupFunction,
                ReturnTypes.ARG0,
                null,
                operandTypeChecker,
                SqlFunctionCategory.SYSTEM);
    }

    /**
     * Creates an auxiliary function from this grouped window function.
     *
     * @param kind Kind; also determines function name
     */
    public SqlGroupedWindowFunction auxiliary(SqlKind kind) {
        return auxiliary(kind.name(), kind);
    }

    /**
     * Creates an auxiliary function from this grouped window function.
     *
     * @param name Function name
     * @param kind Kind
     */
    public SqlGroupedWindowFunction auxiliary(String name, SqlKind kind) {
        switch (kind) {
            case TUMBLE_START:
            case TUMBLE_END:
            case HOP_START:
            case HOP_END:
            case SESSION_START:
            case SESSION_END:
                return new SqlGroupedWindowFunction(
                        name,
                        kind,
                        this,
                        windowStartEndInf,
                        null,
                        getOperandTypeChecker(),
                        SqlFunctionCategory.SYSTEM);
            default:
                return new SqlGroupedWindowFunction(
                        name,
                        kind,
                        this,
                        ReturnTypes.ARG0,
                        null,
                        getOperandTypeChecker(),
                        SqlFunctionCategory.SYSTEM);
        }
    }

    /** Returns a list of this grouped window function's auxiliary functions. */
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
        return ImmutableList.of();
    }

    @Override
    public boolean isGroup() {
        // Auxiliary functions are not group functions
        return groupFunction == null;
    }

    @Override
    public boolean isGroupAuxiliary() {
        return groupFunction != null;
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        // Monotonic iff its first argument is, but not strict.
        //
        // Note: This strategy happens to works for all current group functions
        // (HOP, TUMBLE, SESSION). When there are exceptions to this rule, we'll
        // make the method abstract.
        return call.getOperandMonotonicity(0).unstrict();
    }

    /**
     * ReturnTypeInference that returns TIMESTAMP(3) as the type of window start and window end.
     *
     * <p>We use the first operand of window function to decide the return type which can avoid many
     * necessary `CAST(w$end) AS EXPR$1` expressions that will be produced we directly use
     * `ReturnTypes.explicit(SqlTypeName.TIMESTAMP, 3)`.
     */
    private static class WindowStartEndReturnTypeInference implements SqlReturnTypeInference {

        private static final ExplicitReturnTypeInference explicit =
                ReturnTypes.explicit(SqlTypeName.TIMESTAMP, 3);

        public WindowStartEndReturnTypeInference() {}

        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            return explicit.inferReturnType(opBinding);
        }
    }
}
