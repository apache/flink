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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.internal.SqlAuxiliaryGroupAggFunction;
import org.apache.flink.table.planner.plan.type.FlinkReturnTypes;
import org.apache.flink.table.planner.plan.type.NumericExceptFirstOperandChecker;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.plan.type.FlinkReturnTypes.ARG0_VARCHAR_FORCE_NULLABLE;
import static org.apache.flink.table.planner.plan.type.FlinkReturnTypes.STR_MAP_NULLABLE;
import static org.apache.flink.table.planner.plan.type.FlinkReturnTypes.VARCHAR_FORCE_NULLABLE;
import static org.apache.flink.table.planner.plan.type.FlinkReturnTypes.VARCHAR_NOT_NULL;

/** Operator table that contains only Flink-specific functions and operators. */
public class FlinkSqlOperatorTable extends ReflectiveSqlOperatorTable {

    /** The table of contains Flink-specific operators. */
    private static final Map<Boolean, FlinkSqlOperatorTable> cachedInstances = new HashMap<>();

    /** Returns the Flink operator table, creating it if necessary. */
    public static synchronized FlinkSqlOperatorTable instance(boolean isBatchMode) {
        FlinkSqlOperatorTable instance = cachedInstances.get(isBatchMode);
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the
            // table until the constructor of the sub-class has completed.
            instance = new FlinkSqlOperatorTable();
            instance.init();

            // ensure no dynamic function declares directly
            validateNoDynamicFunction(instance);

            // register functions based on batch or streaming mode
            final FlinkSqlOperatorTable finalInstance = instance;
            dynamicFunctions(isBatchMode).forEach(f -> finalInstance.register(f));
            cachedInstances.put(isBatchMode, finalInstance);
        }
        return instance;
    }

    public static List<SqlFunction> dynamicFunctions(boolean isBatchMode) {
        return Arrays.asList(
                new FlinkTimestampDynamicFunction(
                        SqlStdOperatorTable.LOCALTIME.getName(), SqlTypeName.TIME, isBatchMode),
                new FlinkTimestampDynamicFunction(
                        SqlStdOperatorTable.CURRENT_TIME.getName(), SqlTypeName.TIME, isBatchMode),
                new FlinkCurrentDateDynamicFunction(isBatchMode),
                new FlinkTimestampWithPrecisionDynamicFunction(
                        SqlStdOperatorTable.LOCALTIMESTAMP.getName(),
                        SqlTypeName.TIMESTAMP,
                        isBatchMode,
                        3),
                new FlinkTimestampWithPrecisionDynamicFunction(
                        SqlStdOperatorTable.CURRENT_TIMESTAMP.getName(),
                        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                        isBatchMode,
                        3),
                new FlinkTimestampWithPrecisionDynamicFunction(
                        FlinkTimestampWithPrecisionDynamicFunction.NOW,
                        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                        isBatchMode,
                        3) {
                    @Override
                    public SqlSyntax getSyntax() {
                        return SqlSyntax.FUNCTION;
                    }
                });
    }

    private static void validateNoDynamicFunction(FlinkSqlOperatorTable instance)
            throws TableException {
        instance.getOperatorList()
                .forEach(
                        op -> {
                            if (op.isDynamicFunction() && op.isDeterministic()) {
                                throw new TableException(
                                        String.format(
                                                "Dynamic function: %s is not allowed declaring directly, please add it to initializing.",
                                                op.getName()));
                            }
                        });
    }

    private static final SqlReturnTypeInference PROCTIME_TYPE_INFERENCE =
            ReturnTypes.explicit(
                    factory -> ((FlinkTypeFactory) factory).createProctimeIndicatorType(false));

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        // set caseSensitive=false to make sure the behavior is same with before.
        super.lookupOperatorOverloads(
                opName, category, syntax, operatorList, SqlNameMatchers.withCaseSensitive(false));
    }

    // -----------------------------------------------------------------------------
    // Flink specific built-in scalar SQL functions
    // -----------------------------------------------------------------------------

    // FUNCTIONS

    /** Function used to access a processing time attribute. */
    public static final SqlFunction PROCTIME =
            BuiltInSqlFunction.newBuilder()
                    .name("PROCTIME")
                    .returnType(PROCTIME_TYPE_INFERENCE)
                    .operandTypeChecker(OperandTypes.NILADIC)
                    .notDeterministic()
                    .build();

    /**
     * Function used to access an event time attribute with TIMESTAMP or TIMESTAMP_LTZ type from
     * MATCH_RECOGNIZE.
     */
    public static final SqlFunction MATCH_ROWTIME = new MatchRowTimeFunction();

    /** Function used to access a processing time attribute from MATCH_RECOGNIZE. */
    public static final SqlFunction MATCH_PROCTIME =
            BuiltInSqlFunction.newBuilder()
                    .name("MATCH_PROCTIME")
                    .category(SqlFunctionCategory.MATCH_RECOGNIZE)
                    .returnType(PROCTIME_TYPE_INFERENCE)
                    .operandTypeChecker(OperandTypes.NILADIC)
                    .notDeterministic()
                    .build();

    /**
     * Function that materializes a processing time attribute. After materialization the result can
     * be used in regular arithmetical calculations.
     */
    public static final SqlFunction PROCTIME_MATERIALIZE =
            BuiltInSqlFunction.newBuilder()
                    .name("PROCTIME_MATERIALIZE")
                    .returnType(
                            ReturnTypes.cascade(
                                    ReturnTypes.explicit(
                                            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
                                    SqlTypeTransforms.TO_NULLABLE))
                    .operandTypeInference(InferTypes.RETURN_TYPE)
                    .operandTypeChecker(OperandTypes.family(SqlTypeFamily.TIMESTAMP))
                    .monotonicity(SqlMonotonicity.INCREASING)
                    .notDeterministic()
                    .build();

    /** Function to access the timestamp of a StreamRecord. */
    public static final SqlFunction STREAMRECORD_TIMESTAMP =
            BuiltInSqlFunction.newBuilder()
                    .name("STREAMRECORD_TIMESTAMP")
                    .returnType(ReturnTypes.explicit(SqlTypeName.BIGINT))
                    .operandTypeInference(InferTypes.RETURN_TYPE)
                    .operandTypeChecker(OperandTypes.family(SqlTypeFamily.NUMERIC))
                    .build();

    /** Function to access the constant value of E. */
    public static final SqlFunction E =
            BuiltInSqlFunction.newBuilder()
                    .name("E")
                    .returnType(ReturnTypes.DOUBLE)
                    .operandTypeChecker(OperandTypes.NILADIC)
                    .build();

    /** Function to access the constant value of PI. */
    public static final SqlFunction PI_FUNCTION =
            BuiltInSqlFunction.newBuilder()
                    .name("PI")
                    .returnType(ReturnTypes.DOUBLE)
                    .operandTypeChecker(OperandTypes.NILADIC)
                    .build();

    /** Function for concat strings, it is same with {@link #CONCAT}, but this is a function. */
    public static final SqlFunction CONCAT_FUNCTION =
            BuiltInSqlFunction.newBuilder()
                    .name("CONCAT")
                    .returnType(
                            ReturnTypes.cascade(
                                    ReturnTypes.explicit(SqlTypeName.VARCHAR),
                                    SqlTypeTransforms.TO_NULLABLE))
                    .operandTypeChecker(
                            OperandTypes.repeat(
                                    SqlOperandCountRanges.from(1), OperandTypes.CHARACTER))
                    .build();

    /** Function for concat strings with a separator. */
    public static final SqlFunction CONCAT_WS =
            new SqlFunction(
                    "CONCAT_WS",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.repeat(SqlOperandCountRanges.from(1), OperandTypes.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction LOG =
            new SqlFunction(
                    "LOG",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.NUMERIC,
                            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction LOG2 =
            new SqlFunction(
                    "LOG2",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction ROUND =
            new SqlFunction(
                    "ROUND",
                    SqlKind.OTHER_FUNCTION,
                    FlinkReturnTypes.ROUND_FUNCTION_NULLABLE,
                    null,
                    OperandTypes.or(OperandTypes.NUMERIC_INTEGER, OperandTypes.NUMERIC),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction TRUNCATE =
            new SqlFunction(
                    "TRUNCATE",
                    SqlKind.OTHER_FUNCTION,
                    FlinkReturnTypes.ROUND_FUNCTION_NULLABLE,
                    null,
                    OperandTypes.or(OperandTypes.NUMERIC_INTEGER, OperandTypes.NUMERIC),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction BIN =
            new SqlFunction(
                    "BIN",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    InferTypes.RETURN_TYPE,
                    OperandTypes.family(SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction SINH =
            new SqlFunction(
                    "SINH",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction HEX =
            new SqlFunction(
                    "HEX",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    InferTypes.RETURN_TYPE,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.INTEGER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction STR_TO_MAP =
            new SqlFunction(
                    "STR_TO_MAP",
                    SqlKind.OTHER_FUNCTION,
                    STR_MAP_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction IS_DECIMAL =
            new SqlFunction(
                    "IS_DECIMAL",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.or(OperandTypes.CHARACTER, OperandTypes.NUMERIC),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction IS_DIGIT =
            new SqlFunction(
                    "IS_DIGIT",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.or(OperandTypes.CHARACTER, OperandTypes.NUMERIC),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction IS_ALPHA =
            new SqlFunction(
                    "IS_ALPHA",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.or(OperandTypes.CHARACTER, OperandTypes.NUMERIC),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction COSH =
            new SqlFunction(
                    "COSH",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction TANH =
            new SqlFunction(
                    "TANH",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction CHR =
            new SqlFunction(
                    "CHR",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction LPAD =
            new SqlFunction(
                    "LPAD",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.INTEGER,
                            SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction RPAD =
            new SqlFunction(
                    "RPAD",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.INTEGER,
                            SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction REPEAT =
            new SqlFunction(
                    "REPEAT",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction REVERSE =
            new SqlFunction(
                    "REVERSE",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction REPLACE =
            new SqlFunction(
                    "REPLACE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SPLIT_INDEX =
            new SqlFunction(
                    "SPLIT_INDEX",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER,
                                    SqlTypeFamily.INTEGER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction REGEXP_REPLACE =
            new SqlFunction(
                    "REGEXP_REPLACE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction REGEXP_EXTRACT =
            new SqlFunction(
                    "REGEXP_EXTRACT",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction HASH_CODE =
            new SqlFunction(
                    "HASH_CODE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.BOOLEAN),
                            OperandTypes.family(SqlTypeFamily.NUMERIC),
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                            OperandTypes.family(SqlTypeFamily.TIME),
                            OperandTypes.family(SqlTypeFamily.DATE)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction MD5 =
            new SqlFunction(
                    "MD5",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.CHAR, 32),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SHA1 =
            new SqlFunction(
                    "SHA1",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.CHAR, 40),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SHA224 =
            new SqlFunction(
                    "SHA224",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.CHAR, 56),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SHA256 =
            new SqlFunction(
                    "SHA256",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.CHAR, 64),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SHA384 =
            new SqlFunction(
                    "SHA384",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.CHAR, 96),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SHA512 =
            new SqlFunction(
                    "SHA512",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.CHAR, 128),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SHA2 =
            new SqlFunction(
                    "SHA2",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR, 128),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.sequence(
                            "'SHA2(DATA, HASH_LENGTH)'",
                            OperandTypes.STRING,
                            OperandTypes.NUMERIC_INTEGER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction DATE_FORMAT =
            new SqlFunction(
                    "DATE_FORMAT",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction REGEXP =
            new SqlFunction(
                    "REGEXP",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction PARSE_URL =
            new SqlFunction(
                    "PARSE_URL",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction PRINT =
            new SqlFunction(
                    "PRINT",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG1_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY),
                    SqlFunctionCategory.STRING);

    // Flink timestamp functions
    public static final SqlFunction CURRENT_ROW_TIMESTAMP =
            new FlinkCurrentRowTimestampFunction(
                    "CURRENT_ROW_TIMESTAMP", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3) {

                @Override
                public SqlSyntax getSyntax() {
                    return SqlSyntax.FUNCTION;
                }
            };

    public static final SqlFunction UNIX_TIMESTAMP =
            BuiltInSqlFunction.newBuilder()
                    .name("UNIX_TIMESTAMP")
                    .returnType(ReturnTypes.BIGINT_NULLABLE)
                    .operandTypeChecker(
                            OperandTypes.or(
                                    OperandTypes.NILADIC,
                                    OperandTypes.family(SqlTypeFamily.CHARACTER),
                                    OperandTypes.family(
                                            SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)))
                    .notDeterministic()
                    .monotonicity(
                            call -> {
                                if (call.getOperandCount() == 0) {
                                    return SqlMonotonicity.INCREASING;
                                } else {
                                    return SqlMonotonicity.NOT_MONOTONIC;
                                }
                            })
                    .build();

    public static final SqlFunction FROM_UNIXTIME =
            new SqlFunction(
                    "FROM_UNIXTIME",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.INTEGER),
                            OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction IF =
            new SqlFunction(
                    "IF",
                    SqlKind.OTHER_FUNCTION,
                    FlinkReturnTypes.IF_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.and(
                                    // cannot only use `family(BOOLEAN, NUMERIC, NUMERIC)` here,
                                    // as we don't want non-numeric types to be implicitly casted to
                                    // numeric types.
                                    new NumericExceptFirstOperandChecker(3),
                                    OperandTypes.family(
                                            SqlTypeFamily.BOOLEAN,
                                            SqlTypeFamily.NUMERIC,
                                            SqlTypeFamily.NUMERIC)),
                            // used for a more explicit exception message
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.BOOLEAN),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.BINARY,
                                    SqlTypeFamily.BINARY),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN, SqlTypeFamily.DATE, SqlTypeFamily.DATE),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.TIMESTAMP,
                                    SqlTypeFamily.TIMESTAMP),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN, SqlTypeFamily.TIME, SqlTypeFamily.TIME)),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction TO_BASE64 =
            new SqlFunction(
                    "TO_BASE64",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction FROM_BASE64 =
            new SqlFunction(
                    "FROM_BASE64",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction UUID =
            BuiltInSqlFunction.newBuilder()
                    .name("UUID")
                    .returnType(ReturnTypes.explicit(SqlTypeName.CHAR, 36))
                    .operandTypeChecker(OperandTypes.NILADIC)
                    .notDeterministic()
                    .build();

    public static final SqlFunction SUBSTRING =
            new SqlFunction(
                    "SUBSTRING",
                    SqlKind.OTHER_FUNCTION,
                    ARG0_VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER,
                                    SqlTypeFamily.INTEGER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SUBSTR =
            new SqlFunction(
                    "SUBSTR",
                    SqlKind.OTHER_FUNCTION,
                    ARG0_VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER,
                                    SqlTypeFamily.INTEGER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction LEFT =
            new SqlFunction(
                    "LEFT",
                    SqlKind.OTHER_FUNCTION,
                    ARG0_VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction RIGHT =
            new SqlFunction(
                    "RIGHT",
                    SqlKind.OTHER_FUNCTION,
                    ARG0_VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.STRING);

    // TODO: the return type of TO_TIMESTAMP should be TIMESTAMP(9),
    //  but we keep TIMESTAMP(3) now because we did not support TIMESTAMP(9) as time attribute.
    //  See: https://issues.apache.org/jira/browse/FLINK-14925
    public static final SqlFunction TO_TIMESTAMP =
            new SqlFunction(
                    "TO_TIMESTAMP",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.TIMESTAMP, 3),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction TO_TIMESTAMP_LTZ =
            new SqlFunction(
                    "TO_TIMESTAMP_LTZ",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction TO_DATE =
            new SqlFunction(
                    "TO_DATE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.DATE),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction CONVERT_TZ =
            new SqlFunction(
                    "CONVERT_TZ",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.CHARACTER,
                            SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction LOCATE =
            new SqlFunction(
                    "LOCATE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER)),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction ASCII =
            new SqlFunction(
                    "ASCII",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction ENCODE =
            new SqlFunction(
                    "ENCODE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.BINARY),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction DECODE =
            new SqlFunction(
                    "DECODE",
                    SqlKind.OTHER_FUNCTION,
                    VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction INSTR =
            new SqlFunction(
                    "INSTR",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER,
                                    SqlTypeFamily.INTEGER)),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction LTRIM =
            new SqlFunction(
                    "LTRIM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.ARG0,
                            SqlTypeTransforms.TO_NULLABLE,
                            SqlTypeTransforms.TO_VARYING),
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction RTRIM =
            new SqlFunction(
                    "RTRIM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.ARG0,
                            SqlTypeTransforms.TO_NULLABLE,
                            SqlTypeTransforms.TO_VARYING),
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction TRY_CAST = new SqlTryCastFunction();

    public static final SqlFunction RAND =
            new SqlFunction(
                    "RAND",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE,
                    null,
                    OperandTypes.or(
                            new SqlSingleOperandTypeChecker[] {
                                OperandTypes.NILADIC, OperandTypes.NUMERIC
                            }),
                    SqlFunctionCategory.NUMERIC) {

                @Override
                public boolean isDeterministic() {
                    return false;
                }
            };

    public static final SqlFunction RAND_INTEGER =
            new SqlFunction(
                    "RAND_INTEGER",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER,
                    null,
                    OperandTypes.or(
                            new SqlSingleOperandTypeChecker[] {
                                OperandTypes.NUMERIC, OperandTypes.NUMERIC_NUMERIC
                            }),
                    SqlFunctionCategory.NUMERIC) {

                @Override
                public boolean isDeterministic() {
                    return false;
                }
            };

    /** <code>AUXILIARY_GROUP</code> aggregate function. Only be used in internally. */
    public static final SqlAggFunction AUXILIARY_GROUP = new SqlAuxiliaryGroupAggFunction();

    /** <code>FIRST_VALUE</code> aggregate function. */
    public static final SqlFirstLastValueAggFunction FIRST_VALUE =
            new SqlFirstLastValueAggFunction(SqlKind.FIRST_VALUE);

    /** <code>LAST_VALUE</code> aggregate function. */
    public static final SqlFirstLastValueAggFunction LAST_VALUE =
            new SqlFirstLastValueAggFunction(SqlKind.LAST_VALUE);

    /** <code>LISTAGG</code> aggregate function. */
    public static final SqlListAggFunction LISTAGG = new SqlListAggFunction();

    // -----------------------------------------------------------------------------
    // Window SQL functions
    // -----------------------------------------------------------------------------

    /** We need custom group auxiliary functions in order to support nested windows. */
    public static final SqlGroupedWindowFunction TUMBLE_OLD =
            new SqlGroupedWindowFunction(
                    // The TUMBLE group function was hard code to $TUMBLE in CALCITE-3382.
                    "$TUMBLE",
                    SqlKind.TUMBLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME)) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return Arrays.asList(TUMBLE_START, TUMBLE_END, TUMBLE_ROWTIME, TUMBLE_PROCTIME);
                }
            };

    public static final SqlGroupedWindowFunction TUMBLE_START =
            TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_START);
    public static final SqlGroupedWindowFunction TUMBLE_END =
            TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_END);
    public static final SqlGroupedWindowFunction TUMBLE_ROWTIME =
            TUMBLE_OLD.auxiliary("TUMBLE_ROWTIME", SqlKind.OTHER_FUNCTION);
    public static final SqlGroupedWindowFunction TUMBLE_PROCTIME =
            TUMBLE_OLD.auxiliary("TUMBLE_PROCTIME", SqlKind.OTHER_FUNCTION);

    public static final SqlGroupedWindowFunction HOP_OLD =
            new SqlGroupedWindowFunction(
                    "$HOP",
                    SqlKind.HOP,
                    null,
                    OperandTypes.or(
                            OperandTypes.DATETIME_INTERVAL_INTERVAL,
                            OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME)) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return Arrays.asList(HOP_START, HOP_END, HOP_ROWTIME, HOP_PROCTIME);
                }
            };

    public static final SqlGroupedWindowFunction HOP_START = HOP_OLD.auxiliary(SqlKind.HOP_START);
    public static final SqlGroupedWindowFunction HOP_END = HOP_OLD.auxiliary(SqlKind.HOP_END);
    public static final SqlGroupedWindowFunction HOP_ROWTIME =
            HOP_OLD.auxiliary("HOP_ROWTIME", SqlKind.OTHER_FUNCTION);
    public static final SqlGroupedWindowFunction HOP_PROCTIME =
            HOP_OLD.auxiliary("HOP_PROCTIME", SqlKind.OTHER_FUNCTION);

    public static final SqlGroupedWindowFunction SESSION_OLD =
            new SqlGroupedWindowFunction(
                    "$SESSION",
                    SqlKind.SESSION,
                    null,
                    OperandTypes.or(
                            OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME)) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return Arrays.asList(
                            SESSION_START, SESSION_END, SESSION_ROWTIME, SESSION_PROCTIME);
                }
            };

    public static final SqlGroupedWindowFunction SESSION_START =
            SESSION_OLD.auxiliary(SqlKind.SESSION_START);
    public static final SqlGroupedWindowFunction SESSION_END =
            SESSION_OLD.auxiliary(SqlKind.SESSION_END);
    public static final SqlGroupedWindowFunction SESSION_ROWTIME =
            SESSION_OLD.auxiliary("SESSION_ROWTIME", SqlKind.OTHER_FUNCTION);
    public static final SqlGroupedWindowFunction SESSION_PROCTIME =
            SESSION_OLD.auxiliary("SESSION_PROCTIME", SqlKind.OTHER_FUNCTION);

    // -----------------------------------------------------------------------------
    // operators extend from Calcite
    // -----------------------------------------------------------------------------

    // SET OPERATORS
    public static final SqlOperator UNION = SqlStdOperatorTable.UNION;
    public static final SqlOperator UNION_ALL = SqlStdOperatorTable.UNION_ALL;
    public static final SqlOperator EXCEPT = SqlStdOperatorTable.EXCEPT;
    public static final SqlOperator EXCEPT_ALL = SqlStdOperatorTable.EXCEPT_ALL;
    public static final SqlOperator INTERSECT = SqlStdOperatorTable.INTERSECT;
    public static final SqlOperator INTERSECT_ALL = SqlStdOperatorTable.INTERSECT_ALL;

    // BINARY OPERATORS
    public static final SqlOperator AND = SqlStdOperatorTable.AND;
    public static final SqlOperator AS = SqlStdOperatorTable.AS;
    public static final SqlOperator CONCAT = SqlStdOperatorTable.CONCAT;
    public static final SqlOperator DIVIDE = SqlStdOperatorTable.DIVIDE;
    public static final SqlOperator DIVIDE_INTEGER = SqlStdOperatorTable.DIVIDE_INTEGER;
    public static final SqlOperator DOT = SqlStdOperatorTable.DOT;
    public static final SqlOperator EQUALS = SqlStdOperatorTable.EQUALS;
    public static final SqlOperator GREATER_THAN = SqlStdOperatorTable.GREATER_THAN;
    public static final SqlOperator IS_DISTINCT_FROM = SqlStdOperatorTable.IS_DISTINCT_FROM;
    public static final SqlOperator IS_NOT_DISTINCT_FROM = SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
    public static final SqlOperator GREATER_THAN_OR_EQUAL =
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    public static final SqlOperator LESS_THAN = SqlStdOperatorTable.LESS_THAN;
    public static final SqlOperator LESS_THAN_OR_EQUAL = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    public static final SqlOperator MINUS = SqlStdOperatorTable.MINUS;
    public static final SqlOperator MINUS_DATE = SqlStdOperatorTable.MINUS_DATE;
    public static final SqlOperator MULTIPLY = SqlStdOperatorTable.MULTIPLY;
    public static final SqlOperator NOT_EQUALS = SqlStdOperatorTable.NOT_EQUALS;
    public static final SqlOperator OR = SqlStdOperatorTable.OR;
    public static final SqlOperator PLUS = SqlStdOperatorTable.PLUS;
    public static final SqlOperator DATETIME_PLUS = SqlStdOperatorTable.DATETIME_PLUS;
    public static final SqlOperator PERCENT_REMAINDER = SqlStdOperatorTable.PERCENT_REMAINDER;

    // POSTFIX OPERATORS
    public static final SqlOperator DESC = SqlStdOperatorTable.DESC;
    public static final SqlOperator NULLS_FIRST = SqlStdOperatorTable.NULLS_FIRST;
    public static final SqlOperator NULLS_LAST = SqlStdOperatorTable.NULLS_LAST;
    public static final SqlOperator IS_NOT_NULL = SqlStdOperatorTable.IS_NOT_NULL;
    public static final SqlOperator IS_NULL = SqlStdOperatorTable.IS_NULL;
    public static final SqlOperator IS_NOT_TRUE = SqlStdOperatorTable.IS_NOT_TRUE;
    public static final SqlOperator IS_TRUE = SqlStdOperatorTable.IS_TRUE;
    public static final SqlOperator IS_NOT_FALSE = SqlStdOperatorTable.IS_NOT_FALSE;
    public static final SqlOperator IS_FALSE = SqlStdOperatorTable.IS_FALSE;
    public static final SqlOperator IS_NOT_UNKNOWN = SqlStdOperatorTable.IS_NOT_UNKNOWN;
    public static final SqlOperator IS_UNKNOWN = SqlStdOperatorTable.IS_UNKNOWN;

    // PREFIX OPERATORS
    public static final SqlOperator NOT = SqlStdOperatorTable.NOT;
    public static final SqlOperator UNARY_MINUS = SqlStdOperatorTable.UNARY_MINUS;
    public static final SqlOperator UNARY_PLUS = SqlStdOperatorTable.UNARY_PLUS;

    // GROUPING FUNCTIONS
    public static final SqlFunction GROUP_ID = SqlStdOperatorTable.GROUP_ID;
    public static final SqlFunction GROUPING = SqlStdOperatorTable.GROUPING;
    public static final SqlFunction GROUPING_ID = SqlStdOperatorTable.GROUPING_ID;

    // AGGREGATE OPERATORS
    public static final SqlAggFunction SUM = SqlStdOperatorTable.SUM;
    public static final SqlAggFunction SUM0 = SqlStdOperatorTable.SUM0;
    public static final SqlAggFunction COUNT = SqlStdOperatorTable.COUNT;
    public static final SqlAggFunction COLLECT = SqlStdOperatorTable.COLLECT;
    public static final SqlAggFunction MIN = SqlStdOperatorTable.MIN;
    public static final SqlAggFunction MAX = SqlStdOperatorTable.MAX;
    public static final SqlAggFunction AVG = SqlStdOperatorTable.AVG;
    public static final SqlAggFunction STDDEV = SqlStdOperatorTable.STDDEV;
    public static final SqlAggFunction STDDEV_POP = SqlStdOperatorTable.STDDEV_POP;
    public static final SqlAggFunction STDDEV_SAMP = SqlStdOperatorTable.STDDEV_SAMP;
    public static final SqlAggFunction VARIANCE = SqlStdOperatorTable.VARIANCE;
    public static final SqlAggFunction VAR_POP = SqlStdOperatorTable.VAR_POP;
    public static final SqlAggFunction VAR_SAMP = SqlStdOperatorTable.VAR_SAMP;
    public static final SqlAggFunction SINGLE_VALUE = SqlStdOperatorTable.SINGLE_VALUE;
    public static final SqlAggFunction APPROX_COUNT_DISTINCT =
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT;

    // ARRAY OPERATORS
    public static final SqlOperator ARRAY_VALUE_CONSTRUCTOR = new SqlArrayConstructor();
    public static final SqlOperator ELEMENT = SqlStdOperatorTable.ELEMENT;

    // MAP OPERATORS
    public static final SqlOperator MAP_VALUE_CONSTRUCTOR = new SqlMapConstructor();

    // ARRAY MAP SHARED OPERATORS
    public static final SqlOperator ITEM = SqlStdOperatorTable.ITEM;
    public static final SqlOperator CARDINALITY = SqlStdOperatorTable.CARDINALITY;

    // SPECIAL OPERATORS
    public static final SqlOperator MULTISET_VALUE = SqlStdOperatorTable.MULTISET_VALUE;
    public static final SqlOperator ROW = SqlStdOperatorTable.ROW;
    public static final SqlOperator OVERLAPS = SqlStdOperatorTable.OVERLAPS;
    public static final SqlOperator LITERAL_CHAIN = SqlStdOperatorTable.LITERAL_CHAIN;
    public static final SqlOperator BETWEEN = SqlStdOperatorTable.BETWEEN;
    public static final SqlOperator SYMMETRIC_BETWEEN = SqlStdOperatorTable.SYMMETRIC_BETWEEN;
    public static final SqlOperator NOT_BETWEEN = SqlStdOperatorTable.NOT_BETWEEN;
    public static final SqlOperator SYMMETRIC_NOT_BETWEEN =
            SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN;
    public static final SqlOperator NOT_LIKE = SqlStdOperatorTable.NOT_LIKE;
    public static final SqlOperator LIKE = SqlStdOperatorTable.LIKE;
    public static final SqlOperator NOT_SIMILAR_TO = SqlStdOperatorTable.NOT_SIMILAR_TO;
    public static final SqlOperator SIMILAR_TO = SqlStdOperatorTable.SIMILAR_TO;
    public static final SqlOperator CASE = SqlStdOperatorTable.CASE;
    public static final SqlOperator REINTERPRET = SqlStdOperatorTable.REINTERPRET;
    public static final SqlOperator EXTRACT = SqlStdOperatorTable.EXTRACT;
    public static final SqlOperator IN = SqlStdOperatorTable.IN;
    public static final SqlOperator SEARCH = SqlStdOperatorTable.SEARCH;
    public static final SqlOperator NOT_IN = SqlStdOperatorTable.NOT_IN;

    // FUNCTIONS
    public static final SqlFunction OVERLAY = SqlStdOperatorTable.OVERLAY;
    public static final SqlFunction TRIM = SqlStdOperatorTable.TRIM;
    public static final SqlFunction POSITION = SqlStdOperatorTable.POSITION;
    public static final SqlFunction CHAR_LENGTH = SqlStdOperatorTable.CHAR_LENGTH;
    public static final SqlFunction CHARACTER_LENGTH = SqlStdOperatorTable.CHARACTER_LENGTH;
    public static final SqlFunction UPPER = SqlStdOperatorTable.UPPER;
    public static final SqlFunction LOWER = SqlStdOperatorTable.LOWER;
    public static final SqlFunction INITCAP = SqlStdOperatorTable.INITCAP;
    public static final SqlFunction POWER = SqlStdOperatorTable.POWER;
    public static final SqlFunction SQRT = SqlStdOperatorTable.SQRT;
    public static final SqlFunction MOD = SqlStdOperatorTable.MOD;
    public static final SqlFunction LN = SqlStdOperatorTable.LN;
    public static final SqlFunction LOG10 = SqlStdOperatorTable.LOG10;
    public static final SqlFunction ABS = SqlStdOperatorTable.ABS;
    public static final SqlFunction EXP = SqlStdOperatorTable.EXP;
    public static final SqlFunction NULLIF = SqlStdOperatorTable.NULLIF;
    public static final SqlFunction FLOOR = SqlStdOperatorTable.FLOOR;
    public static final SqlFunction CEIL = SqlStdOperatorTable.CEIL;
    public static final SqlFunction CAST = SqlStdOperatorTable.CAST;
    public static final SqlOperator SCALAR_QUERY = SqlStdOperatorTable.SCALAR_QUERY;
    public static final SqlOperator EXISTS = SqlStdOperatorTable.EXISTS;
    public static final SqlFunction SIN = SqlStdOperatorTable.SIN;
    public static final SqlFunction COS = SqlStdOperatorTable.COS;
    public static final SqlFunction TAN = SqlStdOperatorTable.TAN;
    public static final SqlFunction COT = SqlStdOperatorTable.COT;
    public static final SqlFunction ASIN = SqlStdOperatorTable.ASIN;
    public static final SqlFunction ACOS = SqlStdOperatorTable.ACOS;
    public static final SqlFunction ATAN = SqlStdOperatorTable.ATAN;
    public static final SqlFunction ATAN2 = SqlStdOperatorTable.ATAN2;
    public static final SqlFunction DEGREES = SqlStdOperatorTable.DEGREES;
    public static final SqlFunction RADIANS = SqlStdOperatorTable.RADIANS;
    public static final SqlFunction SIGN = SqlStdOperatorTable.SIGN;
    public static final SqlFunction PI = SqlStdOperatorTable.PI;

    // TIME FUNCTIONS
    public static final SqlFunction YEAR = SqlStdOperatorTable.YEAR;
    public static final SqlFunction QUARTER = SqlStdOperatorTable.QUARTER;
    public static final SqlFunction MONTH = SqlStdOperatorTable.MONTH;
    public static final SqlFunction WEEK = SqlStdOperatorTable.WEEK;
    public static final SqlFunction HOUR = SqlStdOperatorTable.HOUR;
    public static final SqlFunction MINUTE = SqlStdOperatorTable.MINUTE;
    public static final SqlFunction SECOND = SqlStdOperatorTable.SECOND;
    public static final SqlFunction DAYOFYEAR = SqlStdOperatorTable.DAYOFYEAR;
    public static final SqlFunction DAYOFMONTH = SqlStdOperatorTable.DAYOFMONTH;
    public static final SqlFunction DAYOFWEEK = SqlStdOperatorTable.DAYOFWEEK;
    public static final SqlFunction TIMESTAMP_ADD = SqlStdOperatorTable.TIMESTAMP_ADD;
    public static final SqlFunction TIMESTAMP_DIFF = SqlStdOperatorTable.TIMESTAMP_DIFF;

    // MATCH_RECOGNIZE
    public static final SqlFunction FIRST = SqlStdOperatorTable.FIRST;
    public static final SqlFunction LAST = SqlStdOperatorTable.LAST;
    public static final SqlFunction PREV = SqlStdOperatorTable.PREV;
    public static final SqlFunction NEXT = SqlStdOperatorTable.NEXT;
    public static final SqlPrefixOperator SKIP_TO_FIRST = SqlMatchRecognize.SKIP_TO_FIRST;
    public static final SqlPrefixOperator SKIP_TO_LAST = SqlMatchRecognize.SKIP_TO_LAST;
    public static final SqlFunction CLASSIFIER = SqlStdOperatorTable.CLASSIFIER;
    public static final SqlOperator FINAL = SqlStdOperatorTable.FINAL;
    public static final SqlOperator RUNNING = SqlStdOperatorTable.RUNNING;

    // OVER FUNCTIONS
    public static final SqlAggFunction RANK = SqlStdOperatorTable.RANK;
    public static final SqlAggFunction DENSE_RANK = SqlStdOperatorTable.DENSE_RANK;
    public static final SqlAggFunction ROW_NUMBER = SqlStdOperatorTable.ROW_NUMBER;
    public static final SqlAggFunction CUME_DIST = SqlStdOperatorTable.CUME_DIST;
    public static final SqlAggFunction PERCENT_RANK = SqlStdOperatorTable.PERCENT_RANK;
    public static final SqlAggFunction NTILE = SqlStdOperatorTable.NTILE;
    public static final SqlAggFunction LEAD = SqlStdOperatorTable.LEAD;
    public static final SqlAggFunction LAG = SqlStdOperatorTable.LAG;

    // JSON FUNCTIONS
    public static final SqlFunction JSON_EXISTS = SqlStdOperatorTable.JSON_EXISTS;
    public static final SqlFunction JSON_VALUE = new SqlJsonValueFunctionWrapper("JSON_VALUE");
    public static final SqlFunction JSON_QUERY = new SqlJsonQueryFunctionWrapper();
    public static final SqlFunction JSON_OBJECT = new SqlJsonObjectFunctionWrapper();
    public static final SqlAggFunction JSON_OBJECTAGG_NULL_ON_NULL =
            SqlStdOperatorTable.JSON_OBJECTAGG;
    public static final SqlAggFunction JSON_OBJECTAGG_ABSENT_ON_NULL =
            SqlStdOperatorTable.JSON_OBJECTAGG.with(SqlJsonConstructorNullClause.ABSENT_ON_NULL);
    public static final SqlFunction JSON_ARRAY = new SqlJsonArrayFunctionWrapper();
    public static final SqlAggFunction JSON_ARRAYAGG_NULL_ON_NULL =
            SqlStdOperatorTable.JSON_ARRAYAGG.with(SqlJsonConstructorNullClause.NULL_ON_NULL);
    public static final SqlAggFunction JSON_ARRAYAGG_ABSENT_ON_NULL =
            SqlStdOperatorTable.JSON_ARRAYAGG;
    public static final SqlPostfixOperator IS_JSON_VALUE = SqlStdOperatorTable.IS_JSON_VALUE;
    public static final SqlPostfixOperator IS_JSON_OBJECT = SqlStdOperatorTable.IS_JSON_OBJECT;
    public static final SqlPostfixOperator IS_JSON_ARRAY = SqlStdOperatorTable.IS_JSON_ARRAY;
    public static final SqlPostfixOperator IS_JSON_SCALAR = SqlStdOperatorTable.IS_JSON_SCALAR;
    public static final SqlPostfixOperator IS_NOT_JSON_VALUE =
            SqlStdOperatorTable.IS_NOT_JSON_VALUE;
    public static final SqlPostfixOperator IS_NOT_JSON_OBJECT =
            SqlStdOperatorTable.IS_NOT_JSON_OBJECT;
    public static final SqlPostfixOperator IS_NOT_JSON_ARRAY =
            SqlStdOperatorTable.IS_NOT_JSON_ARRAY;
    public static final SqlPostfixOperator IS_NOT_JSON_SCALAR =
            SqlStdOperatorTable.IS_NOT_JSON_SCALAR;

    // WINDOW TABLE FUNCTIONS
    // use the definitions in Flink, because we have different return types
    // and special check on the time attribute.
    // SESSION is not supported yet, because Calcite doesn't support PARTITION BY clause in TVF
    public static final SqlOperator DESCRIPTOR = new SqlDescriptorOperator();
    public static final SqlFunction TUMBLE = new SqlTumbleTableFunction();
    public static final SqlFunction HOP = new SqlHopTableFunction();
    public static final SqlFunction CUMULATE = new SqlCumulateTableFunction();

    // Catalog Functions
    public static final SqlFunction CURRENT_DATABASE =
            BuiltInSqlFunction.newBuilder()
                    .name("CURRENT_DATABASE")
                    .returnType(VARCHAR_NOT_NULL)
                    .operandTypeChecker(OperandTypes.NILADIC)
                    .notDeterministic()
                    .build();
}
