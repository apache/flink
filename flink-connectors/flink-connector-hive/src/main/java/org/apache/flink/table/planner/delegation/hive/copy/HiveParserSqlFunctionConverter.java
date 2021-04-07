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

import org.apache.flink.table.planner.delegation.hive.HiveParserIN;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPositive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Counterpart of hive's
 * org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter.
 */
public class HiveParserSqlFunctionConverter {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserSqlFunctionConverter.class);

    static final Map<String, SqlOperator> HIVE_TO_CALCITE;
    static final Map<SqlOperator, HiveToken> CALCITE_TO_HIVE_TOKEN;
    static final Map<SqlOperator, String> REVERSE_OPERATOR_MAP;

    static {
        StaticBlockBuilder builder = new StaticBlockBuilder();
        HIVE_TO_CALCITE = Collections.unmodifiableMap(builder.hiveToCalcite);
        CALCITE_TO_HIVE_TOKEN = Collections.unmodifiableMap(builder.calciteToHiveToken);
        REVERSE_OPERATOR_MAP = Collections.unmodifiableMap(builder.reverseOperatorMap);
    }

    public static SqlOperator getCalciteOperator(
            String funcTextName,
            GenericUDF hiveUDF,
            List<RelDataType> calciteArgTypes,
            RelDataType retType)
            throws SemanticException {
        // handle overloaded methods first
        if (hiveUDF instanceof GenericUDFOPNegative) {
            return SqlStdOperatorTable.UNARY_MINUS;
        } else if (hiveUDF instanceof GenericUDFOPPositive) {
            return SqlStdOperatorTable.UNARY_PLUS;
        } // do generic lookup
        String name = null;
        if (StringUtils.isEmpty(funcTextName)) {
            name = getName(hiveUDF); // this should probably never happen, see getName comment
            LOG.warn("The function text was empty, name from annotation is " + name);
        } else {
            // We could just do toLowerCase here and let SA qualify it, but
            // let's be proper...
            name = FunctionRegistry.getNormalizedFunctionName(funcTextName);
        }
        return getCalciteFn(
                name, calciteArgTypes, retType, FunctionRegistry.isDeterministic(hiveUDF));
    }

    // TODO: this is not valid. Function names for built-in UDFs are specified in
    // FunctionRegistry, and only happen to match annotations. For user UDFs, the
    // name is what user specifies at creation time (annotation can be absent,
    // different, or duplicate some other function).
    private static String getName(GenericUDF hiveUDF) {
        String udfName = null;
        if (hiveUDF instanceof GenericUDFBridge) {
            udfName = hiveUDF.getUdfName();
        } else {
            Class<? extends GenericUDF> udfClass = hiveUDF.getClass();
            Description udfAnnotation = udfClass.getAnnotation(Description.class);

            if (udfAnnotation != null) {
                udfName = udfAnnotation.name();
                if (udfName != null) {
                    String[] aliases = udfName.split(",");
                    if (aliases.length > 0) {
                        udfName = aliases[0];
                    }
                }
            }

            if (udfName == null || udfName.isEmpty()) {
                udfName = hiveUDF.getClass().getName();
                int indx = udfName.lastIndexOf(".");
                if (indx >= 0) {
                    indx += 1;
                    udfName = udfName.substring(indx);
                }
            }
        }

        return udfName;
    }

    /** This class is used to build immutable hashmaps in the static block above. */
    private static class StaticBlockBuilder {
        final Map<String, SqlOperator> hiveToCalcite = new HashMap<>();
        final Map<SqlOperator, HiveToken> calciteToHiveToken = new HashMap<>();
        final Map<SqlOperator, String> reverseOperatorMap = new HashMap<>();

        StaticBlockBuilder() {
            registerFunction("+", SqlStdOperatorTable.PLUS, hToken(HiveASTParser.PLUS, "+"));
            registerFunction("-", SqlStdOperatorTable.MINUS, hToken(HiveASTParser.MINUS, "-"));
            registerFunction("*", SqlStdOperatorTable.MULTIPLY, hToken(HiveASTParser.STAR, "*"));
            registerFunction("/", SqlStdOperatorTable.DIVIDE, hToken(HiveASTParser.DIVIDE, "/"));
            registerFunction("%", SqlStdOperatorTable.MOD, hToken(HiveASTParser.Identifier, "%"));
            registerFunction("and", SqlStdOperatorTable.AND, hToken(HiveASTParser.KW_AND, "and"));
            registerFunction("or", SqlStdOperatorTable.OR, hToken(HiveASTParser.KW_OR, "or"));
            registerFunction("=", SqlStdOperatorTable.EQUALS, hToken(HiveASTParser.EQUAL, "="));
            registerDuplicateFunction(
                    "==", SqlStdOperatorTable.EQUALS, hToken(HiveASTParser.EQUAL, "="));
            registerFunction(
                    "<", SqlStdOperatorTable.LESS_THAN, hToken(HiveASTParser.LESSTHAN, "<"));
            registerFunction(
                    "<=",
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    hToken(HiveASTParser.LESSTHANOREQUALTO, "<="));
            registerFunction(
                    ">", SqlStdOperatorTable.GREATER_THAN, hToken(HiveASTParser.GREATERTHAN, ">"));
            registerFunction(
                    ">=",
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    hToken(HiveASTParser.GREATERTHANOREQUALTO, ">="));
            registerFunction("not", SqlStdOperatorTable.NOT, hToken(HiveASTParser.KW_NOT, "not"));
            registerDuplicateFunction(
                    "!", SqlStdOperatorTable.NOT, hToken(HiveASTParser.KW_NOT, "not"));
            registerFunction(
                    "<>", SqlStdOperatorTable.NOT_EQUALS, hToken(HiveASTParser.NOTEQUAL, "<>"));
            registerDuplicateFunction(
                    "!=", SqlStdOperatorTable.NOT_EQUALS, hToken(HiveASTParser.NOTEQUAL, "<>"));
            registerFunction("in", HiveParserIN.INSTANCE, hToken(HiveASTParser.Identifier, "in"));
            registerFunction(
                    "between",
                    HiveParserBetween.INSTANCE,
                    hToken(HiveASTParser.Identifier, "between"));
            registerFunction(
                    "struct", SqlStdOperatorTable.ROW, hToken(HiveASTParser.Identifier, "struct"));
            registerFunction(
                    "isnotnull",
                    SqlStdOperatorTable.IS_NOT_NULL,
                    hToken(HiveASTParser.TOK_ISNOTNULL, "TOK_ISNOTNULL"));
            registerFunction(
                    "isnull",
                    SqlStdOperatorTable.IS_NULL,
                    hToken(HiveASTParser.TOK_ISNULL, "TOK_ISNULL"));
            // let's try removing 'when' for better compatibility
            //			registerFunction("when", SqlStdOperatorTable.CASE, hToken(HiveASTParser.Identifier,
            // "when"));
            // let's try removing 'case' for better compatibility
            //			registerDuplicateFunction("case", SqlStdOperatorTable.CASE,
            // hToken(HiveASTParser.Identifier, "when"));
            // timebased
            registerFunction(
                    "year", HiveParserExtractDate.YEAR, hToken(HiveASTParser.Identifier, "year"));
            registerFunction(
                    "quarter",
                    HiveParserExtractDate.QUARTER,
                    hToken(HiveASTParser.Identifier, "quarter"));
            registerFunction(
                    "month",
                    HiveParserExtractDate.MONTH,
                    hToken(HiveASTParser.Identifier, "month"));
            registerFunction(
                    "weekofyear",
                    HiveParserExtractDate.WEEK,
                    hToken(HiveASTParser.Identifier, "weekofyear"));
            registerFunction(
                    "day", HiveParserExtractDate.DAY, hToken(HiveASTParser.Identifier, "day"));
            registerFunction(
                    "hour", HiveParserExtractDate.HOUR, hToken(HiveASTParser.Identifier, "hour"));
            registerFunction(
                    "minute",
                    HiveParserExtractDate.MINUTE,
                    hToken(HiveASTParser.Identifier, "minute"));
            registerFunction(
                    "second",
                    HiveParserExtractDate.SECOND,
                    hToken(HiveASTParser.Identifier, "second"));
            registerFunction(
                    "floor_year",
                    HiveParserFloorDate.YEAR,
                    hToken(HiveASTParser.Identifier, "floor_year"));
            registerFunction(
                    "floor_quarter",
                    HiveParserFloorDate.QUARTER,
                    hToken(HiveASTParser.Identifier, "floor_quarter"));
            registerFunction(
                    "floor_month",
                    HiveParserFloorDate.MONTH,
                    hToken(HiveASTParser.Identifier, "floor_month"));
            registerFunction(
                    "floor_week",
                    HiveParserFloorDate.WEEK,
                    hToken(HiveASTParser.Identifier, "floor_week"));
            registerFunction(
                    "floor_day",
                    HiveParserFloorDate.DAY,
                    hToken(HiveASTParser.Identifier, "floor_day"));
            registerFunction(
                    "floor_hour",
                    HiveParserFloorDate.HOUR,
                    hToken(HiveASTParser.Identifier, "floor_hour"));
            registerFunction(
                    "floor_minute",
                    HiveParserFloorDate.MINUTE,
                    hToken(HiveASTParser.Identifier, "floor_minute"));
            registerFunction(
                    "floor_second",
                    HiveParserFloorDate.SECOND,
                    hToken(HiveASTParser.Identifier, "floor_second"));
            // support <=>
            registerFunction(
                    "<=>",
                    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    hToken(HiveASTParser.EQUAL_NS, "<=>"));
        }

        private void registerFunction(String name, SqlOperator calciteFn, HiveToken hiveToken) {
            reverseOperatorMap.put(calciteFn, name);
            FunctionInfo hFn;
            try {
                hFn = FunctionRegistry.getFunctionInfo(name);
            } catch (SemanticException e) {
                LOG.warn("Failed to load udf " + name, e);
                hFn = null;
            }
            if (hFn != null) {
                String hFnName = getName(hFn.getGenericUDF());
                hiveToCalcite.put(hFnName, calciteFn);

                if (hiveToken != null) {
                    calciteToHiveToken.put(calciteFn, hiveToken);
                }
            }
        }

        private void registerDuplicateFunction(
                String name, SqlOperator calciteFn, HiveToken hiveToken) {
            hiveToCalcite.put(name, calciteFn);
            if (hiveToken != null) {
                calciteToHiveToken.put(calciteFn, hiveToken);
            }
        }
    }

    private static HiveToken hToken(int type, String text) {
        return new HiveToken(type, text);
    }

    /** UDAF is assumed to be deterministic. */
    private static class CalciteUDAF extends SqlAggFunction implements CanAggregateDistinct {
        private final boolean isDistinct;

        public CalciteUDAF(
                boolean isDistinct,
                String opName,
                SqlIdentifier identifier,
                SqlReturnTypeInference returnTypeInference,
                SqlOperandTypeInference operandTypeInference,
                SqlOperandTypeChecker operandTypeChecker) {
            super(
                    opName,
                    identifier,
                    SqlKind.OTHER_FUNCTION,
                    returnTypeInference,
                    operandTypeInference,
                    operandTypeChecker,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
            this.isDistinct = isDistinct;
        }

        @Override
        public boolean isDistinct() {
            return isDistinct;
        }
    }

    /** CalciteSqlFn. */
    public static class CalciteSqlFn extends SqlFunction {
        private final boolean deterministic;

        public CalciteSqlFn(
                String name,
                SqlIdentifier identifier,
                SqlKind kind,
                SqlReturnTypeInference returnTypeInference,
                SqlOperandTypeInference operandTypeInference,
                SqlOperandTypeChecker operandTypeChecker,
                SqlFunctionCategory category,
                boolean deterministic) {
            super(
                    name,
                    identifier,
                    kind,
                    returnTypeInference,
                    operandTypeInference,
                    operandTypeChecker,
                    category);
            this.deterministic = deterministic;
        }

        @Override
        public boolean isDeterministic() {
            return deterministic;
        }
    }

    private static class CalciteUDFInfo {
        private String udfName;
        // need an identifier if we have a composite name
        private SqlIdentifier identifier;
        private SqlReturnTypeInference returnTypeInference;
        private SqlOperandTypeInference operandTypeInference;
        private SqlOperandTypeChecker operandTypeChecker;
    }

    private static CalciteUDFInfo getUDFInfo(
            String hiveUdfName, List<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
        CalciteUDFInfo udfInfo = new CalciteUDFInfo();
        udfInfo.udfName = hiveUdfName;
        String[] nameParts = hiveUdfName.split("\\.");
        if (nameParts.length > 1) {
            udfInfo.identifier =
                    new SqlIdentifier(
                            Arrays.stream(nameParts).collect(Collectors.toList()),
                            new SqlParserPos(0, 0));
        }
        udfInfo.returnTypeInference = ReturnTypes.explicit(calciteRetType);
        udfInfo.operandTypeInference = InferTypes.explicit(calciteArgTypes);
        List<SqlTypeFamily> typeFamily = new ArrayList<>();
        for (RelDataType argType : calciteArgTypes) {
            typeFamily.add(Util.first(argType.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
        }
        udfInfo.operandTypeChecker = OperandTypes.family(Collections.unmodifiableList(typeFamily));
        return udfInfo;
    }

    public static SqlOperator getCalciteFn(
            String hiveUdfName,
            List<RelDataType> calciteArgTypes,
            RelDataType calciteRetType,
            boolean deterministic) {
        SqlOperator calciteOp;
        CalciteUDFInfo uInf = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);
        switch (hiveUdfName) {
                // Follow hive's rules for type inference as oppose to Calcite's
                // for return type.
                // TODO: Perhaps we should do this for all functions, not just +,-
            case "-":
                calciteOp =
                        new SqlMonotonicBinaryOperator(
                                "-",
                                SqlKind.MINUS,
                                40,
                                true,
                                uInf.returnTypeInference,
                                uInf.operandTypeInference,
                                OperandTypes.MINUS_OPERATOR);
                break;
            case "+":
                calciteOp =
                        new SqlMonotonicBinaryOperator(
                                "+",
                                SqlKind.PLUS,
                                40,
                                true,
                                uInf.returnTypeInference,
                                uInf.operandTypeInference,
                                OperandTypes.PLUS_OPERATOR);
                break;
            default:
                calciteOp = HIVE_TO_CALCITE.get(hiveUdfName);
                if (null == calciteOp) {
                    calciteOp =
                            new CalciteSqlFn(
                                    uInf.udfName,
                                    uInf.identifier,
                                    SqlKind.OTHER_FUNCTION,
                                    uInf.returnTypeInference,
                                    uInf.operandTypeInference,
                                    uInf.operandTypeChecker,
                                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                                    deterministic);
                }
                break;
        }
        return calciteOp;
    }

    public static SqlAggFunction getCalciteAggFn(
            String hiveUdfName,
            boolean isDistinct,
            List<RelDataType> calciteArgTypes,
            RelDataType calciteRetType) {
        SqlAggFunction calciteAggFn = (SqlAggFunction) HIVE_TO_CALCITE.get(hiveUdfName);

        if (calciteAggFn == null) {
            CalciteUDFInfo udfInfo = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);

            switch (hiveUdfName.toLowerCase()) {
                case "sum":
                    calciteAggFn =
                            new HiveParserSqlSumAggFunction(
                                    isDistinct,
                                    udfInfo.returnTypeInference,
                                    udfInfo.operandTypeInference,
                                    udfInfo.operandTypeChecker);
                    break;
                case "count":
                    calciteAggFn =
                            new HiveParserSqlCountAggFunction(
                                    isDistinct,
                                    udfInfo.returnTypeInference,
                                    udfInfo.operandTypeInference,
                                    udfInfo.operandTypeChecker);
                    break;
                case "min":
                    calciteAggFn =
                            new HiveParserSqlMinMaxAggFunction(
                                    udfInfo.returnTypeInference,
                                    udfInfo.operandTypeInference,
                                    udfInfo.operandTypeChecker,
                                    true);
                    break;
                case "max":
                    calciteAggFn =
                            new HiveParserSqlMinMaxAggFunction(
                                    udfInfo.returnTypeInference,
                                    udfInfo.operandTypeInference,
                                    udfInfo.operandTypeChecker,
                                    false);
                    break;
                default:
                    calciteAggFn =
                            new CalciteUDAF(
                                    isDistinct,
                                    udfInfo.udfName,
                                    udfInfo.identifier,
                                    udfInfo.returnTypeInference,
                                    udfInfo.operandTypeInference,
                                    udfInfo.operandTypeChecker);
                    break;
            }
        }
        return calciteAggFn;
    }

    static class HiveToken {
        int type;
        String text;
        String[] args;

        HiveToken(int type, String text, String... args) {
            this.type = type;
            this.text = text;
            this.args = args;
        }
    }

    /** CanAggregateDistinct. */
    public interface CanAggregateDistinct {
        boolean isDistinct();
    }
}
