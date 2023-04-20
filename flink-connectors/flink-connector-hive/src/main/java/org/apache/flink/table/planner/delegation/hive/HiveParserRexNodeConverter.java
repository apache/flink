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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFArrayAccessStructField;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFToDecimal;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserExprNodeDescUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserExprNodeSubQueryDesc;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserIntervalDayTime;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserIntervalYearMonth;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserRowResolver;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSqlFunctionConverter;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserTypeConverter;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseNumeric;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Counterpart of Hive's org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter.
 * And adapted to our needs.
 */
public class HiveParserRexNodeConverter {

    private static final Class genericUDFBaseBinaryClz =
            HiveReflectionUtils.tryGetClass(
                    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseBinary");

    private static final BigInteger MIN_LONG_BI = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger MAX_LONG_BI = BigInteger.valueOf(Long.MAX_VALUE);

    private final RelOptCluster cluster;
    private final List<InputCtx> inputCtxs;
    private final boolean flattenExpr;
    private final SqlFunctionConverter funcConverter;

    // outerRR belongs to outer query and is required to resolve correlated references
    private final HiveParserRowResolver outerRR;
    private final Map<String, Integer> outerNameToPos;
    private int correlatedId;

    // subqueries will need outer query's row resolver
    public HiveParserRexNodeConverter(
            RelOptCluster cluster,
            RelDataType inpDataType,
            Map<String, Integer> outerNameToPos,
            Map<String, Integer> nameToPos,
            HiveParserRowResolver hiveRR,
            HiveParserRowResolver outerRR,
            int offset,
            boolean flattenExpr,
            int correlatedId,
            SqlFunctionConverter funcConverter) {
        this.cluster = cluster;
        this.inputCtxs =
                Collections.singletonList(new InputCtx(inpDataType, nameToPos, hiveRR, offset));
        this.flattenExpr = flattenExpr;
        this.outerRR = outerRR;
        this.outerNameToPos = outerNameToPos;
        this.correlatedId = correlatedId;
        this.funcConverter = funcConverter;
    }

    public HiveParserRexNodeConverter(
            RelOptCluster cluster,
            RelDataType inpDataType,
            Map<String, Integer> nameToPosMap,
            int offset,
            boolean flattenExpr,
            SqlFunctionConverter funcConverter) {
        this.cluster = cluster;
        this.inputCtxs =
                Collections.singletonList(new InputCtx(inpDataType, nameToPosMap, null, offset));
        this.flattenExpr = flattenExpr;
        this.outerRR = null;
        this.outerNameToPos = null;
        this.funcConverter = funcConverter;
    }

    private HiveParserRexNodeConverter(
            RelOptCluster cluster,
            List<InputCtx> inpCtxLst,
            boolean flattenExpr,
            SqlFunctionConverter funcConverter) {
        this.cluster = cluster;
        this.inputCtxs = Collections.unmodifiableList(new ArrayList<>(inpCtxLst));
        this.flattenExpr = flattenExpr;
        this.outerRR = null;
        this.outerNameToPos = null;
        this.funcConverter = funcConverter;
    }

    public static RexNode convert(
            RelOptCluster cluster,
            ExprNodeDesc joinCondnExprNode,
            List<RelNode> inputRels,
            LinkedHashMap<RelNode, HiveParserRowResolver> relToHiveRR,
            Map<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap,
            boolean flattenExpr,
            SqlFunctionConverter funcConverter)
            throws SemanticException {
        List<InputCtx> inputCtxLst = new ArrayList<>();

        int offSet = 0;
        for (RelNode r : inputRels) {
            inputCtxLst.add(
                    new InputCtx(
                            r.getRowType(),
                            relToHiveColNameCalcitePosMap.get(r),
                            relToHiveRR.get(r),
                            offSet));
            offSet += r.getRowType().getFieldCount();
        }

        return (new HiveParserRexNodeConverter(cluster, inputCtxLst, flattenExpr, funcConverter))
                .convert(joinCondnExprNode);
    }

    public RexNode convert(ExprNodeDesc expr) throws SemanticException {
        if (expr instanceof ExprNodeGenericFuncDesc) {
            return convertGenericFunc((ExprNodeGenericFuncDesc) expr);
        } else if (expr instanceof ExprNodeConstantDesc) {
            return convertConstant((ExprNodeConstantDesc) expr, cluster);
        } else if (expr instanceof ExprNodeColumnDesc) {
            return convertColumn((ExprNodeColumnDesc) expr);
        } else if (expr instanceof ExprNodeFieldDesc) {
            return convertField((ExprNodeFieldDesc) expr);
        } else if (expr instanceof HiveParserExprNodeSubQueryDesc) {
            return convertSubQuery((HiveParserExprNodeSubQueryDesc) expr);
        } else if (expr instanceof SqlOperatorExprNodeDesc) {
            return convertSqlOperator((SqlOperatorExprNodeDesc) expr);
        } else {
            throw new RuntimeException("Unsupported Expression");
        }
    }

    private RexNode convertSqlOperator(SqlOperatorExprNodeDesc desc) throws SemanticException {
        List<RexNode> operands = new ArrayList<>(desc.getChildren().size());
        for (ExprNodeDesc child : desc.getChildren()) {
            operands.add(convert(child));
        }
        return cluster.getRexBuilder().makeCall(desc.getSqlOperator(), operands);
    }

    private RexNode convertField(final ExprNodeFieldDesc fieldDesc) throws SemanticException {
        RexNode rexNode = convert(fieldDesc.getDesc());
        if (rexNode.getType().isStruct()) {
            // regular case of accessing nested field in a column
            return cluster.getRexBuilder()
                    .makeFieldAccess(rexNode, fieldDesc.getFieldName(), false);
        } else {
            if (fieldDesc.getIsList()) {
                // it's for accessing the value of a field for the struct data contained in a list.
                // to support such case, convert it to call 'flink_hive_array_access_struct_field'
                // function implemented in
                // org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFArrayFieldAccess

                // get the first parameter's data type for 'flink_hive_array_access_struct_field'
                RelDataType arrayDataType =
                        HiveParserTypeConverter.convert(
                                fieldDesc.getDesc().getTypeInfo(), cluster.getTypeFactory());
                // get the second parameter for 'flink_hive_array_access_struct_field', it's always
                // a string literal valued the field's name
                RexNode accessedField =
                        cluster.getRexBuilder().makeLiteral(fieldDesc.getFieldName());
                RelDataType accessFieldType =
                        HiveParserTypeConverter.convert(
                                TypeInfoFactory.stringTypeInfo, cluster.getTypeFactory());
                RelDataType retType =
                        HiveParserTypeConverter.convert(
                                fieldDesc.getTypeInfo(), cluster.getTypeFactory());
                SqlOperator calciteOp =
                        HiveParserSqlFunctionConverter.getCalciteFn(
                                HiveGenericUDFArrayAccessStructField.NAME,
                                Arrays.asList(arrayDataType, accessFieldType),
                                retType,
                                false,
                                funcConverter);

                return cluster.getRexBuilder().makeCall(calciteOp, rexNode, accessedField);
            } else {
                // This may happen for schema-less tables, where columns are dynamically
                // supplied by serdes.
                throw new SemanticException(
                        "Unexpected rexnode : " + rexNode.getClass().getCanonicalName());
            }
        }
    }

    private RexNode convertColumn(ExprNodeColumnDesc col) throws SemanticException {
        // if this is co-rrelated we need to make RexCorrelVariable(with id and type)
        // id and type should be retrieved from outerRR
        InputCtx ic = getInputCtx(col);
        if (ic == null) {
            // we have correlated column, build data type from outer rr
            RelDataType rowType = HiveParserTypeConverter.getType(cluster, this.outerRR, null);
            if (this.outerNameToPos.get(col.getColumn()) == null) {
                throw new SemanticException("Invalid column name " + col.getColumn());
            }

            int pos = this.outerNameToPos.get(col.getColumn());
            CorrelationId colCorr = new CorrelationId(this.correlatedId);
            RexNode corExpr = cluster.getRexBuilder().makeCorrel(rowType, colCorr);
            return cluster.getRexBuilder().makeFieldAccess(corExpr, pos);
        }
        int pos = ic.hiveNameToPosMap.get(col.getColumn());
        return cluster.getRexBuilder()
                .makeInputRef(
                        ic.calciteInpDataType.getFieldList().get(pos).getType(),
                        pos + ic.offsetInCalciteSchema);
    }

    public static RexNode convertConstant(ExprNodeConstantDesc literal, RelOptCluster cluster)
            throws SemanticException {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
        PrimitiveTypeInfo hiveType = (PrimitiveTypeInfo) literal.getTypeInfo();
        RelDataType calciteDataType = HiveParserTypeConverter.convert(hiveType, dtFactory);

        PrimitiveObjectInspector.PrimitiveCategory hiveTypeCategory =
                hiveType.getPrimitiveCategory();

        ConstantObjectInspector coi = literal.getWritableObjectInspector();
        Object value =
                ObjectInspectorUtils.copyToStandardJavaObject(coi.getWritableConstantValue(), coi);

        RexNode calciteLiteral;
        HiveShim hiveShim = HiveParserUtils.getSessionHiveShim();
        // If value is null, return a null literal directly
        if (value == null) {
            return rexBuilder.makeNullLiteral(calciteDataType);
        }
        // TODO: Verify if we need to use ConstantObjectInspector to unwrap data
        switch (hiveTypeCategory) {
            case BOOLEAN:
                calciteLiteral = rexBuilder.makeLiteral((Boolean) value);
                break;
            case BYTE:
                calciteLiteral =
                        rexBuilder.makeExactLiteral(new BigDecimal((Byte) value), calciteDataType);
                break;
            case SHORT:
                calciteLiteral =
                        rexBuilder.makeExactLiteral(new BigDecimal((Short) value), calciteDataType);
                break;
            case INT:
                calciteLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Integer) value));
                break;
            case LONG:
                calciteLiteral = rexBuilder.makeBigintLiteral(new BigDecimal((Long) value));
                break;
                // TODO: is Decimal an exact numeric or approximate numeric?
            case DECIMAL:
                if (value instanceof HiveDecimal) {
                    value = ((HiveDecimal) value).bigDecimalValue();
                } else if (value instanceof Decimal128) {
                    value = ((Decimal128) value).toBigDecimal();
                }
                if (value == null) {
                    // We have found an invalid decimal value while enforcing precision and scale.
                    // Ideally, we would
                    // replace it with null here, which is what Hive does. However, we need to plumb
                    // this thru up
                    // somehow, because otherwise having different expression type in AST causes the
                    // plan generation
                    // to fail after CBO, probably due to some residual state in SA/QB.
                    // For now, we will not run CBO in the presence of invalid decimal literals.
                    throw new SemanticException(
                            "Expression " + literal.getExprString() + " is not a valid decimal");
                    // TODO: return createNullLiteral(literal);
                }
                BigDecimal bd = (BigDecimal) value;
                BigInteger unscaled = bd.unscaledValue();
                if (unscaled.compareTo(MIN_LONG_BI) >= 0 && unscaled.compareTo(MAX_LONG_BI) <= 0) {
                    calciteLiteral = rexBuilder.makeExactLiteral(bd);
                } else {
                    // CBO doesn't support unlimited precision decimals. In practice, this
                    // will work...
                    // An alternative would be to throw CboSemanticException and fall back
                    // to no CBO.
                    RelDataType relType =
                            cluster.getTypeFactory()
                                    .createSqlType(
                                            SqlTypeName.DECIMAL,
                                            unscaled.toString().length(),
                                            bd.scale());
                    calciteLiteral = rexBuilder.makeExactLiteral(bd, relType);
                }
                break;
            case FLOAT:
                calciteLiteral =
                        rexBuilder.makeApproxLiteral(
                                new BigDecimal(Float.toString((Float) value)), calciteDataType);
                break;
            case DOUBLE:
                // TODO: The best solution is to support NaN in expression reduction.
                if (Double.isNaN((Double) value)) {
                    throw new SemanticException("NaN");
                }
                calciteLiteral =
                        rexBuilder.makeApproxLiteral(
                                new BigDecimal(Double.toString((Double) value)), calciteDataType);
                break;
            case CHAR:
                if (value instanceof HiveChar) {
                    value = ((HiveChar) value).getValue();
                }
                calciteLiteral = rexBuilder.makeCharLiteral(asUnicodeString((String) value));
                break;
            case VARCHAR:
                if (value instanceof HiveVarchar) {
                    value = ((HiveVarchar) value).getValue();
                }
                calciteLiteral = rexBuilder.makeCharLiteral(asUnicodeString((String) value));
                break;
            case STRING:
                Object constantDescVal = literal.getValue();
                constantDescVal =
                        constantDescVal instanceof NlsString
                                ? constantDescVal
                                : asUnicodeString((String) value);
                // calcite treat string literal as char type, we should treat it as string just like
                // hive
                RelDataType type = HiveParserTypeConverter.convert(hiveType, dtFactory);
                // if we get here, the value is not null
                type = dtFactory.createTypeWithNullability(type, false);
                calciteLiteral = rexBuilder.makeLiteral(constantDescVal, type, true);
                break;
            case DATE:
                LocalDate localDate = HiveParserUtils.getSessionHiveShim().toFlinkDate(value);
                DateString dateString =
                        new DateString(
                                localDate.getYear(),
                                localDate.getMonthValue(),
                                localDate.getDayOfMonth());
                calciteLiteral = rexBuilder.makeDateLiteral(dateString);
                break;
            case TIMESTAMP:
                TimestampString timestampString;
                if (value instanceof Calendar) {
                    timestampString = TimestampString.fromCalendarFields((Calendar) value);
                } else {
                    LocalDateTime localDateTime =
                            HiveParserUtils.getSessionHiveShim().toFlinkTimestamp(value);
                    timestampString =
                            new TimestampString(
                                    localDateTime.getYear(),
                                    localDateTime.getMonthValue(),
                                    localDateTime.getDayOfMonth(),
                                    localDateTime.getHour(),
                                    localDateTime.getMinute(),
                                    localDateTime.getSecond());
                    timestampString = timestampString.withNanos(localDateTime.getNano());
                }
                // hive always treats timestamp with precision 9
                calciteLiteral = rexBuilder.makeTimestampLiteral(timestampString, 9);
                break;
            case VOID:
                calciteLiteral =
                        cluster.getRexBuilder()
                                .makeLiteral(null, dtFactory.createSqlType(SqlTypeName.NULL), true);
                break;
            case BINARY:
            case UNKNOWN:
            default:
                if (hiveShim.isIntervalYearMonthType(hiveTypeCategory)) {
                    // Calcite year-month literal value is months as BigDecimal
                    BigDecimal totalMonths;
                    if (value instanceof HiveParserIntervalYearMonth) {
                        totalMonths =
                                BigDecimal.valueOf(
                                        ((HiveParserIntervalYearMonth) value).getTotalMonths());
                    } else if (value instanceof HiveIntervalYearMonth) {
                        totalMonths =
                                BigDecimal.valueOf(
                                        ((HiveIntervalYearMonth) value).getTotalMonths());
                    } else {
                        throw new SemanticException(
                                String.format(
                                        "Unexpected class %s for Hive's interval day time type",
                                        value.getClass().getName()));
                    }
                    calciteLiteral =
                            rexBuilder.makeIntervalLiteral(
                                    totalMonths,
                                    new SqlIntervalQualifier(
                                            TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
                } else if (hiveShim.isIntervalDayTimeType(hiveTypeCategory)) {
                    // Calcite day-time interval is millis value as BigDecimal
                    // Seconds converted to millis
                    BigDecimal secsValueBd;
                    // Nanos converted to millis
                    BigDecimal nanosValueBd;
                    if (value instanceof HiveParserIntervalDayTime) {
                        secsValueBd =
                                BigDecimal.valueOf(
                                        ((HiveParserIntervalDayTime) value).getTotalSeconds()
                                                * 1000);
                        nanosValueBd =
                                BigDecimal.valueOf(
                                        ((HiveParserIntervalDayTime) value).getNanos(), 6);
                    } else if (value instanceof HiveIntervalDayTime) {
                        secsValueBd =
                                BigDecimal.valueOf(
                                        ((HiveIntervalDayTime) value).getTotalSeconds() * 1000);
                        nanosValueBd =
                                BigDecimal.valueOf(((HiveIntervalDayTime) value).getNanos(), 6);
                    } else {
                        throw new SemanticException(
                                String.format(
                                        "Unexpected class %s for Hive's interval day time type.",
                                        value.getClass().getName()));
                    }
                    // Nanos converted to millis
                    calciteLiteral =
                            rexBuilder.makeIntervalLiteral(
                                    secsValueBd.add(nanosValueBd),
                                    new SqlIntervalQualifier(
                                            TimeUnit.MILLISECOND, null, new SqlParserPos(1, 1)));
                } else {
                    throw new RuntimeException("UnSupported Literal type " + hiveTypeCategory);
                }
        }

        return calciteLiteral;
    }

    private RexNode convertGenericFunc(ExprNodeGenericFuncDesc func) throws SemanticException {
        ExprNodeDesc tmpExprNode;
        RexNode tmpRN;

        List<RexNode> childRexNodeLst = new ArrayList<>();
        List<RelDataType> argTypes = new ArrayList<>();

        // TODO: 1) Expand to other functions as needed 2) What about types other than primitive.
        TypeInfo tgtDT = null;
        GenericUDF tgtUdf = func.getGenericUDF();

        if (tgtUdf instanceof GenericUDFIn) {
            return convertIN(func);
        }

        boolean isNumeric = isNumericBinary(func);
        boolean isCompare = !isNumeric && tgtUdf instanceof GenericUDFBaseCompare;
        boolean isWhenCase = tgtUdf instanceof GenericUDFWhen || tgtUdf instanceof GenericUDFCase;
        boolean isTransformableTimeStamp =
                func.getGenericUDF() instanceof GenericUDFUnixTimeStamp
                        && func.getChildren().size() != 0;

        if (isNumeric) {
            tgtDT = func.getTypeInfo();

            assert func.getChildren().size() == 2;
            // TODO: checking 2 children is useless, compare already does that.
        } else if (isCompare && (func.getChildren().size() == 2)) {
            tgtDT =
                    FunctionRegistry.getCommonClassForComparison(
                            func.getChildren().get(0).getTypeInfo(),
                            func.getChildren().get(1).getTypeInfo());
        } else if (isWhenCase) {
            // If it is a CASE or WHEN, we need to check that children do not contain stateful
            // functions as they are not allowed
            if (checkForStatefulFunctions(func.getChildren())) {
                throw new SemanticException("Stateful expressions cannot be used inside of CASE");
            }
        } else if (isTransformableTimeStamp) {
            func =
                    ExprNodeGenericFuncDesc.newInstance(
                            new GenericUDFToUnixTimeStamp(), func.getChildren());
        }

        for (ExprNodeDesc childExpr : func.getChildren()) {
            tmpExprNode = childExpr;
            if (tgtDT != null
                    && TypeInfoUtils.isConversionRequiredForComparison(
                            tgtDT, childExpr.getTypeInfo())) {
                if (isCompare) {
                    // For compare, we will convert requisite children
                    tmpExprNode =
                            HiveASTParseUtils.createConversionCast(
                                    childExpr, (PrimitiveTypeInfo) tgtDT);
                } else if (isNumeric) {
                    // For numeric, we'll do minimum necessary cast - if we cast to the type
                    // of expression, bad things will happen.
                    PrimitiveTypeInfo minArgType =
                            HiveParserExprNodeDescUtils.deriveMinArgumentCast(childExpr, tgtDT);
                    tmpExprNode = HiveASTParseUtils.createConversionCast(childExpr, minArgType);
                } else {
                    throw new AssertionError(
                            "Unexpected " + tgtDT + " - not a numeric op or compare");
                }
            }

            argTypes.add(
                    HiveParserTypeConverter.convert(
                            tmpExprNode.getTypeInfo(), cluster.getTypeFactory()));
            tmpRN = convert(tmpExprNode);
            childRexNodeLst.add(tmpRN);
        }

        // process the function
        RelDataType retType =
                HiveParserTypeConverter.convert(func.getTypeInfo(), cluster.getTypeFactory());

        SqlOperator calciteOp =
                HiveParserSqlFunctionConverter.getCalciteOperator(
                        func.getFuncText(), func.getGenericUDF(), argTypes, retType, funcConverter);
        if (calciteOp.getKind() == SqlKind.CASE) {
            // If it is a case operator, we need to rewrite it
            childRexNodeLst = rewriteCaseChildren(func, childRexNodeLst);
        }
        RexNode expr = cluster.getRexBuilder().makeCall(calciteOp, childRexNodeLst);

        // check whether we need a calcite cast
        RexNode cast = handleExplicitCast(func, childRexNodeLst, ((RexCall) expr).getOperator());
        if (cast != null) {
            expr = cast;
            retType = cast.getType();
        }

        // TODO: Cast Function in Calcite have a bug where it infer type on cast throws
        // an exception
        if (flattenExpr
                && expr instanceof RexCall
                && !(((RexCall) expr).getOperator() instanceof SqlCastFunction)) {
            RexCall call = (RexCall) expr;
            expr =
                    cluster.getRexBuilder()
                            .makeCall(
                                    retType,
                                    call.getOperator(),
                                    RexUtil.flatten(call.getOperands(), call.getOperator()));
        }

        return expr;
    }

    private RexNode convertSubQuery(final HiveParserExprNodeSubQueryDesc subQueryDesc)
            throws SemanticException {
        if (subQueryDesc.getType() == HiveParserExprNodeSubQueryDesc.SubqueryType.IN) {
            /*
             * Check.5.h :: For In and Not In the SubQuery must implicitly or
             * explicitly only contain one select item.
             */
            if (subQueryDesc.getRexSubQuery().getRowType().getFieldCount() > 1) {
                throw new SemanticException(
                        ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
                                "SubQuery can contain only 1 item in Select List."));
            }
            // need implicit type conversion here
            ExprNodeDesc lhsDesc = subQueryDesc.getSubQueryLhs();
            TypeInfo lhsType = lhsDesc.getTypeInfo();
            TypeInfo rhsType =
                    HiveParserTypeConverter.convert(
                            subQueryDesc
                                    .getRexSubQuery()
                                    .getRowType()
                                    .getFieldList()
                                    .get(0)
                                    .getType());
            TypeInfo commonType = FunctionRegistry.getCommonClassForComparison(lhsType, rhsType);
            if (commonType == null) {
                throw new SemanticException(
                        "Cannot do equality join on different types: "
                                + lhsType.getTypeName()
                                + " and "
                                + rhsType.getTypeName());
            }
            // type conversion for LHS
            if (TypeInfoUtils.isConversionRequiredForComparison(lhsType, commonType)) {
                lhsDesc =
                        HiveASTParseUtils.createConversionCast(
                                lhsDesc, (PrimitiveTypeInfo) commonType);
            }
            // type conversion for RHS
            RelNode rhsRel =
                    HiveParserDMLHelper.addTypeConversions(
                            cluster.getRexBuilder(),
                            subQueryDesc.getRexSubQuery(),
                            Collections.singletonList(
                                    HiveParserTypeConverter.convert(
                                            commonType, cluster.getTypeFactory())),
                            Collections.singletonList(commonType),
                            funcConverter,
                            true);
            // create RexNode for LHS
            RexNode lhsRex = convert(lhsDesc);

            // create RexSubQuery node
            return HiveParserUtils.rexSubQueryIn(rhsRel, Collections.singletonList(lhsRex));
        } else if (subQueryDesc.getType() == HiveParserExprNodeSubQueryDesc.SubqueryType.EXISTS) {
            return RexSubQuery.exists(subQueryDesc.getRexSubQuery());
        } else if (subQueryDesc.getType() == HiveParserExprNodeSubQueryDesc.SubqueryType.SCALAR) {
            if (subQueryDesc.getRexSubQuery().getRowType().getFieldCount() > 1) {
                throw new SemanticException(
                        ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
                                "SubQuery can contain only 1 item in Select List."));
            }
            // create RexSubQuery node
            return RexSubQuery.scalar(subQueryDesc.getRexSubQuery());
        } else {
            throw new SemanticException(
                    ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
                            "Invalid subquery: " + subQueryDesc.getType()));
        }
    }

    // converts IN for constant value list, RexSubQuery won't get here
    private RexNode convertIN(ExprNodeGenericFuncDesc func) throws SemanticException {
        List<RexNode> childRexNodes = new ArrayList<>();
        for (ExprNodeDesc childExpr : func.getChildren()) {
            childRexNodes.add(convert(childExpr));
        }
        if (funcConverter.hasOverloadedOp(
                HiveParserIN.INSTANCE, SqlFunctionCategory.USER_DEFINED_FUNCTION)) {
            return cluster.getRexBuilder().makeCall(HiveParserIN.INSTANCE, childRexNodes);
        } else {
            // hive module is not loaded, calcite converts IN using either OR or inline table
            // (LogicalValues), we do the same here but only support OR for now
            RexNode leftKey = childRexNodes.get(0);
            Preconditions.checkState(
                    leftKey instanceof RexInputRef,
                    "Expecting LHS key of IN to be a RexInputRef, actually got " + leftKey);
            final List<RexNode> comparisons = new ArrayList<>();
            for (int i = 1; i < childRexNodes.size(); i++) {
                comparisons.add(
                        cluster.getRexBuilder()
                                .makeCall(
                                        SqlStdOperatorTable.EQUALS, leftKey, childRexNodes.get(i)));
            }
            return RexUtil.composeDisjunction(cluster.getRexBuilder(), comparisons, true);
        }
    }

    private List<RexNode> rewriteCaseChildren(
            ExprNodeGenericFuncDesc func, List<RexNode> childRexNodeLst) throws SemanticException {
        List<RexNode> newChildRexNodeLst = new ArrayList<>();
        if (FunctionRegistry.getNormalizedFunctionName(func.getFuncText()).equals("case")) {
            RexNode firstPred = childRexNodeLst.get(0);
            int length =
                    childRexNodeLst.size() % 2 == 1
                            ? childRexNodeLst.size()
                            : childRexNodeLst.size() - 1;
            for (int i = 1; i < length; i++) {
                if (i % 2 == 1) {
                    // We rewrite it
                    newChildRexNodeLst.add(
                            cluster.getRexBuilder()
                                    .makeCall(
                                            SqlStdOperatorTable.EQUALS,
                                            firstPred,
                                            childRexNodeLst.get(i)));
                } else {
                    newChildRexNodeLst.add(childRexNodeLst.get(i));
                }
            }
            // The else clause
            if (length != childRexNodeLst.size()) {
                newChildRexNodeLst.add(childRexNodeLst.get(childRexNodeLst.size() - 1));
            }
        } else {
            newChildRexNodeLst.addAll(childRexNodeLst);
        }
        // Calcite always needs the else clause to be defined explicitly
        if (newChildRexNodeLst.size() % 2 == 0) {
            newChildRexNodeLst.add(
                    cluster.getRexBuilder()
                            .makeNullLiteral(
                                    newChildRexNodeLst
                                            .get(newChildRexNodeLst.size() - 1)
                                            .getType()));
        }
        return newChildRexNodeLst;
    }

    private boolean castExprUsingUDFBridge(GenericUDF gUDF) {
        boolean castExpr = false;
        if (gUDF instanceof GenericUDFBridge) {
            String udfClassName = ((GenericUDFBridge) gUDF).getUdfClassName();
            if (udfClassName != null) {
                int sp = udfClassName.lastIndexOf('.');
                // TODO: add method to UDFBridge to say if it is a cast func
                if (sp >= 0 & (sp + 1) < udfClassName.length()) {
                    udfClassName = udfClassName.substring(sp + 1);
                    if (udfClassName.equals("UDFToBoolean")
                            || udfClassName.equals("UDFToByte")
                            || udfClassName.equals("UDFToDouble")
                            || udfClassName.equals("UDFToInteger")
                            || udfClassName.equals("UDFToLong")
                            || udfClassName.equals("UDFToShort")
                            || udfClassName.equals("UDFToFloat")
                            || udfClassName.equals("UDFToString")) {
                        castExpr = true;
                    }
                }
            }
        }

        return castExpr;
    }

    private RexNode handleExplicitCast(
            ExprNodeGenericFuncDesc func, List<RexNode> childRexNodeLst, SqlOperator operator)
            throws SemanticException {
        GenericUDF udf = func.getGenericUDF();
        if (isExplicitCast(udf) && childRexNodeLst != null && childRexNodeLst.size() == 1) {
            RelDataType targetType =
                    HiveParserTypeConverter.convert(func.getTypeInfo(), cluster.getTypeFactory());
            if (HiveParserUtils.isFromTimeStampToDecimal(
                    childRexNodeLst.get(0).getType(), targetType)) {
                // special case for cast timestamp to decimal for Flink don't support cast from
                // TIMESTAMP type to NUMERIC type.
                // use custom to_decimal function to cast, which is consistent with Hive.
                SqlOperator castOperator =
                        HiveParserSqlFunctionConverter.getCalciteFn(
                                HiveGenericUDFToDecimal.NAME,
                                Arrays.asList(childRexNodeLst.get(0).getType(), targetType),
                                targetType,
                                false,
                                funcConverter);
                return cluster.getRexBuilder()
                        .makeCall(
                                castOperator,
                                childRexNodeLst.get(0),
                                cluster.getRexBuilder().makeNullLiteral(targetType));
            }

            // we cannot handle SettableUDF at the moment so we call calcite to do the cast in
            // that
            // case, otherwise we use hive functions to achieve better compatibility
            if (udf instanceof SettableUDF
                    || !funcConverter.hasOverloadedOp(
                            operator, SqlFunctionCategory.USER_DEFINED_FUNCTION)) {
                return cluster.getRexBuilder()
                        .makeAbstractCast(
                                HiveParserTypeConverter.convert(
                                        func.getTypeInfo(), cluster.getTypeFactory()),
                                childRexNodeLst.get(0));
            }
        }
        return null;
    }

    private boolean isExplicitCast(GenericUDF udf) {
        return udf instanceof GenericUDFToChar
                || udf instanceof GenericUDFToVarchar
                || udf instanceof GenericUDFToDecimal
                || udf instanceof GenericUDFToDate
                || udf instanceof GenericUDFTimestamp
                || udf instanceof GenericUDFToBinary
                || castExprUsingUDFBridge(udf);
    }

    private InputCtx getInputCtx(ExprNodeColumnDesc col) throws SemanticException {
        InputCtx ctxLookingFor = null;

        if (inputCtxs.size() == 1 && inputCtxs.get(0).hiveRR == null) {
            ctxLookingFor = inputCtxs.get(0);
        } else {
            String tableAlias = col.getTabAlias();
            String colAlias = col.getColumn();
            int noInp = 0;
            for (InputCtx ic : inputCtxs) {
                if (tableAlias == null || ic.hiveRR.hasTableAlias(tableAlias)) {
                    if (ic.hiveRR.getPosition(colAlias) >= 0) {
                        ctxLookingFor = ic;
                        noInp++;
                    }
                }
            }

            if (noInp > 1) {
                throw new RuntimeException("Ambiguous column mapping");
            }
        }

        return ctxLookingFor;
    }

    private static NlsString asUnicodeString(String text) {
        return new NlsString(text, ConversionUtil.NATIVE_UTF16_CHARSET_NAME, SqlCollation.IMPLICIT);
    }

    private static boolean checkForStatefulFunctions(List<ExprNodeDesc> list) {
        for (ExprNodeDesc node : list) {
            if (node instanceof ExprNodeGenericFuncDesc) {
                GenericUDF nodeUDF = ((ExprNodeGenericFuncDesc) node).getGenericUDF();
                // Stateful?
                if (FunctionRegistry.isStateful(nodeUDF)) {
                    return true;
                }
                if (node.getChildren() != null
                        && !node.getChildren().isEmpty()
                        && checkForStatefulFunctions(node.getChildren())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isNumericBinary(ExprNodeGenericFuncDesc func) {
        GenericUDF tgtUdf = func.getGenericUDF();
        boolean rightClz;
        if (genericUDFBaseBinaryClz != null) {
            rightClz = genericUDFBaseBinaryClz.isInstance(tgtUdf);
        } else {
            rightClz =
                    tgtUdf instanceof GenericUDFBaseCompare
                            || tgtUdf instanceof GenericUDFBaseNumeric;
        }
        return rightClz
                && func.getTypeInfo().getCategory() == ObjectInspector.Category.PRIMITIVE
                && (PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP
                        == PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                                ((PrimitiveTypeInfo) func.getTypeInfo()).getPrimitiveCategory()));
    }

    private static class InputCtx {
        private final RelDataType calciteInpDataType;
        private final Map<String, Integer> hiveNameToPosMap;
        private final HiveParserRowResolver hiveRR;
        private final int offsetInCalciteSchema;

        private InputCtx(
                RelDataType calciteInpDataType,
                Map<String, Integer> hiveNameToPosMap,
                HiveParserRowResolver hiveRR,
                int offsetInCalciteSchema) {
            this.calciteInpDataType = calciteInpDataType;
            this.hiveNameToPosMap = hiveNameToPosMap;
            this.hiveRR = hiveRR;
            this.offsetInCalciteSchema = offsetInCalciteSchema;
        }
    }
}
