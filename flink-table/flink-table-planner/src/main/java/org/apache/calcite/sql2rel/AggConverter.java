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
package org.apache.calcite.sql2rel;

import org.apache.flink.table.planner.calcite.FlinkSqlCallBinding;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.AggregatingSelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * FLINK modifications are at lines
 *
 * <ol>
 *   <li>Added in FLINK-34057, FLINK-34058, FLINK-34312: Lines 452 ~ 469
 * </ol>
 */
class AggConverter implements SqlVisitor<Void> {
    private final SqlToRelConverter.Blackboard bb;
    private final Map<String, String> nameMap;

    /** The group-by expressions, in {@link SqlNode} format. */
    final SqlNodeList groupExprs = new SqlNodeList(SqlParserPos.ZERO);

    /** The auxiliary group-by expressions. */
    private final Map<SqlNode, Ord<AuxiliaryConverter>> auxiliaryGroupExprs = new HashMap<>();

    /** Measure expressions, in {@link SqlNode} format. */
    private final SqlNodeList measureExprs = new SqlNodeList(SqlParserPos.ZERO);

    /**
     * Input expressions for the group columns and aggregates, in {@link RexNode} format. The first
     * elements of the list correspond to the elements in {@link #groupExprs}; the remaining
     * elements are for aggregates. The right field of each pair is the name of the expression,
     * where the expressions are simple mappings to input fields.
     */
    final PairList<RexNode, @Nullable String> convertedInputExprs = PairList.of();

    /**
     * Expressions to be evaluated as rows are being placed into the aggregate's hash table. This is
     * when group functions such as TUMBLE cause rows to be expanded.
     */
    final List<AggregateCall> aggCalls = new ArrayList<>();

    private final Map<SqlNode, RexNode> aggMapping = new HashMap<>();
    private final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();
    private final SqlValidator validator;
    private final AggregatingSelectScope scope;

    /** Whether we are directly inside a windowed aggregate. */
    boolean inOver = false;

    /** Creates an AggConverter. */
    private AggConverter(SqlToRelConverter.Blackboard bb, ImmutableMap<String, String> nameMap) {
        this(bb, nameMap, null, null);
    }

    private AggConverter(
            SqlToRelConverter.Blackboard bb,
            ImmutableMap<String, String> nameMap,
            SqlValidator validator,
            AggregatingSelectScope scope) {
        this.bb = bb;
        this.nameMap = nameMap;
        this.validator = validator;
        this.scope = scope;
    }

    /**
     * Creates an AggConverter for a pivot query.
     *
     * @param bb Blackboard
     */
    static AggConverter create(SqlToRelConverter.Blackboard bb) {
        return new AggConverter(bb, ImmutableMap.of());
    }

    /**
     * Creates an AggConverter.
     *
     * <p>The {@code aggregatingSelectScope} parameter provides enough context to name aggregate
     * calls which are top-level select list items.
     *
     * @param bb Blackboard
     * @param scope Scope of a SELECT that has a GROUP BY
     */
    static AggConverter create(
            SqlToRelConverter.Blackboard bb, AggregatingSelectScope scope, SqlValidator validator) {
        // Collect all expressions used in the select list so that aggregate
        // calls can be named correctly.
        final Map<String, String> nameMap = new HashMap<>();
        Ord.forEach(
                scope.getNode().getSelectList(),
                (selectItem, i) -> {
                    final String name;
                    if (SqlUtil.isCallTo(selectItem, SqlStdOperatorTable.AS)) {
                        final SqlCall call = (SqlCall) selectItem;
                        selectItem = call.operand(0);
                        name = call.operand(1).toString();
                    } else {
                        name = SqlValidatorUtil.alias(selectItem, i);
                    }
                    nameMap.put(selectItem.toString(), name);
                });

        final AggregatingSelectScope.Resolved resolved = scope.resolved.get();
        return new AggConverter(bb, ImmutableMap.copyOf(nameMap), validator, scope) {
            @Override
            AggregatingSelectScope.Resolved getResolved() {
                return resolved;
            }
        };
    }

    int addGroupExpr(SqlNode expr) {
        int ref = lookupGroupExpr(expr);
        if (ref >= 0) {
            return ref;
        }
        final int index = groupExprs.size();
        groupExprs.add(expr);
        String name = nameMap.get(expr.toString());
        RexNode convExpr = bb.convertExpression(expr);
        addExpr(convExpr, name);

        if (expr instanceof SqlCall) {
            SqlCall call = (SqlCall) expr;
            SqlStdOperatorTable.convertGroupToAuxiliaryCalls(
                    call, (node, converter) -> addAuxiliaryGroupExpr(node, index, converter));
        }

        return index;
    }

    void addAuxiliaryGroupExpr(SqlNode node, int index, AuxiliaryConverter converter) {
        for (SqlNode node2 : auxiliaryGroupExprs.keySet()) {
            if (node2.equalsDeep(node, Litmus.IGNORE)) {
                return;
            }
        }
        auxiliaryGroupExprs.put(node, Ord.of(index, converter));
    }

    boolean addMeasureExpr(SqlNode expr) {
        if (isMeasureExpr(expr)) {
            return false; // already present
        }
        measureExprs.add(expr);
        String name = nameMap.get(expr.toString());
        RexNode convExpr = bb.convertExpression(expr);
        addExpr(convExpr, name);
        return true;
    }

    /**
     * Adds an expression, deducing an appropriate name if possible.
     *
     * @param expr Expression
     * @param name Suggested name
     */
    private void addExpr(RexNode expr, @Nullable String name) {
        if (name == null && expr instanceof RexInputRef) {
            final int i = ((RexInputRef) expr).getIndex();
            name = bb.root().getRowType().getFieldList().get(i).getName();
        }
        if (convertedInputExprs.rightList().contains(name)) {
            // In case like 'SELECT ... GROUP BY x, y, x', don't add
            // name 'x' twice.
            name = null;
        }
        convertedInputExprs.add(expr, name);
    }

    @Override
    public Void visit(SqlIdentifier id) {
        return null;
    }

    @Override
    public Void visit(SqlNodeList nodeList) {
        nodeList.forEach(this::visitNode);
        return null;
    }

    @Override
    public Void visit(SqlLiteral lit) {
        return null;
    }

    @Override
    public Void visit(SqlDataTypeSpec type) {
        return null;
    }

    @Override
    public Void visit(SqlDynamicParam param) {
        return null;
    }

    @Override
    public Void visit(SqlIntervalQualifier intervalQualifier) {
        return null;
    }

    @Override
    public Void visit(SqlCall call) {
        switch (call.getKind()) {
            case FILTER:
            case IGNORE_NULLS:
            case RESPECT_NULLS:
            case WITHIN_DISTINCT:
            case WITHIN_GROUP:
                translateAgg(call);
                return null;
            case SELECT:
                // rchen 2006-10-17:
                // for now do not detect aggregates in sub-queries.
                return null;
            default:
                break;
        }
        final boolean prevInOver = inOver;
        // Ignore window aggregates and ranking functions (associated with OVER
        // operator). However, do not ignore nested window aggregates.
        if (call.getOperator().getKind() == SqlKind.OVER) {
            // Track aggregate nesting levels only within an OVER operator.
            List<SqlNode> operandList = call.getOperandList();
            assert operandList.size() == 2;

            // Ignore the top level window aggregates and ranking functions
            // positioned as the first operand of a OVER operator
            inOver = true;
            operandList.get(0).accept(this);

            // Normal translation for the second operand of a OVER operator
            inOver = false;
            operandList.get(1).accept(this);
            return null;
        }

        // Do not translate the top level window aggregate. Only do so for
        // nested aggregates, if present
        if (call.getOperator().isAggregator()) {
            if (inOver) {
                // Add the parent aggregate level before visiting its children
                inOver = false;
            } else {
                // We're beyond the one ignored level
                translateAgg(call);
                return null;
            }
        }
        for (SqlNode operand : call.getOperandList()) {
            // Operands are occasionally null, e.g. switched CASE arg 0.
            if (operand != null) {
                operand.accept(this);
            }
        }
        // Remove the parent aggregate level after visiting its children
        inOver = prevInOver;
        return null;
    }

    private void translateAgg(SqlCall call) {
        translateAgg(call, null, null, null, false, call);
    }

    private void translateAgg(
            SqlCall call,
            @Nullable SqlNode filter,
            @Nullable SqlNodeList distinctList,
            @Nullable SqlNodeList orderList,
            boolean ignoreNulls,
            SqlCall outerCall) {
        assert bb.agg == this;
        final RexBuilder rexBuilder = bb.getRexBuilder();
        final List<SqlNode> operands = call.getOperandList();
        final SqlParserPos pos = call.getParserPosition();
        final SqlCall call2;
        final List<SqlNode> operands2;
        switch (call.getKind()) {
            case FILTER:
                assert filter == null;
                translateAgg(
                        call.operand(0),
                        call.operand(1),
                        distinctList,
                        orderList,
                        ignoreNulls,
                        outerCall);
                return;
            case WITHIN_DISTINCT:
                assert orderList == null;
                translateAgg(
                        call.operand(0),
                        filter,
                        call.operand(1),
                        orderList,
                        ignoreNulls,
                        outerCall);
                return;
            case WITHIN_GROUP:
                assert orderList == null;
                translateAgg(
                        call.operand(0),
                        filter,
                        distinctList,
                        call.operand(1),
                        ignoreNulls,
                        outerCall);
                return;
            case IGNORE_NULLS:
                ignoreNulls = true;
            // fall through
            case RESPECT_NULLS:
                translateAgg(
                        call.operand(0), filter, distinctList, orderList, ignoreNulls, outerCall);
                return;

            case COUNTIF:
                // COUNTIF(b)  ==> COUNT(*) FILTER (WHERE b)
                // COUNTIF(b) FILTER (WHERE b2)  ==> COUNT(*) FILTER (WHERE b2 AND b)
                call2 = SqlStdOperatorTable.COUNT.createCall(pos, SqlIdentifier.star(pos));
                final SqlNode filter2 = SqlUtil.andExpressions(filter, call.operand(0));
                translateAgg(call2, filter2, distinctList, orderList, ignoreNulls, outerCall);
                return;

            case STRING_AGG:
                // Translate "STRING_AGG(s, sep ORDER BY x, y)"
                // as if it were "LISTAGG(s, sep) WITHIN GROUP (ORDER BY x, y)";
                // and "STRING_AGG(s, sep)" as "LISTAGG(s, sep)".
                if (!operands.isEmpty() && Util.last(operands) instanceof SqlNodeList) {
                    orderList = (SqlNodeList) Util.last(operands);
                    operands2 = Util.skipLast(operands);
                } else {
                    operands2 = operands;
                }
                call2 =
                        SqlStdOperatorTable.LISTAGG.createCall(
                                call.getFunctionQuantifier(), pos, operands2);
                translateAgg(call2, filter, distinctList, orderList, ignoreNulls, outerCall);
                return;

            case GROUP_CONCAT:
                // Translate "GROUP_CONCAT(s ORDER BY x, y SEPARATOR ',')"
                // as if it were "LISTAGG(s, ',') WITHIN GROUP (ORDER BY x, y)".
                // To do this, build a list of operands without ORDER BY with with sep.
                operands2 = new ArrayList<>(operands);
                final SqlNode separator;
                if (!operands2.isEmpty() && Util.last(operands2).getKind() == SqlKind.SEPARATOR) {
                    final SqlCall sepCall = (SqlCall) operands2.remove(operands.size() - 1);
                    separator = sepCall.operand(0);
                } else {
                    separator = null;
                }

                if (!operands2.isEmpty() && Util.last(operands2) instanceof SqlNodeList) {
                    orderList = (SqlNodeList) operands2.remove(operands2.size() - 1);
                }

                if (separator != null) {
                    operands2.add(separator);
                }

                call2 =
                        SqlStdOperatorTable.LISTAGG.createCall(
                                call.getFunctionQuantifier(), pos, operands2);
                translateAgg(call2, filter, distinctList, orderList, ignoreNulls, outerCall);
                return;

            case ARRAY_AGG:
            case ARRAY_CONCAT_AGG:
                // Translate "ARRAY_AGG(s ORDER BY x, y)"
                // as if it were "ARRAY_AGG(s) WITHIN GROUP (ORDER BY x, y)";
                // similarly "ARRAY_CONCAT_AGG".
                if (!operands.isEmpty() && Util.last(operands) instanceof SqlNodeList) {
                    orderList = (SqlNodeList) Util.last(operands);
                    call2 =
                            call.getOperator()
                                    .createCall(
                                            call.getFunctionQuantifier(),
                                            pos,
                                            Util.skipLast(operands));
                    translateAgg(call2, filter, distinctList, orderList, ignoreNulls, outerCall);
                    return;
                }
            // "ARRAY_AGG" and "ARRAY_CONCAT_AGG" without "ORDER BY"
            // are handled normally; fall through.

            default:
                break;
        }
        final List<Integer> args = new ArrayList<>();
        int filterArg = -1;
        final ImmutableBitSet distinctKeys;
        try {
            // switch out of agg mode
            bb.agg = null;
            // ----- FLINK MODIFICATION BEGIN -----
            FlinkSqlCallBinding binding = new FlinkSqlCallBinding(validator, scope, call);
            List<SqlNode> sqlNodes = binding.operands();
            for (int i = 0; i < sqlNodes.size(); i++) {
                SqlNode operand = sqlNodes.get(i);
                // special case for COUNT(*):  delete the *
                if (operand instanceof SqlIdentifier) {
                    SqlIdentifier id = (SqlIdentifier) operand;
                    if (id.isStar()) {
                        assert call.operandCount() == 1;
                        assert args.isEmpty();
                        break;
                    }
                }
                RexNode convertedExpr = bb.convertExpression(operand);
                args.add(lookupOrCreateGroupExpr(convertedExpr));
            }
            // ----- FLINK MODIFICATION END -----

            if (filter != null) {
                RexNode convertedExpr = bb.convertExpression(filter);
                if (convertedExpr.getType().isNullable()) {
                    convertedExpr = rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, convertedExpr);
                }
                filterArg = lookupOrCreateGroupExpr(convertedExpr);
            }

            if (distinctList == null) {
                distinctKeys = null;
            } else {
                final ImmutableBitSet.Builder distinctBuilder = ImmutableBitSet.builder();
                for (SqlNode distinct : distinctList) {
                    RexNode e = bb.convertExpression(distinct);
                    distinctBuilder.set(lookupOrCreateGroupExpr(e));
                }
                distinctKeys = distinctBuilder.build();
            }
        } finally {
            // switch back into agg mode
            bb.agg = this;
        }

        SqlAggFunction aggFunction = (SqlAggFunction) call.getOperator();
        final RelDataType type = bb.getValidator().deriveType(bb.scope, call);
        boolean distinct = false;
        SqlLiteral quantifier = call.getFunctionQuantifier();
        if ((null != quantifier) && (quantifier.getValue() == SqlSelectKeyword.DISTINCT)) {
            distinct = true;
        }
        boolean approximate = false;
        if (aggFunction == SqlStdOperatorTable.APPROX_COUNT_DISTINCT) {
            aggFunction = SqlStdOperatorTable.COUNT;
            distinct = true;
            approximate = true;
        }
        final RelCollation collation;
        if (orderList == null || orderList.size() == 0) {
            collation = RelCollations.EMPTY;
        } else {
            try {
                // switch out of agg mode
                bb.agg = null;
                collation =
                        RelCollations.of(
                                orderList.stream()
                                        .map(
                                                order ->
                                                        bb.convertSortExpression(
                                                                order,
                                                                RelFieldCollation.Direction
                                                                        .ASCENDING,
                                                                RelFieldCollation.NullDirection
                                                                        .UNSPECIFIED,
                                                                this::sortToFieldCollation))
                                        .collect(Collectors.toList()));
            } finally {
                // switch back into agg mode
                bb.agg = this;
            }
        }
        final AggregateCall aggCall =
                AggregateCall.create(
                        aggFunction,
                        distinct,
                        approximate,
                        ignoreNulls,
                        ImmutableList.of(),
                        args,
                        filterArg,
                        distinctKeys,
                        collation,
                        type,
                        nameMap.get(outerCall.toString()));
        RexNode rex =
                rexBuilder.addAggCall(
                        aggCall,
                        groupExprs.size(),
                        aggCalls,
                        aggCallMapping,
                        i -> convertedInputExprs.leftList().get(i).getType().isNullable());
        aggMapping.put(outerCall, rex);
    }

    private RelFieldCollation sortToFieldCollation(
            SqlNode expr,
            RelFieldCollation.Direction direction,
            RelFieldCollation.NullDirection nullDirection) {
        final RexNode node = bb.convertExpression(expr);
        final int fieldIndex = lookupOrCreateGroupExpr(node);
        if (nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED) {
            nullDirection = direction.defaultNullDirection();
        }
        return new RelFieldCollation(fieldIndex, direction, nullDirection);
    }

    private int lookupOrCreateGroupExpr(RexNode expr) {
        int index = 0;
        for (RexNode convertedInputExpr : convertedInputExprs.leftList()) {
            if (expr.equals(convertedInputExpr)) {
                return index;
            }
            ++index;
        }

        // not found -- add it
        addExpr(expr, null);
        return index;
    }

    /**
     * If an expression is structurally identical to one of the group-by expressions, returns a
     * reference to the expression, otherwise returns null.
     */
    int lookupGroupExpr(SqlNode expr) {
        return SqlUtil.indexOfDeep(groupExprs, expr, Litmus.IGNORE);
    }

    boolean isMeasureExpr(SqlNode expr) {
        return SqlUtil.indexOfDeep(measureExprs, expr, Litmus.IGNORE) >= 0;
    }

    @Nullable RexNode lookupMeasure(SqlNode expr) {
        return aggMapping.get(expr);
    }

    @Nullable RexNode lookupAggregates(SqlCall call) {
        // assert call.getOperator().isAggregator();
        assert bb.agg == this;

        for (Map.Entry<SqlNode, Ord<AuxiliaryConverter>> e : auxiliaryGroupExprs.entrySet()) {
            if (call.equalsDeep(e.getKey(), Litmus.IGNORE)) {
                AuxiliaryConverter converter = e.getValue().e;
                final RexBuilder rexBuilder = bb.getRexBuilder();
                final int groupOrdinal = e.getValue().i;
                return converter.convert(
                        rexBuilder,
                        convertedInputExprs.leftList().get(groupOrdinal),
                        rexBuilder.makeInputRef(castNonNull(bb.root), groupOrdinal));
            }
        }

        return aggMapping.get(call);
    }

    /**
     * Returns the resolved. Valid only if this AggConverter was created via {@link
     * #create(SqlToRelConverter.Blackboard, AggregatingSelectScope)}.
     */
    AggregatingSelectScope.Resolved getResolved() {
        throw new UnsupportedOperationException();
    }
}
