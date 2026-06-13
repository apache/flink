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

import org.apache.flink.table.api.ValidationException;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.math.BigDecimal;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Copied from Calcite.
 *
 * <p>Currently disabling EXCLUDE.
 */
public class SqlWindow extends SqlCall {
    /** The FOLLOWING operator used exclusively in a window specification. */
    public static final SqlPostfixOperator FOLLOWING_OPERATOR =
            new SqlPostfixOperator(
                    "FOLLOWING", SqlKind.FOLLOWING, 20, ReturnTypes.ARG0, null, null);

    /** The PRECEDING operator used exclusively in a window specification. */
    public static final SqlPostfixOperator PRECEDING_OPERATOR =
            new SqlPostfixOperator(
                    "PRECEDING", SqlKind.PRECEDING, 20, ReturnTypes.ARG0, null, null);

    // ~ Instance fields --------------------------------------------------------

    /** The name of the window being declared. */
    @Nullable SqlIdentifier declName;

    /** The name of the window being referenced, or null. */
    @Nullable SqlIdentifier refName;

    /** The list of partitioning columns. */
    SqlNodeList partitionList;

    /** The list of ordering columns. */
    SqlNodeList orderList;

    /** Whether it is a physical (rows) or logical (values) range. */
    SqlLiteral isRows;

    /** The lower bound of the window. */
    @Nullable SqlNode lowerBound;

    /** The upper bound of the window. */
    @Nullable SqlNode upperBound;

    /** Exclude rows from the frame. */
    SqlLiteral exclude;

    /** Whether to allow partial results. It may be null. */
    @Nullable SqlLiteral allowPartial;

    private @Nullable SqlCall windowCall = null;

    // ~ Constructors -----------------------------------------------------------

    /** Creates a window. */
    public SqlWindow(
            SqlParserPos pos,
            @Nullable SqlIdentifier declName,
            @Nullable SqlIdentifier refName,
            SqlNodeList partitionList,
            SqlNodeList orderList,
            SqlLiteral isRows,
            @Nullable SqlNode lowerBound,
            @Nullable SqlNode upperBound,
            @Nullable SqlLiteral allowPartial) {
        this(
                pos,
                declName,
                refName,
                partitionList,
                orderList,
                isRows,
                lowerBound,
                upperBound,
                allowPartial,
                createExcludeNoOthers(SqlParserPos.ZERO));
    }

    /** Creates a window. */
    public SqlWindow(
            SqlParserPos pos,
            @Nullable SqlIdentifier declName,
            @Nullable SqlIdentifier refName,
            SqlNodeList partitionList,
            SqlNodeList orderList,
            SqlLiteral isRows,
            @Nullable SqlNode lowerBound,
            @Nullable SqlNode upperBound,
            @Nullable SqlLiteral allowPartial,
            SqlLiteral exclude) {
        super(pos);
        this.declName = declName;
        this.refName = refName;
        this.partitionList = requireNonNull(partitionList, "partitionList");
        this.orderList = requireNonNull(orderList, "orderList");
        this.isRows = isRows;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.allowPartial = allowPartial;
        this.exclude = exclude;

        assert exclude.symbolValue(Exclusion.class) == Exclusion.EXCLUDE_NO_OTHER
                || (lowerBound != null || upperBound != null);
        assert declName == null || declName.isSimple();
    }

    public static SqlWindow create(
            @Nullable SqlIdentifier declName,
            @Nullable SqlIdentifier refName,
            SqlNodeList partitionList,
            SqlNodeList orderList,
            SqlLiteral isRows,
            @Nullable SqlNode lowerBound,
            @Nullable SqlNode upperBound,
            @Nullable SqlLiteral allowPartial,
            SqlParserPos pos) {
        return create(
                declName,
                refName,
                partitionList,
                orderList,
                isRows,
                lowerBound,
                upperBound,
                allowPartial,
                createExcludeNoOthers(SqlParserPos.ZERO),
                pos);
    }

    public static SqlWindow create(
            @Nullable SqlIdentifier declName,
            @Nullable SqlIdentifier refName,
            SqlNodeList partitionList,
            SqlNodeList orderList,
            SqlLiteral isRows,
            @Nullable SqlNode lowerBound,
            @Nullable SqlNode upperBound,
            @Nullable SqlLiteral allowPartial,
            SqlLiteral exclude,
            SqlParserPos pos) {
        // If there's only one bound and it's 'FOLLOWING', make it the upper
        // bound.
        if (upperBound == null && lowerBound != null && lowerBound.getKind() == SqlKind.FOLLOWING) {
            upperBound = lowerBound;
            lowerBound = null;
        }
        return new SqlWindow(
                pos,
                declName,
                refName,
                partitionList,
                orderList,
                isRows,
                lowerBound,
                upperBound,
                allowPartial,
                exclude);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public SqlOperator getOperator() {
        return SqlWindowOperator.INSTANCE;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.WINDOW;
    }

    @SuppressWarnings("nullness")
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                declName,
                refName,
                partitionList,
                orderList,
                isRows,
                lowerBound,
                upperBound,
                allowPartial,
                exclude);
    }

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    public void setOperand(int i, @Nullable SqlNode operand) {
        switch (i) {
            case 0:
                this.declName = (SqlIdentifier) operand;
                break;
            case 1:
                this.refName = (SqlIdentifier) operand;
                break;
            case 2:
                this.partitionList = (SqlNodeList) operand;
                break;
            case 3:
                this.orderList = (SqlNodeList) operand;
                break;
            case 4:
                this.isRows = (SqlLiteral) operand;
                break;
            case 5:
                this.lowerBound = operand;
                break;
            case 6:
                this.upperBound = operand;
                break;
            case 7:
                this.allowPartial = (SqlLiteral) operand;
                break;
            case 8:
                this.exclude = (SqlLiteral) operand;
                break;
            default:
                throw new AssertionError(i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (null != declName) {
            declName.unparse(writer, 0, 0);
            writer.keyword("AS");
        }

        // Override, so we don't print extra parentheses.
        getOperator().unparse(writer, this, 0, 0);
    }

    public @Nullable SqlIdentifier getDeclName() {
        return declName;
    }

    public void setDeclName(SqlIdentifier declName) {
        assert declName.isSimple();
        this.declName = declName;
    }

    public @Nullable SqlNode getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(@Nullable SqlNode lowerBound) {
        this.lowerBound = lowerBound;
    }

    public @Nullable SqlNode getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(@Nullable SqlNode upperBound) {
        this.upperBound = upperBound;
    }

    public SqlLiteral getExclude() {
        return exclude;
    }

    /**
     * Returns if the window is guaranteed to have rows. This is useful to refine data type of
     * window aggregates. For instance sum(non-nullable) over (empty window) is NULL.
     *
     * @return true when the window is non-empty
     * @see org.apache.calcite.rel.core.Window.Group#isAlwaysNonEmpty()
     * @see SqlOperatorBinding#getGroupCount()
     * @see org.apache.calcite.sql.validate.SqlValidatorImpl#resolveWindow(SqlNode,
     *     SqlValidatorScope)
     */
    public boolean isAlwaysNonEmpty() {
        final RexWindowBound lower;
        final RexWindowBound upper;
        if (lowerBound == null) {
            if (upperBound == null) {
                lower = RexWindowBounds.UNBOUNDED_PRECEDING;
            } else {
                lower = RexWindowBounds.CURRENT_ROW;
            }
        } else if (lowerBound instanceof SqlLiteral) {
            lower = RexWindowBounds.create(lowerBound, null);
        } else {
            return false;
        }
        if (upperBound == null) {
            upper = RexWindowBounds.CURRENT_ROW;
        } else if (upperBound instanceof SqlLiteral) {
            upper = RexWindowBounds.create(upperBound, null);
        } else {
            return false;
        }
        return isAlwaysNonEmpty(lower, upper);
    }

    public static boolean isAlwaysNonEmpty(RexWindowBound lower, RexWindowBound upper) {
        final int lowerKey = lower.getOrderKey();
        final int upperKey = upper.getOrderKey();
        return lowerKey > -1 && lowerKey <= upperKey;
    }

    public void setRows(SqlLiteral isRows) {
        this.isRows = isRows;
    }

    @Pure
    public boolean isRows() {
        return isRows.booleanValue();
    }

    public SqlNodeList getOrderList() {
        return orderList;
    }

    public void setOrderList(SqlNodeList orderList) {
        this.orderList = orderList;
    }

    public SqlNodeList getPartitionList() {
        return partitionList;
    }

    public void setPartitionList(SqlNodeList partitionList) {
        this.partitionList = partitionList;
    }

    public @Nullable SqlIdentifier getRefName() {
        return refName;
    }

    public void setWindowCall(@Nullable SqlCall windowCall) {
        this.windowCall = windowCall;
        assert windowCall == null || windowCall.getOperator() instanceof SqlAggFunction;
    }

    public @Nullable SqlCall getWindowCall() {
        return windowCall;
    }

    // CHECKSTYLE: IGNORE 1
    /**
     * @see Util#deprecated(Object, boolean)
     */
    static void checkSpecialLiterals(SqlWindow window, SqlValidator validator) {
        final SqlNode lowerBound = window.getLowerBound();
        final SqlNode upperBound = window.getUpperBound();
        Object lowerLitType = null;
        Object upperLitType = null;
        SqlOperator lowerOp = null;
        SqlOperator upperOp = null;
        if (null != lowerBound) {
            if (lowerBound.getKind() == SqlKind.LITERAL) {
                lowerLitType = ((SqlLiteral) lowerBound).getValue();
                if (Bound.UNBOUNDED_FOLLOWING == lowerLitType) {
                    throw validator.newValidationError(lowerBound, RESOURCE.badLowerBoundary());
                }
            } else if (lowerBound instanceof SqlCall) {
                lowerOp = ((SqlCall) lowerBound).getOperator();
            }
        }
        if (null != upperBound) {
            if (upperBound.getKind() == SqlKind.LITERAL) {
                upperLitType = ((SqlLiteral) upperBound).getValue();
                if (Bound.UNBOUNDED_PRECEDING == upperLitType) {
                    throw validator.newValidationError(upperBound, RESOURCE.badUpperBoundary());
                }
            } else if (upperBound instanceof SqlCall) {
                upperOp = ((SqlCall) upperBound).getOperator();
            }
        }

        if (Bound.CURRENT_ROW == lowerLitType) {
            if (null != upperOp) {
                if (upperOp == PRECEDING_OPERATOR) {
                    throw validator.newValidationError(
                            castNonNull(upperBound), RESOURCE.currentRowPrecedingError());
                }
            }
        } else if (null != lowerOp) {
            if (lowerOp == FOLLOWING_OPERATOR) {
                if (null != upperOp) {
                    if (upperOp == PRECEDING_OPERATOR) {
                        throw validator.newValidationError(
                                castNonNull(upperBound), RESOURCE.followingBeforePrecedingError());
                    }
                } else if (null != upperLitType) {
                    if (Bound.CURRENT_ROW == upperLitType) {
                        throw validator.newValidationError(
                                castNonNull(upperBound), RESOURCE.currentRowFollowingError());
                    }
                }
            }
        }
    }

    public static SqlLiteral createExcludeNoOthers(SqlParserPos pos) {
        return Exclusion.EXCLUDE_NO_OTHER.symbol(pos);
    }

    public static SqlLiteral createExcludeCurrentRow(SqlParserPos pos) {
        // FLINK MODIFICATION BEGIN
        throw new ValidationException("Exclusion of current row is not supported");
        // FLINK MODIFICATION END
    }

    public static SqlLiteral createExcludeTies(SqlParserPos pos) {
        // FLINK MODIFICATION BEGIN
        throw new ValidationException("Exclusion of ties is not supported");
        // FLINK MODIFICATION END
    }

    public static SqlLiteral createExcludeGroup(SqlParserPos pos) {
        // FLINK MODIFICATION BEGIN
        throw new ValidationException("Exclusion of group is not supported");
        // FLINK MODIFICATION END
    }

    public static boolean isExcludeNoOthers(SqlLiteral node) {
        return node.symbolValue(Exclusion.class) == Exclusion.EXCLUDE_NO_OTHER;
    }

    public static boolean isExcludeCurrentRow(SqlLiteral node) {
        return node.symbolValue(Exclusion.class) == Exclusion.EXCLUDE_CURRENT_ROW;
    }

    public static boolean isExcludeGroup(SqlLiteral node) {
        return node.symbolValue(Exclusion.class) == Exclusion.EXCLUDE_GROUP;
    }

    public static boolean isExcludeTies(SqlLiteral node) {
        return node.symbolValue(Exclusion.class) == Exclusion.EXCLUDE_TIES;
    }

    public static SqlNode createCurrentRow(SqlParserPos pos) {
        return Bound.CURRENT_ROW.symbol(pos);
    }

    public static SqlNode createUnboundedFollowing(SqlParserPos pos) {
        return Bound.UNBOUNDED_FOLLOWING.symbol(pos);
    }

    public static SqlNode createUnboundedPreceding(SqlParserPos pos) {
        return Bound.UNBOUNDED_PRECEDING.symbol(pos);
    }

    public static SqlNode createFollowing(SqlNode e, SqlParserPos pos) {
        return FOLLOWING_OPERATOR.createCall(pos, e);
    }

    public static SqlNode createPreceding(SqlNode e, SqlParserPos pos) {
        return PRECEDING_OPERATOR.createCall(pos, e);
    }

    public static SqlNode createBound(SqlLiteral range) {
        return range;
    }

    /** Returns whether an expression represents the "CURRENT ROW" bound. */
    public static boolean isCurrentRow(SqlNode node) {
        return (node instanceof SqlLiteral)
                && ((SqlLiteral) node).symbolValue(Bound.class) == Bound.CURRENT_ROW;
    }

    /** Returns whether an expression represents the "UNBOUNDED PRECEDING" bound. */
    public static boolean isUnboundedPreceding(SqlNode node) {
        return (node instanceof SqlLiteral)
                && ((SqlLiteral) node).symbolValue(Bound.class) == Bound.UNBOUNDED_PRECEDING;
    }

    /** Returns whether an expression represents the "UNBOUNDED FOLLOWING" bound. */
    public static boolean isUnboundedFollowing(SqlNode node) {
        return (node instanceof SqlLiteral)
                && ((SqlLiteral) node).symbolValue(Bound.class) == Bound.UNBOUNDED_FOLLOWING;
    }

    /**
     * Creates a new window by combining this one with another.
     *
     * <p>For example,
     *
     * <blockquote>
     *
     * <pre>WINDOW (w PARTITION BY x ORDER BY y)
     *   overlay
     *   WINDOW w AS (PARTITION BY z)</pre>
     *
     * </blockquote>
     *
     * <p>yields
     *
     * <blockquote>
     *
     * <pre>WINDOW (PARTITION BY z ORDER BY y)</pre>
     *
     * </blockquote>
     *
     * <p>Does not alter this or the other window.
     *
     * @return A new window
     */
    public SqlWindow overlay(SqlWindow that, SqlValidator validator) {
        // check 7.11 rule 10c
        final SqlNodeList partitions = getPartitionList();
        if (!partitions.isEmpty()) {
            throw validator.newValidationError(partitions.get(0), RESOURCE.partitionNotAllowed());
        }

        // 7.11 rule 10d
        final SqlNodeList baseOrder = getOrderList();
        final SqlNodeList refOrder = that.getOrderList();
        if (!baseOrder.isEmpty() && !refOrder.isEmpty()) {
            throw validator.newValidationError(baseOrder.get(0), RESOURCE.orderByOverlap());
        }

        // 711 rule 10e
        final SqlNode lowerBound = that.getLowerBound();
        final SqlNode upperBound = that.getUpperBound();
        final SqlLiteral exclude = that.getExclude();
        if ((null != lowerBound)
                || (null != upperBound)
                || exclude.symbolValue(Exclusion.class) != Exclusion.EXCLUDE_NO_OTHER) {
            throw validator.newValidationError(that.isRows, RESOURCE.refWindowWithFrame());
        }

        SqlIdentifier declNameNew = declName;
        SqlIdentifier refNameNew;
        SqlNodeList partitionListNew = partitionList;
        SqlNodeList orderListNew = orderList;
        SqlLiteral isRowsNew = isRows;
        SqlNode lowerBoundNew = lowerBound;
        SqlNode upperBoundNew = upperBound;
        SqlLiteral allowPartialNew = allowPartial;

        // Clear the reference window, because the reference is now resolved.
        // The overlaying window may have its own reference, of course.
        refNameNew = null;

        // Overlay other parameters.
        if (setOperand(partitionListNew, that.partitionList, validator)) {
            partitionListNew = that.partitionList;
        }
        if (setOperand(orderListNew, that.orderList, validator)) {
            orderListNew = that.orderList;
        }
        if (setOperand(lowerBoundNew, that.lowerBound, validator)) {
            lowerBoundNew = that.lowerBound;
        }
        if (setOperand(upperBoundNew, that.upperBound, validator)) {
            upperBoundNew = that.upperBound;
        }
        return new SqlWindow(
                SqlParserPos.ZERO,
                declNameNew,
                refNameNew,
                partitionListNew,
                orderListNew,
                isRowsNew,
                lowerBoundNew,
                upperBoundNew,
                allowPartialNew,
                exclude);
    }

    private static boolean setOperand(
            @Nullable SqlNode clonedOperand,
            @Nullable SqlNode thatOperand,
            SqlValidator validator) {
        if ((thatOperand != null) && !SqlNodeList.isEmptyList(thatOperand)) {
            if ((clonedOperand == null) || SqlNodeList.isEmptyList(clonedOperand)) {
                return true;
            } else {
                throw validator.newValidationError(
                        clonedOperand, RESOURCE.cannotOverrideWindowAttribute());
            }
        }
        return false;
    }

    /**
     * Overridden method to specifically check only the right subtree of a window definition.
     *
     * @param node The SqlWindow to compare to "this" window
     * @param litmus What to do if an error is detected (nodes are not equal)
     * @return boolean true if all nodes in the subtree are equal
     */
    @Override
    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
        // This is the difference over super.equalsDeep.  It skips
        // operands[0] the declared name fo this window.  We only want
        // to check the window components.
        return node == this
                || node instanceof SqlWindow
                        && SqlNode.equalDeep(
                                Util.skip(getOperandList()),
                                Util.skip(((SqlWindow) node).getOperandList()),
                                litmus);
    }

    /**
     * Returns whether partial windows are allowed. If false, a partial window (for example, a
     * window of size 1 hour which has only 45 minutes of data in it) will appear to windowed
     * aggregate functions to be empty.
     */
    @EnsuresNonNullIf(expression = "allowPartial", result = false)
    public boolean isAllowPartial() {
        // Default (and standard behavior) is to allow partial windows.
        return allowPartial == null || allowPartial.booleanValue();
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        SqlValidatorScope operandScope = scope; // REVIEW

        @SuppressWarnings("unused")
        SqlIdentifier declName = this.declName;
        SqlIdentifier refName = this.refName;
        SqlNodeList partitionList = this.partitionList;
        SqlNodeList orderList = this.orderList;
        SqlLiteral isRows = this.isRows;
        SqlNode lowerBound = this.lowerBound;
        SqlNode upperBound = this.upperBound;
        SqlLiteral allowPartial = this.allowPartial;

        if (refName != null) {
            SqlWindow win = validator.resolveWindow(this, operandScope);
            partitionList = win.partitionList;
            orderList = win.orderList;
            isRows = win.isRows;
            lowerBound = win.lowerBound;
            upperBound = win.upperBound;
            allowPartial = win.allowPartial;
        }

        for (SqlNode partitionItem : partitionList) {
            try {
                partitionItem.accept(Util.OverFinder.INSTANCE);
            } catch (ControlFlowException e) {
                throw validator.newValidationError(
                        this, RESOURCE.partitionbyShouldNotContainOver());
            }

            partitionItem.validateExpr(validator, operandScope);
        }

        for (SqlNode orderItem : orderList) {
            boolean savedColumnReferenceExpansion = validator.config().columnReferenceExpansion();
            validator.transform(config -> config.withColumnReferenceExpansion(false));
            try {
                orderItem.accept(Util.OverFinder.INSTANCE);
            } catch (ControlFlowException e) {
                throw validator.newValidationError(this, RESOURCE.orderbyShouldNotContainOver());
            }

            try {
                orderItem.validateExpr(validator, scope);
            } finally {
                validator.transform(
                        config ->
                                config.withColumnReferenceExpansion(savedColumnReferenceExpansion));
            }
        }

        // 6.10 rule 6a Function RANK & DENSE_RANK require ORDER BY clause
        if (orderList.isEmpty()
                && !SqlValidatorUtil.containsMonotonic(scope)
                && windowCall != null
                && windowCall.getOperator().requiresOrder()) {
            throw validator.newValidationError(this, RESOURCE.funcNeedsOrderBy());
        }

        // Run framing checks if there are any
        if (upperBound != null || lowerBound != null) {
            // 6.10 Rule 6a RANK & DENSE_RANK do not allow ROWS or RANGE
            if (windowCall != null && !windowCall.getOperator().allowsFraming()) {
                throw validator.newValidationError(isRows, RESOURCE.rankWithFrame());
            }
            SqlTypeFamily orderTypeFam = null;

            // SQL03 7.10 Rule 11a
            if (!orderList.isEmpty()) {
                // if order by is a compound list then range not allowed
                if (orderList.size() > 1
                        && !isRows()
                        && !onlySymbolBounds(lowerBound, upperBound)) {
                    throw validator.newValidationError(
                            isRows, RESOURCE.compoundOrderByProhibitsRange());
                }

                // get the type family for the sort key for Frame Boundary Val.
                RelDataType orderType = validator.deriveType(operandScope, orderList.get(0));
                orderTypeFam = orderType.getSqlTypeName().getFamily();
            } else {
                // requires an ORDER BY clause if frame is logical(RANGE)
                // We relax this requirement if the table appears to be
                // sorted already
                if (!onlySymbolBounds(lowerBound, upperBound)
                        && !isRows()
                        && !SqlValidatorUtil.containsMonotonic(scope)) {
                    throw validator.newValidationError(this, RESOURCE.overMissingOrderBy());
                }
            }

            // Let the bounds validate themselves
            validateFrameBoundary(lowerBound, isRows(), orderTypeFam, validator, operandScope);
            validateFrameBoundary(upperBound, isRows(), orderTypeFam, validator, operandScope);

            // Validate across boundaries. 7.10 Rule 8 a-d
            checkSpecialLiterals(this, validator);
        } else if (orderList.isEmpty()
                && !SqlValidatorUtil.containsMonotonic(scope)
                && windowCall != null
                && windowCall.getOperator().requiresOrder()) {
            throw validator.newValidationError(this, RESOURCE.overMissingOrderBy());
        }

        if (!isRows() && !isAllowPartial()) {
            throw validator.newValidationError(
                    castNonNull(allowPartial), RESOURCE.cannotUseDisallowPartialWithRange());
        }
    }

    private static boolean onlySymbolBounds(
            @Nullable SqlNode lowerBound, @Nullable SqlNode upperBound) {
        return lowerBound != null
                && upperBound != null
                && (isCurrentRow(lowerBound) || isUnboundedPreceding(lowerBound))
                && (isCurrentRow(upperBound) || isUnboundedFollowing(upperBound));
    }

    private static void validateFrameBoundary(
            @Nullable SqlNode bound,
            boolean isRows,
            @Nullable SqlTypeFamily orderTypeFam,
            SqlValidator validator,
            SqlValidatorScope scope) {
        if (null == bound) {
            return;
        }
        bound.validate(validator, scope);
        switch (bound.getKind()) {
            case LITERAL:
                // is there really anything to validate here? this covers
                // "CURRENT_ROW","unbounded preceding" & "unbounded following"
                break;

            case OTHER:
            case FOLLOWING:
            case PRECEDING:
                assert bound instanceof SqlCall;
                final SqlNode boundVal = ((SqlCall) bound).operand(0);

                // SQL03 7.10 rule 11b Physical ROWS must be a numeric constant. JR:
                // actually it's SQL03 7.11 rule 11b "exact numeric with scale 0"
                // means not only numeric constant but exact numeric integral
                // constant. We also interpret the spec. to not allow negative
                // values, but allow zero.
                if (isRows) {
                    if (boundVal instanceof SqlNumericLiteral) {
                        final SqlNumericLiteral boundLiteral = (SqlNumericLiteral) boundVal;
                        if (!boundLiteral.isExact()
                                || (boundLiteral.getScale() != null
                                        && boundLiteral
                                                        .getValueAs(BigDecimal.class)
                                                        .stripTrailingZeros()
                                                        .scale()
                                                > 0)
                                || (0 > boundLiteral.longValue(true))) {
                            // true == throw if not exact (we just tested that - right?)
                            throw validator.newValidationError(
                                    boundVal, RESOURCE.rowMustBeNonNegativeIntegral());
                        }
                    } else {
                        // Allow expressions in ROWS clause
                    }
                }

                // if this is a range spec check and make sure the boundary type
                // and order by type are compatible
                if (orderTypeFam != null && !isRows) {
                    final RelDataType boundType = validator.deriveType(scope, boundVal);
                    final SqlTypeFamily boundTypeFamily = boundType.getSqlTypeName().getFamily();
                    final List<SqlTypeFamily> allowableBoundTypeFamilies =
                            orderTypeFam.allowableDifferenceTypes();
                    if (allowableBoundTypeFamilies.isEmpty()) {
                        throw validator.newValidationError(
                                boundVal, RESOURCE.orderByDataTypeProhibitsRange());
                    }
                    if (!allowableBoundTypeFamilies.contains(boundTypeFamily)) {
                        throw validator.newValidationError(
                                boundVal, RESOURCE.orderByRangeMismatch());
                    }
                }
                break;
            default:
                throw new AssertionError("Unexpected node type");
        }
    }

    /**
     * Creates a window <code>(RANGE <i>columnName</i> CURRENT ROW)</code>.
     *
     * @param columnName Order column
     */
    public SqlWindow createCurrentRowWindow(final String columnName) {
        return SqlWindow.create(
                null,
                null,
                new SqlNodeList(SqlParserPos.ZERO),
                new SqlNodeList(
                        ImmutableList.of(new SqlIdentifier(columnName, SqlParserPos.ZERO)),
                        SqlParserPos.ZERO),
                SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
                SqlWindow.createCurrentRow(SqlParserPos.ZERO),
                SqlWindow.createCurrentRow(SqlParserPos.ZERO),
                SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
                SqlParserPos.ZERO);
    }

    /**
     * Creates a window <code>(RANGE <i>columnName</i> UNBOUNDED
     * PRECEDING)</code>.
     *
     * @param columnName Order column
     */
    public SqlWindow createUnboundedPrecedingWindow(final String columnName) {
        return SqlWindow.create(
                null,
                null,
                new SqlNodeList(SqlParserPos.ZERO),
                new SqlNodeList(
                        ImmutableList.of(new SqlIdentifier(columnName, SqlParserPos.ZERO)),
                        SqlParserPos.ZERO),
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO),
                SqlWindow.createCurrentRow(SqlParserPos.ZERO),
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                SqlParserPos.ZERO);
    }

    @Deprecated // to be removed before 2.0
    public void populateBounds() {
        if (lowerBound == null && upperBound == null) {
            setLowerBound(SqlWindow.createUnboundedPreceding(pos));
        }
        if (lowerBound == null) {
            setLowerBound(SqlWindow.createCurrentRow(pos));
        }
        if (upperBound == null) {
            setUpperBound(SqlWindow.createCurrentRow(pos));
        }
    }

    /**
     * An enumeration of types of exclusion rows in a window: <code>EXCLUDE NO OTHERS</code>, <code>
     * EXCLUDE CURRENT ROW</code>, <code>EXCLUDE TIES</code> and <code>EXCLUDE GROUP</code>.
     */
    enum Exclusion implements Symbolizable {
        EXCLUDE_NO_OTHER("EXCLUDE NO OTHER"),
        EXCLUDE_CURRENT_ROW("EXCLUDE CURRENT ROW"),
        EXCLUDE_TIES("EXCLUDE TIES"),
        EXCLUDE_GROUP("EXCLUDE GROUP");

        private final String sql;

        Exclusion(String sql) {
            this.sql = sql;
        }

        @Override
        public String toString() {
            return sql;
        }
    }

    /**
     * An enumeration of types of bounds in a window: <code>CURRENT ROW</code>, <code>
     * UNBOUNDED PRECEDING</code>, and <code>UNBOUNDED FOLLOWING</code>.
     */
    enum Bound implements Symbolizable {
        CURRENT_ROW("CURRENT ROW"),
        UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"),
        UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING");

        private final String sql;

        Bound(String sql) {
            this.sql = sql;
        }

        @Override
        public String toString() {
            return sql;
        }
    }

    /** An operator describing a window specification. */
    private static class SqlWindowOperator extends SqlOperator {
        private static final SqlWindowOperator INSTANCE = new SqlWindowOperator();

        private SqlWindowOperator() {
            super("WINDOW", SqlKind.WINDOW, 2, true, null, null, null);
        }

        @Override
        public SqlSyntax getSyntax() {
            return SqlSyntax.SPECIAL;
        }

        @SuppressWarnings("argument.type.incompatible")
        @Override
        public SqlCall createCall(
                @Nullable SqlLiteral functionQualifier,
                SqlParserPos pos,
                @Nullable SqlNode... operands) {
            assert functionQualifier == null;
            assert operands.length == 9;
            return create(
                    (SqlIdentifier) operands[0],
                    (SqlIdentifier) operands[1],
                    (SqlNodeList) requireNonNull(operands[2]),
                    (SqlNodeList) requireNonNull(operands[3]),
                    (SqlLiteral) requireNonNull(operands[4]),
                    operands[5],
                    operands[6],
                    (SqlLiteral) operands[7],
                    (SqlLiteral) requireNonNull(operands[8]),
                    pos);
        }

        @Override
        public <R> void acceptCall(
                SqlVisitor<R> visitor,
                SqlCall call,
                boolean onlyExpressions,
                SqlBasicVisitor.ArgHandler<R> argHandler) {
            if (onlyExpressions) {
                for (Ord<SqlNode> operand : Ord.zip(call.getOperandList())) {
                    // if the second param is an Identifier then it's supposed to
                    // be a name from a window clause and isn't part of the
                    // group by check
                    if (operand.e == null) {
                        continue;
                    }
                    if (operand.i == 1 && operand.e instanceof SqlIdentifier) {
                        // skip refName
                        continue;
                    }
                    argHandler.visitChild(visitor, call, operand.i, operand.e);
                }
            } else {
                super.acceptCall(visitor, call, onlyExpressions, argHandler);
            }
        }

        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            final SqlWindow window = (SqlWindow) call;
            final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.WINDOW, "(", ")");
            if (window.refName != null) {
                window.refName.unparse(writer, 0, 0);
            }
            if (!window.partitionList.isEmpty()) {
                writer.sep("PARTITION BY");
                final SqlWriter.Frame partitionFrame = writer.startList("", "");
                window.partitionList.unparse(writer, 0, 0);
                writer.endList(partitionFrame);
            }
            if (!window.orderList.isEmpty()) {
                writer.sep("ORDER BY");
                final SqlWriter.Frame orderFrame = writer.startList("", "");
                window.orderList.unparse(writer, 0, 0);
                writer.endList(orderFrame);
            }
            SqlNode lowerBound = window.lowerBound;
            SqlNode upperBound = window.upperBound;
            SqlLiteral exclude = window.exclude;
            if (lowerBound == null) {
                // No ROWS or RANGE clause
            } else if (upperBound == null) {
                if (window.isRows()) {
                    writer.sep("ROWS");
                } else {
                    writer.sep("RANGE");
                }
                lowerBound.unparse(writer, 0, 0);
                if (!isExcludeNoOthers(exclude)) {
                    exclude.unparse(writer, 0, 0);
                }
            } else {
                if (window.isRows()) {
                    writer.sep("ROWS BETWEEN");
                } else {
                    writer.sep("RANGE BETWEEN");
                }
                lowerBound.unparse(writer, 0, 0);
                writer.keyword("AND");
                upperBound.unparse(writer, 0, 0);
                if (!isExcludeNoOthers(exclude)) {
                    exclude.unparse(writer, 0, 0);
                }
            }

            // ALLOW PARTIAL/DISALLOW PARTIAL
            if (window.allowPartial == null) {
                // do nothing
            } else if (window.isAllowPartial()) {
                // We could output "ALLOW PARTIAL", but this syntax is
                // non-standard. Omitting the clause has the same effect.
            } else {
                writer.keyword("DISALLOW PARTIAL");
            }

            writer.endList(frame);
        }
    }
}
