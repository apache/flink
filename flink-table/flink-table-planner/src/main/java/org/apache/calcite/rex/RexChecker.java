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
package org.apache.calcite.rex;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Litmus;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Visitor which checks the validity of a {@link RexNode} expression.
 *
 * <p>FLINK modifications (backport of CALCITE-6764): Lines 121 ~ 133, 164 ~ 177
 *
 * <p>There are two modes of operation:
 *
 * <ul>
 *   <li>Use<code>fail=true</code> to throw an {@link AssertionError} as soon as an invalid node is
 *       detected:
 *       <blockquote>
 *       <code>RexNode node;<br>
 * RelDataType rowType;<br>
 * assert new RexChecker(rowType, true).isValid(node);</code>
 *       </blockquote>
 *       <p>This mode requires that assertions are enabled.
 *   <li>Use <code>fail=false</code> to test for validity without throwing an error.
 *       <blockquote>
 *       <code>RexNode node;<br>
 * RelDataType rowType;<br>
 * RexChecker checker = new RexChecker(rowType, false);<br>
 * node.accept(checker);<br>
 * if (!checker.valid) {<br>
 * &nbsp;&nbsp;&nbsp;...<br>
 * }</code>
 *       </blockquote>
 * </ul>
 *
 * @see RexNode
 */
public class RexChecker extends RexVisitorImpl<Boolean> {
    // ~ Instance fields --------------------------------------------------------

    protected final RelNode.Context context;
    protected final Litmus litmus;
    protected final List<RelDataType> inputTypeList;
    protected int failCount;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a RexChecker with a given input row type.
     *
     * <p>If <code>fail</code> is true, the checker will throw an {@link AssertionError} if an
     * invalid node is found and assertions are enabled.
     *
     * <p>Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputRowType Input row type
     * @param context Context of the enclosing {@link RelNode}, or null
     * @param litmus What to do if an invalid node is detected
     */
    public RexChecker(final RelDataType inputRowType, RelNode.Context context, Litmus litmus) {
        this(RelOptUtil.getFieldTypeList(inputRowType), context, litmus);
    }

    /**
     * Creates a RexChecker with a given set of input fields.
     *
     * <p>If <code>fail</code> is true, the checker will throw an {@link AssertionError} if an
     * invalid node is found and assertions are enabled.
     *
     * <p>Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputTypeList Input row type
     * @param context Context of the enclosing {@link RelNode}, or null
     * @param litmus What to do if an error is detected
     */
    public RexChecker(List<RelDataType> inputTypeList, RelNode.Context context, Litmus litmus) {
        super(true);
        this.inputTypeList = inputTypeList;
        this.context = context;
        this.litmus = litmus;
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * Returns the number of failures encountered.
     *
     * @return Number of failures
     */
    public int getFailureCount() {
        return failCount;
    }

    @Override
    public Boolean visitInputRef(RexInputRef ref) {
        final int index = ref.getIndex();
        if ((index < 0) || (index >= inputTypeList.size())) {
            ++failCount;
            return litmus.fail(
                    "RexInputRef index {} out of range 0..{}", index, inputTypeList.size() - 1);
        }
        // Type of field and type of result can differ in nullability.  See [CALCITE-6764]
        if (!ref.getType().isStruct()
                && !RelOptUtil.eqUpToNullability(
                        ref.getType().isNullable(),
                        "ref",
                        ref.getType(),
                        "input",
                        inputTypeList.get(index),
                        litmus)) {
            ++failCount;
            return litmus.fail(null);
        }
        return litmus.succeed();
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef ref) {
        ++failCount;
        return litmus.fail("RexLocalRef illegal outside program");
    }

    @Override
    public Boolean visitCall(RexCall call) {
        for (RexNode operand : call.getOperands()) {
            Boolean valid = operand.accept(this);
            if (valid != null && !valid) {
                return litmus.fail(null);
            }
        }
        return litmus.succeed();
    }

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
        super.visitFieldAccess(fieldAccess);
        final RelDataType refType = fieldAccess.getReferenceExpr().getType();
        assert refType.isStruct();
        final RelDataTypeField field = fieldAccess.getField();
        final int index = field.getIndex();
        if ((index < 0) || (index >= refType.getFieldList().size())) {
            ++failCount;
            return litmus.fail(null);
        }
        // Type of field may not match type of field access - they may differ in nullability
        final RelDataTypeField typeField = refType.getFieldList().get(index);
        if (!RelOptUtil.eqUpToNullability(
                refType.isNullable(),
                "type1",
                typeField.getType(),
                "type2",
                fieldAccess.getType(),
                litmus)) {
            ++failCount;
            return litmus.fail(null);
        }
        return litmus.succeed();
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable v) {
        if (context != null && !context.correlationIds().contains(v.id)) {
            ++failCount;
            return litmus.fail(
                    "correlation id {} not found in correlation list {}",
                    v,
                    context.correlationIds());
        }
        return litmus.succeed();
    }

    /** Returns whether an expression is valid. */
    public final boolean isValid(RexNode expr) {
        return requireNonNull(expr.accept(this), () -> "expr.accept(RexChecker) for expr=" + expr);
    }
}
