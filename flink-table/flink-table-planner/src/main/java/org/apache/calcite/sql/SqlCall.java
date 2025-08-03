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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}. (Operators can be used to
 * describe any syntactic construct, so in practice, every non-leaf node in a SQL parse tree is a
 * <code>SqlCall</code> of some kind.)
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>Should be removed after fixing CALCITE-6944 (Calcite 1.40.0): Lines 121-139
 * </ol>
 */
public abstract class SqlCall extends SqlNode {
    // ~ Constructors -----------------------------------------------------------

    protected SqlCall(SqlParserPos pos) {
        super(pos);
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * Whether this call was created by expanding a parentheses-free call to what was syntactically
     * an identifier.
     */
    public boolean isExpanded() {
        return false;
    }

    /**
     * Changes the value of an operand. Allows some rewrite by {@link SqlValidator}; use sparingly.
     *
     * @param i Operand index
     * @param operand Operand value
     */
    public void setOperand(int i, @Nullable SqlNode operand) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SqlKind getKind() {
        return getOperator().getKind();
    }

    @Pure
    public abstract SqlOperator getOperator();

    /**
     * Returns the list of operands. The set and order of operands is call-specific.
     *
     * <p>Note: the proper type would be {@code List<@Nullable SqlNode>}, however, it would trigger
     * too many changes to the current codebase.
     *
     * @return the list of call operands, never null, the operands can be null
     */
    public abstract List</*Nullable*/ SqlNode> getOperandList();

    /**
     * Returns i-th operand (0-based).
     *
     * <p>Note: the result might be null, so the proper signature would be {@code <S
     * extends @Nullable SqlNode>}, however, it would trigger to many changes to the current
     * codebase.
     *
     * @param i operand index (0-based)
     * @param <S> type of the result
     * @return i-th operand (0-based), the result might be null
     */
    @SuppressWarnings("unchecked")
    public <S extends /*Nullable*/ SqlNode> S operand(int i) {
        // Note: in general, null elements exist in the list, however, the code
        // assumes operand(..) is non-nullable, so we add a cast here
        return (S) castNonNull(getOperandList().get(i));
    }

    public int operandCount() {
        return getOperandList().size();
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return getOperator().createCall(getFunctionQuantifier(), pos, getOperandList());
    }

    // FLINK MODIFICATION BEGIN
    private boolean needsParentheses(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getKind() == SqlKind.SET_SEMANTICS_TABLE) {
            return false;
        }
        final SqlOperator operator = getOperator();
        return leftPrec > operator.getLeftPrec()
                || (operator.getRightPrec() <= rightPrec && (rightPrec != 0))
                || writer.isAlwaysUseParentheses() && isA(SqlKind.EXPRESSION);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlDialect dialect = writer.getDialect();
        if (needsParentheses(writer, leftPrec, rightPrec)) {
            final SqlWriter.Frame frame = writer.startList("(", ")");
            dialect.unparseCall(writer, this, 0, 0);
            writer.endList(frame);
            // FLINK MODIFICATION END
        } else {
            dialect.unparseCall(writer, this, leftPrec, rightPrec);
        }
    }

    /**
     * Validates this call.
     *
     * <p>The default implementation delegates the validation to the operator's {@link
     * SqlOperator#validateCall}. Derived classes may override (as do, for example {@link SqlSelect}
     * and {@link SqlUpdate}).
     */
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateCall(this, scope);
    }

    @Override
    public void findValidOptions(
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlParserPos pos,
            Collection<SqlMoniker> hintList) {
        for (SqlNode operand : getOperandList()) {
            if (operand instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) operand;
                SqlParserPos idPos = id.getParserPosition();
                if (idPos.toString().equals(pos.toString())) {
                    ((SqlValidatorImpl) validator)
                            .lookupNameCompletionHints(scope, id.names, pos, hintList);
                    return;
                }
            }
        }
        // no valid options
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
        if (node == this) {
            return true;
        }
        if (!(node instanceof SqlCall)) {
            return litmus.fail("{} != {}", this, node);
        }
        SqlCall that = (SqlCall) node;

        // Compare operators by name, not identity, because they may not
        // have been resolved yet. Use case insensitive comparison since
        // this may be a case insensitive system.
        if (!this.getOperator().getName().equalsIgnoreCase(that.getOperator().getName())) {
            return litmus.fail("{} != {}", this, node);
        }
        if (!equalDeep(this.getFunctionQuantifier(), that.getFunctionQuantifier(), litmus)) {
            return litmus.fail("{} != {} (function quantifier differs)", this, node);
        }
        return equalDeep(this.getOperandList(), that.getOperandList(), litmus);
    }

    /**
     * Returns a string describing the actual argument types of a call, e.g. "SUBSTR(VARCHAR(12),
     * NUMBER(3,2), INTEGER)".
     */
    protected String getCallSignature(SqlValidator validator, @Nullable SqlValidatorScope scope) {
        List<String> signatureList = new ArrayList<>();
        for (final SqlNode operand : getOperandList()) {
            final RelDataType argType =
                    validator.deriveType(Objects.requireNonNull(scope, "scope"), operand);
            if (null == argType) {
                continue;
            }
            signatureList.add(argType.toString());
        }
        return SqlUtil.getOperatorSignature(getOperator(), signatureList);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
        // Delegate to operator.
        final SqlCallBinding binding = new SqlCallBinding(scope.getValidator(), scope, this);
        return getOperator().getMonotonicity(binding);
    }

    /**
     * Returns whether it is the function {@code COUNT(*)}.
     *
     * @return true if function call to COUNT(*)
     */
    public boolean isCountStar() {
        SqlOperator sqlOperator = getOperator();
        if (sqlOperator.getName().equals("COUNT") && operandCount() == 1) {
            final SqlNode parm = operand(0);
            if (parm instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) parm;
                if (id.isStar() && id.names.size() == 1) {
                    return true;
                }
            }
        }

        return false;
    }

    @Pure
    public @Nullable SqlLiteral getFunctionQuantifier() {
        return null;
    }
}
