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

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Access to a field of a row-expression.
 *
 * <p>You might expect to use a <code>RexFieldAccess</code> to access columns of relational tables,
 * for example, the expression <code>emp.empno</code> in the query
 *
 * <blockquote>
 *
 * <pre>SELECT emp.empno FROM emp</pre>
 *
 * </blockquote>
 *
 * <p>but there is a specialized expression {@link RexInputRef} for this purpose. So in practice,
 * <code>RexFieldAccess</code> is usually used to access fields of correlating variables, for
 * example the expression <code>emp.deptno</code> in
 *
 * <blockquote>
 *
 * <pre>SELECT ename
 * FROM dept
 * WHERE EXISTS (
 *     SELECT NULL
 *     FROM emp
 *     WHERE emp.deptno = dept.deptno
 *     AND gender = 'F')</pre>
 *
 * </blockquote>
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>Should be removed after fixing CALCITE-6342 (Calcite 1.36.0): Lines 84-89
 * </ol>
 */
public class RexFieldAccess extends RexNode {
    // ~ Instance fields --------------------------------------------------------

    private final RexNode expr;
    private final RelDataTypeField field;

    // ~ Constructors -----------------------------------------------------------

    RexFieldAccess(RexNode expr, RelDataTypeField field) {
        checkValid(expr, field);
        this.expr = expr;
        this.field = field;
        this.digest = expr + "." + field.getName();
    }

    // ~ Methods ----------------------------------------------------------------

    private static void checkValid(RexNode expr, RelDataTypeField field) {
        RelDataType exprType = expr.getType();
        int fieldIdx = field.getIndex();
        Preconditions.checkArgument(
                fieldIdx >= 0
                        && fieldIdx < exprType.getFieldList().size()
                        && exprType.getFieldList().get(fieldIdx).equals(field),
                // FLINK MODIFICATION BEGIN
                "Field %s does not exist for expression %s",
                field,
                expr);
        // FLINK MODIFICATION END
    }

    public RelDataTypeField getField() {
        return field;
    }

    @Override
    public RelDataType getType() {
        return field.getType();
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.FIELD_ACCESS;
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        return visitor.visitFieldAccess(this);
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        return visitor.visitFieldAccess(this, arg);
    }

    /** Returns the expression whose field is being accessed. */
    public RexNode getReferenceExpr() {
        return expr;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RexFieldAccess that = (RexFieldAccess) o;

        return field.equals(that.field) && expr.equals(that.expr);
    }

    @Override
    public int hashCode() {
        int result = expr.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }
}
