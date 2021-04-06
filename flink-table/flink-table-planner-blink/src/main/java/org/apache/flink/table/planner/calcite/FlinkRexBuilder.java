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

package org.apache.flink.table.planner.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.TimestampString;

/** A slim extension over a {@link RexBuilder}. See the overridden methods for more explanation. */
public final class FlinkRexBuilder extends RexBuilder {
    public FlinkRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    /**
     * Compared to the original method we adjust the nullability of the nested column based on the
     * nullability of the enclosing type.
     *
     * <p>If the fields type is NOT NULL, but the enclosing ROW is nullable we still can produce
     * nulls.
     */
    @Override
    public RexNode makeFieldAccess(RexNode expr, String fieldName, boolean caseSensitive) {
        RexNode field = super.makeFieldAccess(expr, fieldName, caseSensitive);
        if (expr.getType().isNullable() && !field.getType().isNullable()) {
            return makeCast(
                    typeFactory.createTypeWithNullability(field.getType(), true), field, true);
        }

        return field;
    }

    /**
     * Compared to the original method we adjust the nullability of the nested column based on the
     * nullability of the enclosing type.
     *
     * <p>If the fields type is NOT NULL, but the enclosing ROW is nullable we still can produce
     * nulls.
     */
    @Override
    public RexNode makeFieldAccess(RexNode expr, int i) {
        RexNode field = super.makeFieldAccess(expr, i);
        if (expr.getType().isNullable() && !field.getType().isNullable()) {
            return makeCast(
                    typeFactory.createTypeWithNullability(field.getType(), true), field, true);
        }

        return field;
    }

    /**
     * Creates a literal of the default value for the given type.
     *
     * <p>This value is:
     *
     * <ul>
     *   <li>0 for numeric types;
     *   <li>FALSE for BOOLEAN;
     *   <li>The epoch for TIMESTAMP and DATE;
     *   <li>Midnight for TIME;
     *   <li>The empty string for string types (CHAR, BINARY, VARCHAR, VARBINARY).
     * </ul>
     *
     * <p>Uses '1970-01-01 00:00:00'(epoch 0 second) as zero value for TIMESTAMP_LTZ, the zero value
     * '0000-00-00 00:00:00' in Calcite is an invalid time whose month and day is invalid, we
     * workaround here. Stop overriding once CALCITE-4555 fixed.
     *
     * @param type Type
     * @return Simple literal, or cast simple literal
     */
    @Override
    public RexNode makeZeroLiteral(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return makeLiteral(new TimestampString(1970, 1, 1, 0, 0, 0), type, false);
            default:
                return super.makeZeroLiteral(type);
        }
    }
}
