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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.UnresolvedDataType;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;

/**
 * Unresolved type literal for a given {@link UnresolvedDataType}.
 *
 * <p>This is purely an API-facing expression with unvalidated arguments and unknown output data
 * type.
 */
@PublicEvolving
public class UnresolvedTypeLiteralExpression implements Expression {

    private final UnresolvedDataType unresolvedDataType;

    UnresolvedTypeLiteralExpression(UnresolvedDataType unresolvedDataType) {
        this.unresolvedDataType = unresolvedDataType;
    }

    public UnresolvedDataType getUnresolvedDataType() {
        return unresolvedDataType;
    }

    public TypeLiteralExpression resolve(DataTypeFactory dataTypeFactory) {
        return typeLiteral(unresolvedDataType.toDataType(dataTypeFactory));
    }

    @Override
    public String asSummaryString() {
        return unresolvedDataType.toString();
    }

    @Override
    public List<Expression> getChildren() {
        return List.of();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final UnresolvedTypeLiteralExpression that = (UnresolvedTypeLiteralExpression) o;
        return unresolvedDataType.equals(that.unresolvedDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unresolvedDataType);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
