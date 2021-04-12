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

package org.apache.flink.table.expressions.utils;

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** {@link ResolvedExpression} mock for testing purposes. */
public class ResolvedExpressionMock implements ResolvedExpression {

    private final DataType outputDataType;

    private final Supplier<String> stringRepresentation;

    public ResolvedExpressionMock(DataType outputDataType, Supplier<String> stringRepresentation) {
        this.outputDataType = outputDataType;
        this.stringRepresentation = stringRepresentation;
    }

    public static ResolvedExpression of(DataType outputDataType, String stringRepresentation) {
        return new ResolvedExpressionMock(outputDataType, () -> stringRepresentation);
    }

    @Override
    public String asSummaryString() {
        return stringRepresentation.get();
    }

    @Override
    public List<Expression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String asSerializableString() {
        return stringRepresentation.get();
    }

    @Override
    public DataType getOutputDataType() {
        return outputDataType;
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return asSummaryString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResolvedExpression)) {
            return false;
        }
        ResolvedExpression that = (ResolvedExpression) o;
        return Objects.equals(outputDataType, that.getOutputDataType())
                && Objects.equals(stringRepresentation.get(), that.asSerializableString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputDataType, stringRepresentation);
    }
}
