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

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Expression for aggregate function which is corresponding to the AggregateCall in Calcite. */
public class AggregateExpression implements ResolvedExpression {

    private final FunctionDefinition functionDefinition;

    private final List<FieldReferenceExpression> args;

    private final @Nullable CallExpression filterExpression;

    private final DataType resultType;

    private final boolean distinct;

    private final boolean approximate;

    private final boolean ignoreNulls;

    public AggregateExpression(
            FunctionDefinition functionDefinition,
            List<FieldReferenceExpression> args,
            @Nullable CallExpression filterExpression,
            DataType resultType,
            boolean distinct,
            boolean approximate,
            boolean ignoreNulls) {
        this.functionDefinition =
                Preconditions.checkNotNull(
                        functionDefinition, "Function definition must not be null.");
        this.args = args;
        this.filterExpression = filterExpression;
        this.resultType = resultType;
        this.distinct = distinct;
        this.approximate = approximate;
        this.ignoreNulls = ignoreNulls;
    }

    public FunctionDefinition getFunctionDefinition() {
        return functionDefinition;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isApproximate() {
        return approximate;
    }

    public boolean isIgnoreNulls() {
        return ignoreNulls;
    }

    public List<FieldReferenceExpression> getArgs() {
        return args;
    }

    @Nullable
    public CallExpression getFilterExpression() {
        return filterExpression;
    }

    /**
     * Returns a string representation of the aggregate function for logging or printing to a
     * console.
     */
    public String getFunctionName() {
        return functionDefinition.toString();
    }

    @Override
    public DataType getOutputDataType() {
        return resultType;
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        return Collections.singletonList(this.filterExpression);
    }

    @Override
    public String asSummaryString() {
        final String argList =
                args.stream()
                        .map(Expression::asSummaryString)
                        .collect(Collectors.joining(", ", "(", ")"));
        return getFunctionName() + argList;
    }

    @Override
    public List<Expression> getChildren() {
        return Collections.singletonList(this.filterExpression);
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
        AggregateExpression that = (AggregateExpression) o;
        return Objects.equals(functionDefinition, that.functionDefinition)
                && args.equals(that.args)
                && filterExpression.equals(that.filterExpression)
                && resultType.equals(that.resultType)
                && Objects.equals(distinct, that.distinct)
                && Objects.equals(approximate, that.approximate)
                && Objects.equals(ignoreNulls, that.ignoreNulls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                functionDefinition,
                args,
                filterExpression,
                resultType,
                distinct,
                approximate,
                ignoreNulls);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
