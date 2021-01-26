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
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Resolved and validated expression for calling an aggregate function.
 *
 * <p>A aggregate call contains:
 *
 * <ul>
 *   <li>a {@link FunctionDefinition} that identifies the function to be called
 *   <li>a list of {@link FieldReferenceExpression} represents the arguments for aggregate function.
 *   <li>a {@link CallExpression} represents the filter with the aggregate function.
 *   <li>a {@link DataType} represents the result data type of aggregate function.
 *   <li>{@code distinct} indicates whether this is a distinct aggregate function.
 *   <li>{@code approximate} indicates whether this is a approximate aggregate function.
 *   <li>{@code ignoreNulls} indicates whether this aggregate function ignore null value.
 * </ul>
 *
 * <p>Note: currently, the {@link AggregateExpression} is only used in {@link
 * SupportsAggregatePushDown}.
 */
@PublicEvolving
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
    public Optional<CallExpression> getFilterExpression() {
        return Optional.ofNullable(filterExpression);
    }

    @Override
    public DataType getOutputDataType() {
        return resultType;
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        List<ResolvedExpression> resolvedChildren = new ArrayList<>(this.args);
        resolvedChildren.add(this.filterExpression);
        return Collections.unmodifiableList(resolvedChildren);
    }

    @Override
    public String asSummaryString() {
        final String argList =
                args.stream()
                        .map(Expression::asSummaryString)
                        .collect(Collectors.joining(", ", "(", ")"));
        return functionDefinition.toString() + argList;
    }

    @Override
    public List<Expression> getChildren() {
        List<Expression> children = new ArrayList<>(this.args);
        children.add(this.filterExpression);
        return Collections.unmodifiableList(children);
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
                && Objects.equals(args, that.args)
                && Objects.equals(filterExpression, that.filterExpression)
                && Objects.equals(resultType, that.resultType)
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
