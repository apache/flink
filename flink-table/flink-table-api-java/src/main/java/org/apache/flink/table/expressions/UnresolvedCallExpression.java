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
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Unresolved call expression for calling a function identified by a {@link FunctionDefinition}.
 *
 * <p>This is purely an API facing expression with unvalidated arguments and unknown output data
 * type.
 *
 * <p>A unresolved call contains:
 *
 * <ul>
 *   <li>a {@link FunctionDefinition} that identifies the function to be called
 *   <li>an optional {@link FunctionIdentifier} that tracks the origin of a function
 *   <li>whether the called function is temporary
 * </ul>
 */
@PublicEvolving
public final class UnresolvedCallExpression implements Expression {

    private final ContextResolvedFunction resolvedFunction;

    private final List<Expression> args;

    UnresolvedCallExpression(ContextResolvedFunction resolvedFunction, List<Expression> args) {
        this.resolvedFunction = resolvedFunction;
        this.args =
                Collections.unmodifiableList(
                        Preconditions.checkNotNull(args, "Arguments must not be null."));
    }

    public Optional<FunctionIdentifier> getFunctionIdentifier() {
        return resolvedFunction.getIdentifier();
    }

    public FunctionDefinition getFunctionDefinition() {
        return resolvedFunction.getDefinition();
    }

    public UnresolvedCallExpression replaceArgs(List<Expression> args) {
        return new UnresolvedCallExpression(resolvedFunction, args);
    }

    public CallExpression resolve(List<ResolvedExpression> args, DataType dataType) {
        return resolvedFunction.toCallExpression(args, dataType);
    }

    @Override
    public String asSummaryString() {
        final String functionName = resolvedFunction.asSummaryString();

        final String argList =
                args.stream()
                        .map(Expression::asSummaryString)
                        .collect(Collectors.joining(", ", "(", ")"));

        return functionName + argList;
    }

    @Override
    public List<Expression> getChildren() {
        return this.args;
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
        UnresolvedCallExpression that = (UnresolvedCallExpression) o;
        return resolvedFunction.equals(that.resolvedFunction) && args.equals(that.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resolvedFunction, args);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
