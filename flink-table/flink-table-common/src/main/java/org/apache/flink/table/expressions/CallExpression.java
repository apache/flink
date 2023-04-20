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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.module.Module;
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
 * Resolved and validated call expression for calling a function.
 *
 * <p>A call contains:
 *
 * <ul>
 *   <li>an output type
 *   <li>a {@link FunctionDefinition} that identifies the function to be called
 *   <li>an optional {@link ObjectIdentifier} that tracks the origin of a function
 *   <li>whether the called function is temporary
 * </ul>
 */
@PublicEvolving
public final class CallExpression implements ResolvedExpression {

    private final boolean isTemporary;

    private final @Nullable FunctionIdentifier functionIdentifier;

    private final FunctionDefinition functionDefinition;

    private final List<ResolvedExpression> args;

    private final DataType dataType;

    @Internal
    public CallExpression(
            boolean isTemporary,
            @Nullable FunctionIdentifier functionIdentifier,
            FunctionDefinition functionDefinition,
            List<ResolvedExpression> args,
            DataType dataType) {
        this.isTemporary = isTemporary;
        this.functionIdentifier = functionIdentifier;
        this.functionDefinition =
                Preconditions.checkNotNull(
                        functionDefinition, "Function definition must not be null.");
        this.args =
                new ArrayList<>(Preconditions.checkNotNull(args, "Arguments must not be null."));
        this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
    }

    /**
     * Creates a {@link CallExpression} to a permanent function (persisted in a {@link Catalog} or
     * provided by a {@link Module}).
     */
    public static CallExpression permanent(
            FunctionIdentifier functionIdentifier,
            FunctionDefinition functionDefinition,
            List<ResolvedExpression> args,
            DataType dataType) {
        return new CallExpression(
                false,
                Preconditions.checkNotNull(
                        functionIdentifier,
                        "Function identifier must not be null for permanent functions."),
                functionDefinition,
                args,
                dataType);
    }

    /**
     * Creates a {@link CallExpression} to a resolved built-in function. It assumes that the {@link
     * BuiltInFunctionDefinition} instance is provided by the framework (usually the core module).
     */
    @Internal
    public static CallExpression permanent(
            BuiltInFunctionDefinition builtInFunctionDefinition,
            List<ResolvedExpression> args,
            DataType dataType) {
        return new CallExpression(
                false,
                FunctionIdentifier.of(builtInFunctionDefinition.getName()),
                builtInFunctionDefinition,
                args,
                dataType);
    }

    /**
     * Creates a {@link CallExpression} to a temporary function (potentially shadowing a {@link
     * Catalog} function or providing a system function).
     */
    public static CallExpression temporary(
            FunctionIdentifier functionIdentifier,
            FunctionDefinition functionDefinition,
            List<ResolvedExpression> args,
            DataType dataType) {
        return new CallExpression(
                true,
                Preconditions.checkNotNull(
                        functionIdentifier,
                        "Function identifier must not be null for temporary functions."),
                functionDefinition,
                args,
                dataType);
    }

    /**
     * Creates a {@link CallExpression} to an anonymous function that has been declared inline
     * without a {@link FunctionIdentifier}.
     */
    public static CallExpression anonymous(
            FunctionDefinition functionDefinition,
            List<ResolvedExpression> args,
            DataType dataType) {
        return new CallExpression(true, null, functionDefinition, args, dataType);
    }

    /**
     * @deprecated Use {@link #permanent(FunctionIdentifier, FunctionDefinition, List, DataType)} or
     *     {@link #temporary(FunctionIdentifier, FunctionDefinition, List, DataType)} instead.
     */
    @Deprecated
    public CallExpression(
            FunctionIdentifier functionIdentifier,
            FunctionDefinition functionDefinition,
            List<ResolvedExpression> args,
            DataType dataType) {
        this(false, functionIdentifier, functionDefinition, args, dataType);
    }

    /** @deprecated Use {@link #anonymous(FunctionDefinition, List, DataType)} instead. */
    @Deprecated
    public CallExpression(
            FunctionDefinition functionDefinition,
            List<ResolvedExpression> args,
            DataType dataType) {
        this(false, null, functionDefinition, args, dataType);
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public Optional<FunctionIdentifier> getFunctionIdentifier() {
        return Optional.ofNullable(functionIdentifier);
    }

    public FunctionDefinition getFunctionDefinition() {
        return functionDefinition;
    }

    /**
     * Returns a string representation of the call's function for logging or printing to a console.
     */
    public String getFunctionName() {
        if (functionIdentifier == null) {
            return functionDefinition.toString();
        } else {
            return functionIdentifier.asSummaryString();
        }
    }

    public CallExpression replaceArgs(List<ResolvedExpression> args, DataType dataType) {
        return new CallExpression(
                this.isTemporary, this.functionIdentifier, this.functionDefinition, args, dataType);
    }

    @Override
    public DataType getOutputDataType() {
        return dataType;
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        return args;
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
        return Collections.unmodifiableList(this.args);
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
        CallExpression that = (CallExpression) o;
        return isTemporary == that.isTemporary
                && Objects.equals(functionIdentifier, that.functionIdentifier)
                && functionDefinition.equals(that.functionDefinition)
                && args.equals(that.args)
                && dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isTemporary, functionIdentifier, functionDefinition, args, dataType);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
