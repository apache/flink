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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * This class contains information about a function and its relationship with a {@link Catalog}, if
 * any.
 *
 * <p>There can be 3 kinds of {@link ContextResolvedFunction}:
 *
 * <ul>
 *   <li>A permanent function: a function which is stored in a {@link Catalog} or is provided by a
 *       {@link Module} as a system function. It always has an associated unique {@link
 *       FunctionIdentifier}.
 *   <li>A temporary function: a function which is stored in the {@link FunctionCatalog} (either as
 *       a catalog or system function), has an associated unique {@link FunctionIdentifier} and is
 *       flagged as temporary.
 *   <li>An anonymous/inline function: a function which is not stored in a catalog or module and
 *       doesn't have an associated unique {@link FunctionIdentifier}. During Table API translation,
 *       {@link BuiltInFunctionDefinition}s are treated as anonymous functions in the API builders
 *       that get translated to permanent functions if available in the {@link CoreModule}.
 * </ul>
 *
 * <p>This class is meant for internal usages. However, it needs to be kept in sync with the public
 * {@link CallExpression} which contains similar context information. The convenience methods {@link
 * #fromCallExpression(CallExpression)} and {@link #toCallExpression(List, DataType)} allow a
 * symmetric conversion.
 */
@Internal
public final class ContextResolvedFunction {

    private final boolean isTemporary;

    private final @Nullable FunctionIdentifier functionIdentifier;

    private final FunctionDefinition functionDefinition;

    public static ContextResolvedFunction permanent(
            FunctionIdentifier functionIdentifier, FunctionDefinition functionDefinition) {
        Preconditions.checkNotNull(
                functionIdentifier,
                "Function identifier should not be null for a permanent function.");
        return new ContextResolvedFunction(false, functionIdentifier, functionDefinition);
    }

    public static ContextResolvedFunction temporary(
            FunctionIdentifier functionIdentifier, FunctionDefinition functionDefinition) {
        Preconditions.checkNotNull(
                functionIdentifier,
                "Function identifier should not be null for a temporary function.");
        return new ContextResolvedFunction(true, functionIdentifier, functionDefinition);
    }

    public static ContextResolvedFunction anonymous(FunctionDefinition functionDefinition) {
        return new ContextResolvedFunction(true, null, functionDefinition);
    }

    public static ContextResolvedFunction fromCallExpression(CallExpression callExpression) {
        return new ContextResolvedFunction(
                callExpression.isTemporary(),
                callExpression.getFunctionIdentifier().orElse(null),
                callExpression.getFunctionDefinition());
    }

    private ContextResolvedFunction(
            boolean isTemporary,
            @Nullable FunctionIdentifier functionIdentifier,
            FunctionDefinition functionDefinition) {
        this.isTemporary = isTemporary;
        this.functionIdentifier = functionIdentifier;
        this.functionDefinition =
                Preconditions.checkNotNull(
                        functionDefinition, "Function definition must not be null.");
    }

    public boolean isAnonymous() {
        return functionIdentifier == null;
    }

    /** @return true if the function is temporary. An anonymous function is always temporary. */
    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean isPermanent() {
        return !isTemporary;
    }

    public Optional<FunctionIdentifier> getIdentifier() {
        return Optional.ofNullable(functionIdentifier);
    }

    public FunctionDefinition getDefinition() {
        return functionDefinition;
    }

    public String asSummaryString() {
        if (functionIdentifier == null) {
            return functionDefinition.toString();
        } else {
            return functionIdentifier.asSummaryString();
        }
    }

    public CallExpression toCallExpression(
            List<ResolvedExpression> resolvedArgs, DataType outputDataType) {
        return new CallExpression(
                isTemporary, functionIdentifier, functionDefinition, resolvedArgs, outputDataType);
    }

    @Override
    public String toString() {
        if (functionIdentifier != null) {
            return functionIdentifier.asSummaryString();
        }
        return functionDefinition.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContextResolvedFunction that = (ContextResolvedFunction) o;
        return isTemporary == that.isTemporary
                && Objects.equals(functionIdentifier, that.functionIdentifier)
                && functionDefinition.equals(that.functionDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isTemporary, functionIdentifier, functionDefinition);
    }
}
