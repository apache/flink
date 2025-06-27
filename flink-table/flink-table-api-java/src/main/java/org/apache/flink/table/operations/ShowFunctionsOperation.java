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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Operation to describe a SHOW FUNCTIONS statement. The full syntax for SHOW FUNCTIONS is as
 * followings:
 *
 * <pre>{@code
 * SHOW [USER] FUNCTIONS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE)
 * &lt;sql_like_pattern&gt; ] statement
 * }</pre>
 */
@Internal
public class ShowFunctionsOperation extends AbstractShowOperation {

    /**
     * Represent scope of function.
     *
     * <ul>
     *   <li><b>USER</b> return only user-defined functions
     *   <li><b>ALL</b> return all user-defined and built-in functions
     * </ul>
     */
    @Internal
    public enum FunctionScope {
        USER,
        ALL
    }

    private final FunctionScope functionScope;
    private final @Nullable String databaseName;

    public ShowFunctionsOperation(@Nullable String catalogName, @Nullable String databaseName) {
        // "SHOW FUNCTIONS" default is ALL scope
        this(FunctionScope.ALL, catalogName, databaseName, null);
    }

    public ShowFunctionsOperation(
            FunctionScope functionScope,
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp) {
        this(functionScope, null, catalogName, databaseName, likeOp);
    }

    public ShowFunctionsOperation(
            FunctionScope functionScope,
            @Nullable String preposition,
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp) {
        super(catalogName, preposition, likeOp);
        this.functionScope = functionScope;
        this.databaseName = databaseName;
    }

    @Override
    protected Collection<String> retrieveDataForTableResult(Context ctx) {
        switch (functionScope) {
            case USER:
                if (preposition == null) {
                    return Arrays.asList(ctx.getFunctionCatalog().getUserDefinedFunctions());
                }
                return ctx
                        .getFunctionCatalog()
                        .getUserDefinedFunctions(catalogName, databaseName)
                        .stream()
                        .map(FunctionIdentifier::getFunctionName)
                        .collect(Collectors.toList());
            case ALL:
                if (preposition == null) {
                    return Arrays.asList(ctx.getFunctionCatalog().getFunctions());
                }
                return Arrays.asList(
                        ctx.getFunctionCatalog().getFunctions(catalogName, databaseName));
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "SHOW FUNCTIONS with %s scope is not supported.", databaseName));
        }
    }

    @Override
    protected String getOperationName() {
        return functionScope == FunctionScope.ALL ? "SHOW FUNCTIONS" : "SHOW USER FUNCTIONS";
    }

    @Override
    protected String getColumnName() {
        return "function name";
    }

    @Override
    public String getPrepositionSummaryString() {
        if (databaseName == null) {
            return super.getPrepositionSummaryString();
        }
        return super.getPrepositionSummaryString() + "." + databaseName;
    }
}
