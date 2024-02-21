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
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.SqlLikeUtils;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/**
 * Operation to describe a SHOW FUNCTIONS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT]
 * (LIKE | ILIKE) &lt;sql_like_pattern&gt; ] statement.
 */
@Internal
public class ShowFunctionsOperation implements ShowOperation {

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
    private final String preposition;
    private final String catalogName;
    private final String databaseName;
    // different like type such as like, ilike
    private final LikeType likeType;
    private final String likePattern;
    private final boolean notLike;

    public ShowFunctionsOperation() {
        // "SHOW FUNCTIONS" default is ALL scope
        this.functionScope = FunctionScope.ALL;
        this.preposition = null;
        this.catalogName = null;
        this.databaseName = null;
        this.likeType = null;
        this.likePattern = null;
        this.notLike = false;
    }

    public ShowFunctionsOperation(
            FunctionScope functionScope, String likeType, String likePattern, boolean notLike) {
        this.functionScope = functionScope;
        this.preposition = null;
        this.catalogName = null;
        this.databaseName = null;
        if (likeType != null) {
            this.likeType = LikeType.of(likeType);
            this.likePattern = requireNonNull(likePattern, "Like pattern must not be null");
            this.notLike = notLike;
        } else {
            this.likeType = null;
            this.likePattern = null;
            this.notLike = false;
        }
    }

    public ShowFunctionsOperation(
            FunctionScope functionScope,
            String preposition,
            String catalogName,
            String databaseName,
            String likeType,
            String likePattern,
            boolean notLike) {
        this.functionScope = functionScope;
        this.preposition = preposition;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        if (likeType != null) {
            this.likeType = LikeType.of(likeType);
            this.likePattern = requireNonNull(likePattern, "Like pattern must not be null");
            this.notLike = notLike;
        } else {
            this.likeType = null;
            this.likePattern = null;
            this.notLike = false;
        }
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder();
        if (functionScope == FunctionScope.ALL) {
            builder.append("SHOW FUNCTIONS");
        } else {
            builder.append(String.format("SHOW %s FUNCTIONS", functionScope));
        }
        if (preposition != null) {
            builder.append(String.format(" %s %s.%s", preposition, catalogName, databaseName));
        }
        if (isWithLike()) {
            if (isNotLike()) {
                builder.append(String.format(" NOT %s '%s'", likeType.name(), likePattern));
            } else {
                builder.append(String.format(" %s '%s'", likeType.name(), likePattern));
            }
        }
        return builder.toString();
    }

    public FunctionScope getFunctionScope() {
        return functionScope;
    }

    public boolean isLike() {
        return likeType == LikeType.LIKE;
    }

    public boolean isWithLike() {
        return likeType != null;
    }

    public boolean isNotLike() {
        return notLike;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        final String[] functionNames;
        if (preposition == null) {
            // it's to show current_catalog.current_database
            switch (functionScope) {
                case USER:
                    functionNames = ctx.getFunctionCatalog().getUserDefinedFunctions();
                    break;
                case ALL:
                    functionNames = ctx.getFunctionCatalog().getFunctions();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "SHOW FUNCTIONS with %s scope is not supported.",
                                    functionScope));
            }
        } else {
            switch (functionScope) {
                case USER:
                    functionNames =
                            ctx.getFunctionCatalog()
                                    .getUserDefinedFunctions(catalogName, databaseName).stream()
                                    .map(FunctionIdentifier::getFunctionName)
                                    .toArray(String[]::new);
                    break;
                case ALL:
                    functionNames =
                            ctx.getFunctionCatalog().getFunctions(catalogName, databaseName);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "SHOW FUNCTIONS with %s scope is not supported.",
                                    functionScope));
            }
        }

        String[] rows;
        if (isWithLike()) {
            rows =
                    Arrays.stream(functionNames)
                            .filter(
                                    row -> {
                                        if (likeType == LikeType.ILIKE) {
                                            return isNotLike()
                                                    != SqlLikeUtils.ilike(row, likePattern, "\\");
                                        } else {
                                            return isNotLike()
                                                    != SqlLikeUtils.like(row, likePattern, "\\");
                                        }
                                    })
                            .sorted()
                            .toArray(String[]::new);
        } else {
            rows = Arrays.stream(functionNames).sorted().toArray(String[]::new);
        }

        return buildStringArrayResult("function name", rows);
    }
}
