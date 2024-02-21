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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.functions.SqlLikeUtils;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/**
 * Operation to describe a SHOW PROCEDURES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT]
 * (LIKE | ILIKE) &lt;sql_like_pattern&gt; ] statement.
 */
@Internal
public class ShowProceduresOperation implements ExecutableOperation {

    private final @Nullable String catalogName;

    private final @Nullable String databaseName;
    private final @Nullable String preposition;

    private final boolean notLike;

    // different like type such as like, ilike
    private final LikeType likeType;

    @Nullable private final String sqlLikePattern;

    public ShowProceduresOperation(boolean isNotLike, String likeType, String sqlLikePattern) {
        this(null, null, null, isNotLike, likeType, sqlLikePattern);
    }

    public ShowProceduresOperation(
            @Nullable String preposition,
            @Nullable String catalogName,
            @Nullable String databaseName,
            boolean notLike,
            @Nullable String likeType,
            @Nullable String sqlLikePattern) {
        this.preposition = preposition;
        this.catalogName = catalogName;
        this.databaseName = databaseName;

        if (likeType != null) {
            this.likeType = LikeType.of(likeType);
            this.sqlLikePattern = requireNonNull(sqlLikePattern, "Like pattern must not be null");
            this.notLike = notLike;
        } else {
            this.likeType = null;
            this.sqlLikePattern = null;
            this.notLike = false;
        }
    }

    public boolean isWithLike() {
        return likeType != null;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        final List<String> procedures;
        CatalogManager catalogManager = ctx.getCatalogManager();
        try {
            if (preposition == null) {
                // it's to show current_catalog.current_database
                procedures =
                        catalogManager
                                .getCatalogOrError(catalogManager.getCurrentCatalog())
                                .listProcedures(catalogManager.getCurrentDatabase());
            } else {
                Catalog catalog = catalogManager.getCatalogOrThrowException(catalogName);
                procedures = catalog.listProcedures(databaseName);
            }
        } catch (DatabaseNotExistException e) {
            throw new TableException(
                    String.format(
                            "Fail to show procedures because the Database `%s` to show from/in does not exist in Catalog `%s`.",
                            preposition == null
                                    ? catalogManager.getCurrentDatabase()
                                    : databaseName,
                            preposition == null
                                    ? catalogManager.getCurrentCatalog()
                                    : catalogName));
        }

        final String[] rows;
        if (isWithLike()) {
            rows =
                    procedures.stream()
                            .filter(
                                    row -> {
                                        boolean likeMatch =
                                                likeType == LikeType.ILIKE
                                                        ? SqlLikeUtils.ilike(
                                                                row, sqlLikePattern, "\\")
                                                        : SqlLikeUtils.like(
                                                                row, sqlLikePattern, "\\");
                                        return notLike != likeMatch;
                                    })
                            .sorted()
                            .toArray(String[]::new);
        } else {
            rows = procedures.stream().sorted().toArray(String[]::new);
        }
        return buildStringArrayResult("procedure name", rows);
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder().append("SHOW PROCEDURES");
        if (this.preposition != null) {
            builder.append(String.format(" %s %s.%s", preposition, catalogName, databaseName));
        }
        if (isWithLike()) {
            if (notLike) {
                builder.append(String.format(" %s %s %s", "NOT", likeType.name(), sqlLikePattern));
            } else {
                builder.append(String.format(" %s %s", likeType.name(), sqlLikePattern));
            }
        }
        return builder.toString();
    }
}
