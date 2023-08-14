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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.functions.SqlLikeUtils;

import java.util.Set;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operation to describe a SHOW TABLES statement. */
@Internal
public class ShowTablesOperation implements ShowOperation {

    private final String catalogName;
    private final String databaseName;
    private final boolean useLike;
    private final boolean notLike;
    private final String likePattern;
    private final String preposition;

    public ShowTablesOperation() {
        this.catalogName = null;
        this.databaseName = null;
        this.likePattern = null;
        this.useLike = false;
        this.notLike = false;
        this.preposition = null;
    }

    public ShowTablesOperation(String likePattern, boolean useLike, boolean notLike) {
        this.catalogName = null;
        this.databaseName = null;
        this.likePattern =
                useLike ? checkNotNull(likePattern, "Like pattern must not be null") : null;
        this.useLike = useLike;
        this.notLike = notLike;
        this.preposition = null;
    }

    public ShowTablesOperation(
            String catalogName,
            String databaseName,
            String likePattern,
            boolean useLike,
            boolean notLike,
            String preposition) {
        this.catalogName = checkNotNull(catalogName, "Catalog name must not be null");
        this.databaseName = checkNotNull(databaseName, "Database name must not be null");
        this.likePattern =
                useLike ? checkNotNull(likePattern, "Like pattern must not be null") : null;
        this.useLike = useLike;
        this.notLike = notLike;
        this.preposition = checkNotNull(preposition, "Preposition must not be null");
    }

    public String getLikePattern() {
        return likePattern;
    }

    public String getPreposition() {
        return preposition;
    }

    public boolean isUseLike() {
        return useLike;
    }

    public boolean isNotLike() {
        return notLike;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder().append("SHOW TABLES");
        if (this.preposition != null) {
            builder.append(String.format(" %s %s.%s", preposition, catalogName, databaseName));
        }
        if (this.useLike) {
            if (notLike) {
                builder.append(String.format(" %s LIKE %s", "NOT", likePattern));
            } else {
                builder.append(String.format(" LIKE %s", likePattern));
            }
        }
        return builder.toString();
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        final Set<String> tables;
        if (preposition == null) {
            tables = ctx.getCatalogManager().listTables();
        } else {
            Catalog catalog = ctx.getCatalogManager().getCatalogOrThrowException(catalogName);
            if (catalog.databaseExists(databaseName)) {
                tables = ctx.getCatalogManager().listTables(catalogName, databaseName);
            } else {
                throw new ValidationException(
                        String.format(
                                "Database '%s'.'%s' doesn't exist.", catalogName, databaseName));
            }
        }

        final String[] rows;
        if (useLike) {
            rows =
                    tables.stream()
                            .filter(row -> notLike != SqlLikeUtils.like(row, likePattern, "\\"))
                            .sorted()
                            .toArray(String[]::new);
        } else {
            rows = tables.stream().sorted().toArray(String[]::new);
        }
        return buildStringArrayResult("table name", rows);
    }
}
