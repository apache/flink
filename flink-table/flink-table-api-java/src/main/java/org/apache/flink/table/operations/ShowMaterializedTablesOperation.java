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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Operation to describe a SHOW MATERIALIZED TABLES statement. The full syntax for SHOW MATERIALIZED
 * TABLES is as followings:
 *
 * <pre>{@code
 * SHOW MATERIALIZED TABLES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] LIKE
 * &lt;sql_like_pattern&gt; ] statement
 * }</pre>
 */
@Internal
public class ShowMaterializedTablesOperation extends AbstractShowOperation {
    private final @Nullable String databaseName;

    public ShowMaterializedTablesOperation(
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable String preposition,
            @Nullable ShowLikeOperator likeOp) {
        super(catalogName, preposition, likeOp);
        this.databaseName = databaseName;
    }

    public ShowMaterializedTablesOperation(
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp) {
        this(catalogName, databaseName, null, likeOp);
    }

    @Override
    protected Collection<String> retrieveDataForTableResult(Context ctx) {
        final CatalogManager catalogManager = ctx.getCatalogManager();
        final String qualifiedCatalogName = catalogManager.qualifyCatalog(catalogName);
        final String qualifiedDatabaseName = catalogManager.qualifyDatabase(databaseName);
        if (preposition == null) {
            return catalogManager.listMaterializedTables();
        } else {
            Catalog catalog = catalogManager.getCatalogOrThrowException(qualifiedCatalogName);
            if (catalog.databaseExists(qualifiedDatabaseName)) {
                return catalogManager.listMaterializedTables(
                        qualifiedCatalogName, qualifiedDatabaseName);
            } else {
                throw new ValidationException(
                        String.format(
                                "Database '%s'.'%s' doesn't exist.",
                                qualifiedCatalogName, qualifiedDatabaseName));
            }
        }
    }

    @Override
    protected String getOperationName() {
        return "SHOW MATERIALIZED TABLES";
    }

    @Override
    protected String getColumnName() {
        return "materialized table";
    }

    @Override
    public String getPrepositionSummaryString() {
        if (databaseName == null) {
            return super.getPrepositionSummaryString();
        }
        return super.getPrepositionSummaryString() + "." + databaseName;
    }
}
