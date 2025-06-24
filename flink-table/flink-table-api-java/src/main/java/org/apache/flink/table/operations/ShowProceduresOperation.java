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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Operation to describe a SHOW PROCEDURES statement. The full syntax for SHOW PROCEDURES is as
 * followings:
 *
 * <pre>{@code
 * SHOW PROCEDURES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE)
 * &lt;sql_like_pattern&gt; ] statement
 * }</pre>
 */
@Internal
public class ShowProceduresOperation extends AbstractShowOperation {

    private final @Nullable String databaseName;

    public ShowProceduresOperation(
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable String preposition,
            @Nullable ShowLikeOperator likeOp) {
        super(catalogName, preposition, likeOp);
        this.databaseName = databaseName;
    }

    public ShowProceduresOperation(
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
        try {
            if (preposition == null) {
                // it's to show current_catalog.current_database
                return catalogManager
                        .getCatalogOrError(qualifiedCatalogName)
                        .listProcedures(qualifiedDatabaseName);
            } else {
                Catalog catalog = catalogManager.getCatalogOrThrowException(qualifiedCatalogName);
                return catalog.listProcedures(qualifiedDatabaseName);
            }
        } catch (DatabaseNotExistException e) {
            throw new TableException(
                    String.format(
                            "Fail to show procedures because the Database `%s` to show from/in does not exist in Catalog `%s`.",
                            qualifiedDatabaseName, qualifiedCatalogName),
                    e);
        }
    }

    @Override
    protected String getOperationName() {
        return "SHOW PROCEDURES";
    }

    @Override
    protected String getColumnName() {
        return "procedure name";
    }

    @Override
    public String getPrepositionSummaryString() {
        if (databaseName == null) {
            return super.getPrepositionSummaryString();
        }
        return super.getPrepositionSummaryString() + "." + databaseName;
    }
}
