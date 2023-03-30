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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import java.util.stream.Collectors;

/**
 * Operation to describe a ALTER TABLE .. SET .. statement.
 *
 * @deprecated Please use {@link AlterTableChangeOperation} instead.
 */
@Deprecated
public class AlterTableOptionsOperation extends AlterTableOperation {
    private final CatalogTable catalogTable;

    public AlterTableOptionsOperation(
            ObjectIdentifier tableIdentifier,
            CatalogTable catalogTable,
            boolean ignoreIfNotExists) {
        super(tableIdentifier, ignoreIfNotExists);
        this.catalogTable = catalogTable;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    @Override
    public String asSummaryString() {
        String description =
                catalogTable.getOptions().entrySet().stream()
                        .map(
                                entry ->
                                        OperationUtils.formatParameter(
                                                entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(", "));
        return String.format(
                "ALTER %sTABLE %s SET (%s)",
                ignoreIfTableNotExists ? "IF EXISTS " : "",
                tableIdentifier.asSummaryString(),
                description);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .alterTable(getCatalogTable(), getTableIdentifier(), ignoreIfTableNotExists());
        return TableResultImpl.TABLE_RESULT_OK;
    }
}
