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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.ddl.CreateOperation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Operation to describe a CREATE MATERIALIZED TABLE statement. */
@Internal
public class CreateMaterializedTableOperation
        implements CreateOperation, MaterializedTableOperation {

    private final ObjectIdentifier tableIdentifier;
    private final CatalogMaterializedTable materializedTable;

    public CreateMaterializedTableOperation(
            ObjectIdentifier tableIdentifier, ResolvedCatalogMaterializedTable materializedTable) {
        this.tableIdentifier = tableIdentifier;
        this.materializedTable = materializedTable;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // create materialized table in catalog
        ctx.getCatalogManager().createTable(materializedTable, tableIdentifier, false);
        return TableResultImpl.TABLE_RESULT_OK;
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public CatalogMaterializedTable getCatalogMaterializedTable() {
        return materializedTable;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("materializedTable", materializedTable);
        params.put("identifier", tableIdentifier);

        return OperationUtils.formatWithChildren(
                "CREATE MATERIALIZED TABLE",
                params,
                Collections.emptyList(),
                Operation::asSummaryString);
    }
}
