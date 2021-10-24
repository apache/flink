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

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.constraints.Constraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** AlterTableColumnsOperation class for ALTER TABLE ADD clause. */
public class AlterTableAddColumnsOperation extends AlterTableOperation {

    protected final CatalogTable catalogTable;
    protected final CatalogTable origCatalogTable;

    public AlterTableAddColumnsOperation(
            ObjectIdentifier tableIdentifier,
            CatalogTable catalogTable,
            CatalogTable origCatalogTable) {
        super(tableIdentifier);
        this.catalogTable = catalogTable;
        this.origCatalogTable = origCatalogTable;
    }

    public CatalogTable getNewCatalogTable() {
        return catalogTable;
    }

    public CatalogTable getOrigCatalogTable() {
        return origCatalogTable;
    }

    public Optional<List<TableColumn>> getAddedColumns() {
        List<TableColumn> result = new ArrayList<>();
        for (TableColumn column : catalogTable.getSchema().getTableColumns()) {
            Optional<TableColumn> tableColumn =
                    origCatalogTable.getSchema().getTableColumn(column.getName());
            if (!tableColumn.isPresent()) {
                result.add(column);
            }
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(result);
    }

    public Optional<List<WatermarkSpec>> getAddedWatermarks() {
        List<WatermarkSpec> result = new ArrayList<>();
        for (WatermarkSpec wm : catalogTable.getSchema().getWatermarkSpecs()) {
            if (!origCatalogTable.getSchema().getWatermarkSpecs().contains(wm)) {
                result.add(wm);
            }
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(result);
    }

    public Optional<List<Constraint>> getAddedConstraints() {
        List<Constraint> result = new ArrayList<>();
        catalogTable
                .getSchema()
                .getPrimaryKey()
                .ifPresent(
                        uniqueConstraint -> {
                            if (!origCatalogTable.getSchema().getPrimaryKey().isPresent()) {
                                result.add(uniqueConstraint);
                            }
                        });
        return result.isEmpty() ? Optional.empty() : Optional.of(result);
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", tableIdentifier);
        getAddedColumns().ifPresent(tableColumns -> params.put("addedColumns", getAddedColumns()));
        getAddedWatermarks()
                .ifPresent(watermarkSpecs -> params.put("addedWatermarks", getAddedWatermarks()));

        getAddedConstraints()
                .ifPresent(constraints -> params.put("addedConstraintKeys", getAddedConstraints()));

        return OperationUtils.formatWithChildren(
                "ALTER TABLE ADD COLUMNS",
                params,
                Collections.emptyList(),
                Operation::asSummaryString);
    }
}
