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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.calcite.FlinkContext;

import java.util.Collections;
import java.util.Map;

/** Base class for {@link DynamicTableSinkSpec} and {@link DynamicTableSourceSpec}. */
class DynamicTableSpecBase {

    Map<String, String> loadOptionsFromCatalogTable(
            ContextResolvedTable contextResolvedTable, FlinkContext flinkContext) {
        // We need to load options from the catalog only if PLAN_RESTORE_CATALOG_OBJECTS == ALL and
        // the table is permanent.
        // In case of CatalogPlanRestore.ALL_ENFORCED, catalog querying is disabled.
        // In case of CatalogPlanRestore.IDENTIFIER, getCatalogTable() already returns the table
        //  loaded from the catalog
        final TableConfigOptions.CatalogPlanRestore catalogPlanRestore =
                flinkContext.getTableConfig().get(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS);
        if (!contextResolvedTable.isPermanent()
                || catalogPlanRestore != TableConfigOptions.CatalogPlanRestore.ALL) {
            return Collections.emptyMap();
        }

        return flinkContext
                .getCatalogManager()
                .getTable(contextResolvedTable.getIdentifier())
                .map(ContextResolvedTable::<ResolvedCatalogTable>getResolvedTable)
                .map(CatalogBaseTable::getOptions)
                .orElse(Collections.emptyMap());
    }
}
