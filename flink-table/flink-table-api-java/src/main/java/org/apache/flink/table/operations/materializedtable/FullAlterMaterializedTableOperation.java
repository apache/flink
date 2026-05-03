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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;

import java.util.List;
import java.util.function.Function;

/**
 * Operation for CREATE OR ALTER MATERIALIZED TABLE ... in case materialized table is present and
 * full materialized table changes should be calculated.
 */
@Internal
public class FullAlterMaterializedTableOperation extends AlterMaterializedTableChangeOperation {

    public FullAlterMaterializedTableOperation(
            final ObjectIdentifier tableIdentifier,
            final Function<ResolvedCatalogMaterializedTable, List<TableChange>> tableChangeForTable,
            final ResolvedCatalogMaterializedTable oldTable) {
        super(tableIdentifier, tableChangeForTable, oldTable);
    }

    @Override
    protected String getOperationName() {
        return "CREATE OR ALTER MATERIALIZED TABLE";
    }
}
