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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.stream.Collectors;

/** Operation to describe a ALTER TABLE IF EXISTS .. SET .. statement. */
public class AlterTableOptionsOperation extends AlterTableOperation {
    private final CatalogTable catalogTable;
    private final Map<String, String> setOptions;

    public AlterTableOptionsOperation(ObjectIdentifier tableIdentifier, CatalogTable catalogTable) {
        this(tableIdentifier, catalogTable, false);
    }

    public AlterTableOptionsOperation(
            ObjectIdentifier tableIdentifier, CatalogTable catalogTable, boolean ifExists) {
        super(tableIdentifier, ifExists);
        this.catalogTable = catalogTable;
        this.setOptions = catalogTable.getOptions();
    }

    public AlterTableOptionsOperation(
            ObjectIdentifier tableIdentifier, Map<String, String> tableOptions, boolean ifExists) {
        super(tableIdentifier, ifExists);
        this.catalogTable = null;
        this.setOptions = tableOptions;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    @Override
    public String asSummaryString() {
        String description =
                setOptions.entrySet().stream()
                        .map(
                                entry ->
                                        OperationUtils.formatParameter(
                                                entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(", "));
        return String.format(
                "ALTER TABLE %s%s SET (%s)",
                ifExists ? "IF EXISTS " : StringUtils.EMPTY,
                tableIdentifier.asSummaryString(),
                description);
    }
}
