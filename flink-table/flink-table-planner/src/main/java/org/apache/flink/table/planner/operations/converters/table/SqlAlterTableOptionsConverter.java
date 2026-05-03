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

package org.apache.flink.table.planner.operations.converters.table;

import org.apache.flink.sql.parser.ddl.table.SqlAlterTableOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Convert ALTER TABLE [IF EXISTS] [[catalogName.] dataBasesName].tableName SET ( name=value [,
 * name=value]*) statement.
 */
public class SqlAlterTableOptionsConverter
        extends AbstractAlterTableConverter<SqlAlterTableOptions> {
    @Override
    protected Operation convertToOperation(
            SqlAlterTableOptions alterTableOptions,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        final ObjectIdentifier tableIdentifier = resolveIdentifier(alterTableOptions, context);
        final Map<String, String> partitionKVs = alterTableOptions.getPartitionKVs();
        // it's altering partitions
        if (partitionKVs != null) {
            CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(partitionKVs);
            CatalogPartition catalogPartition =
                    context.getCatalogManager()
                            .getPartition(tableIdentifier, partitionSpec)
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "Partition %s of table %s doesn't exist",
                                                            partitionSpec.getPartitionSpec(),
                                                            tableIdentifier)));
            Map<String, String> newProps = new HashMap<>(catalogPartition.getProperties());
            newProps.putAll(alterTableOptions.getProperties());
            return new AlterPartitionPropertiesOperation(
                    tableIdentifier,
                    partitionSpec,
                    new CatalogPartitionImpl(newProps, catalogPartition.getComment()));
        } else {
            // it's altering a table
            Map<String, String> changeOptions = alterTableOptions.getProperties();
            Map<String, String> newOptions = new HashMap<>(oldTable.getOptions());
            newOptions.putAll(changeOptions);
            return new AlterTableChangeOperation(
                    tableIdentifier,
                    changeOptions.entrySet().stream()
                            .map(entry -> TableChange.set(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList()),
                    oldTable.copy(newOptions),
                    alterTableOptions.ifTableExists());
        }
    }
}
