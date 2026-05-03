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

import org.apache.flink.sql.parser.ddl.table.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDropDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;

import java.util.List;

/** Convert ALTER TABLE DROP DISTRIBUTION statement. */
public class SqlAlterTableDropDistributionConverter
        extends AbstractAlterTableConverter<SqlAlterTableDropDistribution> {
    @Override
    protected Operation convertToOperation(
            SqlAlterTableDropDistribution sqlAlterTable,
            ResolvedCatalogTable resolvedCatalogTable,
            ConvertContext context) {
        final ObjectIdentifier tableIdentifier = resolveIdentifier(sqlAlterTable, context);
        if (resolvedCatalogTable.getDistribution().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Table %s does not have a distribution to drop.", tableIdentifier));
        }

        List<TableChange> tableChanges = List.of(TableChange.dropDistribution());
        return buildAlterTableChangeOperation(
                sqlAlterTable,
                tableChanges,
                resolvedCatalogTable.getUnresolvedSchema(),
                resolvedCatalogTable,
                context.getCatalogManager());
    }

    @Override
    protected TableDistribution getTableDistribution(
            SqlAlterTable alterTable, ResolvedCatalogTable oldTable) {
        return null;
    }
}
