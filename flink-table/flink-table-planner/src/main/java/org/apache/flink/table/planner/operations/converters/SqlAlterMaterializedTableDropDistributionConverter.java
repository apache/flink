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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableDropDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;

import java.util.List;

/** A converter for {@link SqlAlterMaterializedTableDropDistribution}. */
public class SqlAlterMaterializedTableDropDistributionConverter
        extends AbstractAlterMaterializedTableConverter<SqlAlterMaterializedTableDropDistribution> {
    @Override
    public Operation convertSqlNode(
            SqlAlterMaterializedTableDropDistribution node, ConvertContext context) {
        ObjectIdentifier identifier = resolveIdentifier(node, context);

        ResolvedCatalogMaterializedTable oldTable =
                getResolvedMaterializedTable(
                        context,
                        identifier,
                        () -> "Operation is supported only for materialized tables");

        if (oldTable.getDistribution().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Materialized table %s does not have a distribution to drop.",
                            identifier));
        }

        // Build new materialized table and apply changes
        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(oldTable, builder -> builder.distribution(null));

        return new AlterMaterializedTableChangeOperation(
                identifier, List.of(TableChange.dropDistribution()), updatedTable);
    }
}
