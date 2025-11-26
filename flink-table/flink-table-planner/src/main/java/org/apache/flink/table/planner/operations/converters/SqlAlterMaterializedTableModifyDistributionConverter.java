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

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableDistribution.SqlAlterMaterializedTableModifyDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.List;

/** A converter for {@link SqlAlterMaterializedTableModifyDistribution}. */
public class SqlAlterMaterializedTableModifyDistributionConverter
        extends AbstractAlterMaterializedTableConverter<
                SqlAlterMaterializedTableModifyDistribution> {

    @Override
    protected Operation convertToOperation(
            SqlAlterMaterializedTableModifyDistribution sqlModifyDistribution,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            ConvertContext context) {
        ObjectIdentifier identifier = resolveIdentifier(sqlModifyDistribution, context);
        if (oldMaterializedTable.getDistribution().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Materialized table %s does not have a distribution to modify.",
                            identifier));
        }

        TableDistribution tableDistribution =
                OperationConverterUtils.getDistributionFromSqlDistribution(
                        sqlModifyDistribution.getDistribution().get());
        // Build new materialized table and apply changes
        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(
                        oldMaterializedTable, builder -> builder.distribution(tableDistribution));

        return new AlterMaterializedTableChangeOperation(
                identifier, List.of(TableChange.modify(tableDistribution)), updatedTable);
    }
}
