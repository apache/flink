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
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDistribution;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDistribution.SqlAlterTableModifyDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.List;
import java.util.Optional;

/** A converter for {@link SqlAlterTableDistribution} for MODIFY call. */
public class SqlAlterTableModifyDistributionConverter
        extends AbstractAlterTableConverter<SqlAlterTableModifyDistribution> {

    @Override
    protected Operation convertToOperation(
            SqlAlterTableModifyDistribution sqlModifyDistribution,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        return buildAlterTableChangeOperation(
                sqlModifyDistribution,
                List.of(
                        TableChange.modify(
                                OperationConverterUtils.getDistributionFromSqlDistribution(
                                        sqlModifyDistribution.getDistribution()))),
                oldTable.getUnresolvedSchema(),
                oldTable,
                context.getCatalogManager());
    }

    protected TableDistribution getTableDistribution(
            SqlAlterTable alterTable, ResolvedCatalogTable oldTable) {
        Optional<TableDistribution> oldDistribution = oldTable.getDistribution();
        if (oldDistribution.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table does not define any distribution. You might want to add a new one.",
                            EX_MSG_PREFIX));
        }
        return OperationConverterUtils.getDistributionFromSqlDistribution(
                ((SqlAlterTableDistribution) alterTable).getDistribution());
    }
}
