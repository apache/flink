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

import org.apache.flink.sql.parser.ddl.SqlAddPartitions;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AddPartitionsOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A converter for {@link SqlAddPartitions}. */
public class SqlAlterTableAddPartitionConverter implements SqlNodeConverter<SqlAddPartitions> {

    @Override
    public Operation convertSqlNode(SqlAddPartitions sqlAddPartitions, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAddPartitions.fullTableName());
        ObjectIdentifier tableIdentifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        List<CatalogPartitionSpec> specs = new ArrayList<>();
        List<CatalogPartition> partitions = new ArrayList<>();
        for (int i = 0; i < sqlAddPartitions.getPartSpecs().size(); i++) {
            specs.add(new CatalogPartitionSpec(sqlAddPartitions.getPartitionKVs(i)));
            Map<String, String> props =
                    OperationConverterUtils.extractProperties(
                            sqlAddPartitions.getPartProps().get(i));
            partitions.add(new CatalogPartitionImpl(props, null));
        }
        return new AddPartitionsOperation(
                tableIdentifier, sqlAddPartitions.ifPartitionNotExists(), specs, partitions);
    }
}
