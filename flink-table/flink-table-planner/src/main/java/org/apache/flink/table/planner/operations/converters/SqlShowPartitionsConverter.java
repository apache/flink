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

import org.apache.flink.sql.parser.dql.SqlShowPartitions;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowPartitionsOperation;

import java.util.LinkedHashMap;

/** A converter for {@link SqlShowPartitions}. */
public class SqlShowPartitionsConverter implements SqlNodeConverter<SqlShowPartitions> {

    @Override
    public Operation convertSqlNode(SqlShowPartitions sqlShowPartitions, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlShowPartitions.fullTableName());
        ObjectIdentifier tableIdentifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        LinkedHashMap<String, String> partitionKVs = sqlShowPartitions.getPartitionKVs();
        if (partitionKVs != null) {
            CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(partitionKVs);
            return new ShowPartitionsOperation(tableIdentifier, partitionSpec);
        }
        return new ShowPartitionsOperation(tableIdentifier, null);
    }
}
