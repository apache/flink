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

import org.apache.flink.sql.parser.SqlPartitionUtils;
import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableRefresh;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableRefreshOperation;

import java.util.Map;

/** A converter for {@link SqlAlterMaterializedTableRefresh}. */
public class SqlAlterMaterializedTableRefreshConverter
        implements SqlNodeConverter<SqlAlterMaterializedTableRefresh> {

    @Override
    public Operation convertSqlNode(SqlAlterMaterializedTableRefresh node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        Map<String, String> partitionSpec =
                SqlPartitionUtils.getPartitionKVs(node.getPartitionSpec());

        return new AlterMaterializedTableRefreshOperation(identifier, partitionSpec);
    }
}
