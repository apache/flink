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

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * SqlNode to describe ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name REFRESH
 * [PARTITION (key1=val1, key2=val2, ...)] clause.
 */
public class SqlAlterMaterializedTableRefresh extends SqlAlterMaterializedTable {

    private final SqlNodeList partitionSpec;

    public SqlAlterMaterializedTableRefresh(
            SqlParserPos pos, SqlIdentifier tableName, SqlNodeList partitionSpec) {
        super(pos, tableName);
        this.partitionSpec = partitionSpec;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getTableName(), partitionSpec);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("REFRESH");
        if (!partitionSpec.isEmpty()) {
            writer.keyword("PARTITION");
            partitionSpec.unparse(
                    writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
        }
    }

    public SqlNodeList getPartitionSpec() {
        return partitionSpec;
    }
}
